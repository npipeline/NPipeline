using System.Collections.Immutable;
using System.Reflection;
using NPipeline.Configuration;
using NPipeline.DataFlow.Routing;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Execution.Services;

/// <summary>
///     Default runtime binder for pipeline runs.
/// </summary>
public sealed class RuntimePipelineBinder : IRuntimePipelineBinder
{
    private static readonly MethodInfo AdaptLineageRouteOptionsMethod = typeof(RuntimePipelineBinder)
        .GetMethod(nameof(AdaptLineageRouteOptionsGeneric), BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException($"Method '{nameof(AdaptLineageRouteOptionsGeneric)}' not found.");

    /// <summary>
    ///     Shared singleton instance for the stateless runtime binder.
    /// </summary>
    public static RuntimePipelineBinder Instance { get; } = new();

    /// <inheritdoc />
    public Task<RuntimePipelineBindingResult> BindAsync(PipelineGraph graph, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(context);

        var overriddenGraph = ApplyRuntimeItemLevelLineageOverride(graph, context);
        overriddenGraph = ApplyRuntimeLineageOptionsOverride(overriddenGraph, context);
        overriddenGraph = NormalizeRuntimeExecutionAnnotations(overriddenGraph);

        var deadLetterSink = ResolveDeadLetterSink(overriddenGraph, context.ErrorHandlerFactory);
        deadLetterSink = ApplyDeadLetterSinkDecorator(context, deadLetterSink);
        var resiliencePolicy = ResolveResiliencePolicy(overriddenGraph);

        var lineageSink = overriddenGraph.Lineage.ItemLevelLineageEnabled
            ? ResolveLineageSink(overriddenGraph, context.LineageFactory, context)
            : null;

        lineageSink = ApplyLineageSinkDecorator(context, lineageSink);

        var pipelineLineageSink = ResolvePipelineLineageSink(overriddenGraph, context.LineageFactory, context);

        return Task.FromResult(new RuntimePipelineBindingResult(
            overriddenGraph,
            deadLetterSink,
            lineageSink,
            pipelineLineageSink,
            resiliencePolicy));
    }

    private static IResiliencePolicy ResolveResiliencePolicy(PipelineGraph graph)
    {
        if (graph.ErrorHandling.ResiliencePolicy is not null)
            return graph.ErrorHandling.ResiliencePolicy;

        if (graph.ErrorHandling.ResiliencePolicyType is null)
            return DefaultResiliencePolicy.Instance;

        if (!typeof(IResiliencePolicy).IsAssignableFrom(graph.ErrorHandling.ResiliencePolicyType))
        {
            throw new InvalidOperationException(
                $"Configured resilience policy type '{graph.ErrorHandling.ResiliencePolicyType.FullName}' does not implement IResiliencePolicy.");
        }

        if (Activator.CreateInstance(graph.ErrorHandling.ResiliencePolicyType) is IResiliencePolicy policy)
            return policy;

        throw new InvalidOperationException(
            $"Unable to create resilience policy instance for type '{graph.ErrorHandling.ResiliencePolicyType.FullName}'.");
    }

    private static PipelineGraph ApplyRuntimeLineageOptionsOverride(PipelineGraph graph, PipelineContext context)
    {
        if (!context.Properties.TryGetValue(PipelineContextKeys.LineageOptionsOverride, out var overrideObj) || overrideObj is null)
            return graph;

        var resolved = overrideObj switch
        {
            LineageOptions direct => direct,
            Func<LineageOptions?, LineageOptions?> factory => factory(graph.Lineage.LineageOptions),
            _ => graph.Lineage.LineageOptions,
        };

        return graph with
        {
            Lineage = graph.Lineage with
            {
                LineageOptions = resolved,
            },
        };
    }

    private static PipelineGraph ApplyRuntimeItemLevelLineageOverride(PipelineGraph graph, PipelineContext context)
    {
        if (!context.Properties.TryGetValue(PipelineContextKeys.ItemLevelLineageEnabledOverride, out var overrideObj) ||
            overrideObj is not bool enabled)
            return graph;

        var resolvedOptions = graph.Lineage.LineageOptions;

        if (enabled && resolvedOptions is null)
        {
            // Mirror PipelineBuilder.EnableItemLevelLineage() defaults for runtime enablement.
            resolvedOptions = LineageOptions.CompleteLineage;
        }

        return graph with
        {
            Lineage = graph.Lineage with
            {
                ItemLevelLineageEnabled = enabled,
                LineageOptions = resolvedOptions,
            },
        };
    }

    private static PipelineGraph NormalizeRuntimeExecutionAnnotations(PipelineGraph graph)
    {
        var normalizedAnnotations =
            (graph.ExecutionOptions.NodeExecutionAnnotations ?? ImmutableDictionary<string, object>.Empty).ToBuilder();

        foreach (var nodeDef in graph.Nodes)
        {
            var contract = BuildRuntimeStreamContract(nodeDef, graph.Lineage.ItemLevelLineageEnabled);
            normalizedAnnotations[ExecutionAnnotationKeys.RuntimeStreamContractForNode(nodeDef.Id)] = contract;

            if (nodeDef.Kind != NodeKind.Route)
                continue;

            var routeKey = ExecutionAnnotationKeys.RouteOptionsForNode(nodeDef.Id);

            if (!normalizedAnnotations.TryGetValue(routeKey, out var routeOptions) || routeOptions is null)
                continue;

            normalizedAnnotations[routeKey] = NormalizeRouteOptions(nodeDef, contract, routeOptions);
        }

        return graph with
        {
            ExecutionOptions = graph.ExecutionOptions with
            {
                NodeExecutionAnnotations = normalizedAnnotations.ToImmutable(),
            },
        };
    }

    private static RuntimeNodeStreamContract BuildRuntimeStreamContract(NodeDefinition nodeDef, bool lineageEnabled)
    {
        var effectiveInputItemType = ResolveEffectiveInputItemType(nodeDef, lineageEnabled);
        var effectiveOutputItemType = ResolveEffectiveOutputItemType(nodeDef, lineageEnabled);

        return new RuntimeNodeStreamContract(effectiveInputItemType, effectiveOutputItemType, lineageEnabled);
    }

    private static Type? ResolveEffectiveInputItemType(NodeDefinition nodeDef, bool lineageEnabled)
    {
        return nodeDef.Kind switch
        {
            NodeKind.Source or NodeKind.CompositeInput => null,
            NodeKind.Join => typeof(object),
            _ => WrapWithLineageIfEnabled(nodeDef.InputType, lineageEnabled),
        };
    }

    private static Type? ResolveEffectiveOutputItemType(NodeDefinition nodeDef, bool lineageEnabled)
    {
        return nodeDef.Kind switch
        {
            NodeKind.Sink or NodeKind.CompositeOutput => null,
            _ => WrapWithLineageIfEnabled(nodeDef.OutputType, lineageEnabled),
        };
    }

    private static Type? WrapWithLineageIfEnabled(Type? payloadType, bool lineageEnabled)
    {
        if (payloadType is null)
            return null;

        return lineageEnabled
            ? typeof(LineagePacket<>).MakeGenericType(payloadType)
            : payloadType;
    }

    private static object NormalizeRouteOptions(NodeDefinition nodeDef, RuntimeNodeStreamContract contract, object routeOptions)
    {
        var expectedItemType = contract.EffectiveOutputItemType
            ?? throw new InvalidOperationException($"Route node '{nodeDef.Id}' has no effective runtime output item type.");

        var expectedRouteOptionsType = typeof(RouteOptions<>).MakeGenericType(expectedItemType);
        var actualRouteOptionsType = routeOptions.GetType();

        if (actualRouteOptionsType == expectedRouteOptionsType)
            return routeOptions;

        // Breaking change: route options must match effective runtime stream type.
        // Compatibility bridge: when lineage is enabled and options are payload-typed, normalize once at bind-time.
        if (contract.ItemLevelLineageEnabled)
        {
            var payloadType = nodeDef.OutputType
                              ?? nodeDef.InputType
                              ?? throw new InvalidOperationException(
                                  $"Unable to normalize route options for node '{nodeDef.Id}' without payload type metadata.");

            var payloadRouteOptionsType = typeof(RouteOptions<>).MakeGenericType(payloadType);

            if (actualRouteOptionsType == payloadRouteOptionsType)
            {
                var expectedLineageItemType = typeof(LineagePacket<>).MakeGenericType(payloadType);

                if (expectedItemType != expectedLineageItemType)
                {
                    throw new InvalidOperationException(
                        $"Route options normalization mismatch for node '{nodeDef.Id}'. " +
                        $"Expected runtime route item type '{GetAssemblyQualifiedTypeName(expectedLineageItemType)}' " +
                        $"but resolved '{GetAssemblyQualifiedTypeName(expectedItemType)}'.");
                }

                return AdaptLineageRouteOptions(payloadType, routeOptions);
            }
        }

        throw new InvalidOperationException(
            $"Route options type mismatch for route node '{nodeDef.Id}'. " +
            $"Expected '{GetAssemblyQualifiedTypeName(expectedRouteOptionsType)}' " +
            $"but got '{GetAssemblyQualifiedTypeName(actualRouteOptionsType)}'.");
    }

    private static object AdaptLineageRouteOptions(Type payloadType, object routeOptions)
    {
        var genericMethod = AdaptLineageRouteOptionsMethod.MakeGenericMethod(payloadType);
        return genericMethod.Invoke(null, [routeOptions])!;
    }

    private static object AdaptLineageRouteOptionsGeneric<TPayload>(object routeOptions)
    {
        var payloadRouteOptions = (RouteOptions<TPayload>)routeOptions;

        var adapted = new RouteOptions<LineagePacket<TPayload>>()
            .WithMatchMode(payloadRouteOptions.MatchMode)
            .WithNoMatchBehavior(payloadRouteOptions.NoMatchBehavior);

        if (payloadRouteOptions.OtherwiseOutputName is { } otherwiseOutputName)
            adapted.Otherwise(otherwiseOutputName);

        foreach (var rule in payloadRouteOptions.Rules)
        {
            adapted.When(rule.OutputName, packet => rule.Predicate(packet.Data));
        }

        return adapted;
    }

    private static string GetAssemblyQualifiedTypeName(Type type)
        => type.AssemblyQualifiedName ?? type.FullName ?? type.Name;

    private static IDeadLetterSink? ApplyDeadLetterSinkDecorator(PipelineContext context, IDeadLetterSink? deadLetterSink)
    {
        if (context.Properties.TryGetValue(PipelineContextKeys.DeadLetterSinkDecorator, out var decoratorObj) &&
            decoratorObj is Func<IDeadLetterSink?, IDeadLetterSink?> decorator)
            return decorator(deadLetterSink);

        return deadLetterSink;
    }

    private static ILineageSink? ApplyLineageSinkDecorator(PipelineContext context, ILineageSink? lineageSink)
    {
        if (context.Properties.TryGetValue(PipelineContextKeys.LineageSinkDecorator, out var decoratorObj) &&
            decoratorObj is Func<ILineageSink?, ILineageSink?> decorator)
            return decorator(lineageSink);

        return lineageSink;
    }

    private static IDeadLetterSink? ResolveDeadLetterSink(PipelineGraph graph, IErrorHandlerFactory errorHandlerFactory)
    {
        if (graph.ErrorHandling.DeadLetterSink is not null)
            return graph.ErrorHandling.DeadLetterSink;

        if (graph.ErrorHandling.DeadLetterSinkType is not null)
            return errorHandlerFactory.CreateDeadLetterSink(graph.ErrorHandling.DeadLetterSinkType);

        return null;
    }

    private static ILineageSink? ResolveLineageSink(PipelineGraph graph, ILineageFactory lineageFactory, PipelineContext context)
    {
        if (graph.Lineage.LineageSink is not null)
            return graph.Lineage.LineageSink;

        if (graph.Lineage.LineageSinkType is not null)
            return lineageFactory.CreateLineageSink(graph.Lineage.LineageSinkType);

        if (context.LineageSink is not null)
            return context.LineageSink;

        return null;
    }

    private static IPipelineLineageSink? ResolvePipelineLineageSink(PipelineGraph graph, ILineageFactory lineageFactory, PipelineContext context)
    {
        if (graph.Lineage.PipelineLineageSink is not null)
            return graph.Lineage.PipelineLineageSink;

        if (graph.Lineage.PipelineLineageSinkType is not null)
            return lineageFactory.CreatePipelineLineageSink(graph.Lineage.PipelineLineageSinkType);

        if (context.PipelineLineageSink is not null)
            return context.PipelineLineageSink;

        // Provider-based default (no reflection):
        // When item-level lineage is enabled and no explicit sink is configured,
        // attempt to resolve a provider (supplied by optional packages like NPipeline.Lineage)
        // and let it create the default sink.
        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var provider = lineageFactory.ResolvePipelineLineageSinkProvider();
            var provided = provider?.Create(context);

            if (provided is not null)
                return provided;
        }

        return null;
    }
}