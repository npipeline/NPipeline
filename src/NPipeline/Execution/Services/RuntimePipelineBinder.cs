using System.Collections.Immutable;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Configuration;
using NPipeline.DataFlow.Routing;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution.Services;

/// <summary>
///     Default runtime binder for pipeline runs.
/// </summary>
public sealed class RuntimePipelineBinder : IRuntimePipelineBinder
{
    private static readonly MethodInfo AdaptLineageRouteOptionsMethod = typeof(RuntimePipelineBinder)
                                                                            .GetMethod(nameof(AdaptLineageRouteOptionsGeneric),
                                                                                BindingFlags.NonPublic | BindingFlags.Static)
                                                                        ?? throw new InvalidOperationException(
                                                                            $"Method '{nameof(AdaptLineageRouteOptionsGeneric)}' not found.");

    private static readonly ConditionalWeakTable<ExecutionOptionsConfiguration, NormalizedAnnotationCache>
        NormalizedAnnotations = new();

    private sealed class NormalizedAnnotationCache
    {
        private readonly object _lock = new();
        private NormalizedAnnotationEntry? _entry;

        public ImmutableDictionary<string, object> GetOrAdd(PipelineGraph graph, bool lineageEnabled)
        {
            lock (_lock)
            {
                // ImmutableArray equality compares the backing array, so a graph copied with new nodes misses the cache.
                if (_entry is { } existing && existing.LineageEnabled == lineageEnabled && existing.Nodes == graph.Nodes)
                    return existing.Annotations;

                var annotations = ComputeNormalizedAnnotations(graph, lineageEnabled);
                _entry = new NormalizedAnnotationEntry(lineageEnabled, graph.Nodes, annotations);
                return annotations;
            }
        }
    }

    private sealed record NormalizedAnnotationEntry(
        bool LineageEnabled,
        ImmutableArray<NodeDefinition> Nodes,
        ImmutableDictionary<string, object> Annotations);

    /// <summary>
    ///     Shared singleton instance for the stateless runtime binder.
    /// </summary>
    public static RuntimePipelineBinder Instance { get; } = new();

    /// <inheritdoc />
    public async Task<RuntimePipelineBindingResult> BindAsync(PipelineGraph graph, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(context);

        // Instances created from a configured type belong to this run, not to the context, which a caller may reuse
        // for many runs. They are handed to the run's owned-instance set, or released here if binding fails.
        var runOwned = new List<object>();

        try
        {
            return Bind(graph, context, runOwned);
        }
        catch
        {
            await DisposeCreatedAsync(runOwned).ConfigureAwait(false);
            throw;
        }
    }

    private static async ValueTask DisposeCreatedAsync(List<object> created)
    {
        var owned = new OwnedNodeInstances();
        owned.AddRange(created);

        // Best effort: the binding failure is the error worth reporting.
        _ = await owned.DisposeAllAsync().ConfigureAwait(false);
    }

    private static RuntimePipelineBindingResult Bind(PipelineGraph graph, PipelineContext context, List<object> runOwned)
    {
        var overriddenGraph = ApplyRuntimeItemLevelLineageOverride(graph, context);
        overriddenGraph = ApplyRuntimeLineageOptionsOverride(overriddenGraph, context);
        overriddenGraph = NormalizeRuntimeExecutionAnnotations(overriddenGraph);

        var deadLetterSink = ResolveDeadLetterSink(overriddenGraph, context.ErrorHandlerFactory, runOwned);
        deadLetterSink = ApplyDeadLetterSinkDecorator(context, deadLetterSink);
        var resiliencePolicy = ResolveResiliencePolicy(overriddenGraph, context, runOwned);

        var itemLevelLineageEnabled = overriddenGraph.Lineage.ItemLevelLineageEnabled;

        var lineageSink = itemLevelLineageEnabled
            ? ResolveLineageSink(overriddenGraph, context.Lineage.LineageFactory, context, runOwned)
            : null;

        if (!itemLevelLineageEnabled)
            WarnIfItemLevelLineageSinkIgnored(overriddenGraph, context);

        lineageSink = ApplyLineageSinkDecorator(context, lineageSink);

        // Resolved only for item-level lineage: the collector is fed from the per-item record funnel.
        // The tee is applied outside any caller-supplied decorator so the collector observes every record
        // the run emits, regardless of how that decorator reshapes the sink.
        var lineageCollector = itemLevelLineageEnabled
            ? context.Lineage.LineageFactory.ResolveLineageCollector()
            : null;

        if (lineageCollector is not null)
            lineageSink = new CollectorTeeingLineageSink(lineageCollector, lineageSink);

        var pipelineLineageSink = ResolvePipelineLineageSink(overriddenGraph, context.Lineage.LineageFactory, context, runOwned);

        return new RuntimePipelineBindingResult(
            overriddenGraph,
            deadLetterSink,
            lineageSink,
            pipelineLineageSink,
            resiliencePolicy,
            lineageCollector,
            runOwned);
    }

    /// <summary>
    ///     Warns when an item-level lineage sink is configured but item-level lineage is switched off, in which
    ///     case the sink is never invoked. Pipeline-level sinks are unaffected: those report the graph structure
    ///     and do not require item-level tracking.
    /// </summary>
    private static void WarnIfItemLevelLineageSinkIgnored(PipelineGraph graph, PipelineContext context)
    {
        var configuredSink = graph.Lineage.LineageSink?.GetType().Name
                             ?? graph.Lineage.LineageSinkType?.Name
                             ?? context.Lineage.LineageSink?.GetType().Name;

        if (configuredSink is null)
            return;

        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(RuntimePipelineBinder));
        RuntimePipelineBinderLogMessages.ItemLevelLineageSinkIgnored(logger, configuredSink);
    }

    private static IResiliencePolicy ResolveResiliencePolicy(PipelineGraph graph, PipelineContext context, List<object> runOwned)
    {
        if (graph.ErrorHandling.ResiliencePolicy is not null)
            return graph.ErrorHandling.ResiliencePolicy;

        if (graph.ErrorHandling.ResiliencePolicyType is null)
            return context.ConfiguredResiliencePolicy ?? DefaultResiliencePolicy.Instance;

        if (!typeof(IResiliencePolicy).IsAssignableFrom(graph.ErrorHandling.ResiliencePolicyType))
        {
            throw new InvalidOperationException(
                $"Configured resilience policy type '{graph.ErrorHandling.ResiliencePolicyType.FullName}' does not implement IResiliencePolicy.");
        }

        if (Activator.CreateInstance(graph.ErrorHandling.ResiliencePolicyType) is IResiliencePolicy policy)
        {
            // The caller owns instances created from a type, so the run disposes it.
            runOwned.Add(policy);
            return policy;
        }

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
        return graph with
        {
            ExecutionOptions = graph.ExecutionOptions with
            {
                NodeExecutionAnnotations = GetNormalizedAnnotations(graph),
            },
        };
    }

    // The normalised annotations depend only on the annotations bag, the node definitions and the lineage flag, all of
    // which are fixed for a cached graph. Recomputing builds a generic lineage type per node on every run, so the
    // result is memoised against the configuration instance and reused while the nodes and the lineage flag match.
    private static ImmutableDictionary<string, object> GetNormalizedAnnotations(PipelineGraph graph) =>
        NormalizedAnnotations.GetOrCreateValue(graph.ExecutionOptions).GetOrAdd(graph, graph.Lineage.ItemLevelLineageEnabled);

    private static ImmutableDictionary<string, object> ComputeNormalizedAnnotations(PipelineGraph graph, bool lineageEnabled)
    {
        var normalizedAnnotations =
            (graph.ExecutionOptions.NodeExecutionAnnotations ?? ImmutableDictionary<string, object>.Empty).ToBuilder();

        foreach (var nodeDef in graph.Nodes)
        {
            var contract = BuildRuntimeStreamContract(nodeDef, lineageEnabled);
            normalizedAnnotations[ExecutionAnnotationKeys.RuntimeStreamContractForNode(nodeDef.Id)] = contract;

            if (nodeDef.Kind != NodeKind.Route)
                continue;

            var routeKey = ExecutionAnnotationKeys.RouteOptionsForNode(nodeDef.Id);

            if (!normalizedAnnotations.TryGetValue(routeKey, out var routeOptions) || routeOptions is null)
                continue;

            normalizedAnnotations[routeKey] = NormalizeRouteOptions(nodeDef, contract, routeOptions);
        }

        return normalizedAnnotations.ToImmutable();
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
                               ?? throw new InvalidOperationException(ErrorMessages.RouteNodeMissingOutputType(nodeDef.Id));

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
                        $"Expected runtime route item type '{TypeNameFormatter.GetAssemblyQualifiedName(expectedLineageItemType)}' " +
                        $"but resolved '{TypeNameFormatter.GetAssemblyQualifiedName(expectedItemType)}'.");
                }

                return AdaptLineageRouteOptions(payloadType, routeOptions);
            }
        }

        throw new InvalidOperationException(
            $"Route options type mismatch for route node '{nodeDef.Id}'. " +
            $"Expected '{TypeNameFormatter.GetAssemblyQualifiedName(expectedRouteOptionsType)}' " +
            $"but got '{TypeNameFormatter.GetAssemblyQualifiedName(actualRouteOptionsType)}'.");
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

    private static IDeadLetterSink? ResolveDeadLetterSink(PipelineGraph graph, IErrorHandlerFactory errorHandlerFactory, List<object> runOwned)
    {
        if (graph.ErrorHandling.DeadLetterSink is not null)
            return graph.ErrorHandling.DeadLetterSink;

        if (graph.ErrorHandling.DeadLetterSinkType is null)
            return null;

        var created = errorHandlerFactory.CreateDeadLetterSink(graph.ErrorHandling.DeadLetterSinkType);

        if (created is not null && errorHandlerFactory.CallerOwnsCreatedInstance(created))
            runOwned.Add(created);

        return created;
    }

    private static ILineageSink? ResolveLineageSink(PipelineGraph graph, ILineageFactory lineageFactory, PipelineContext context,
        List<object> runOwned)
    {
        if (graph.Lineage.LineageSink is not null)
            return graph.Lineage.LineageSink;

        if (graph.Lineage.LineageSinkType is not null)
        {
            var created = lineageFactory.CreateLineageSink(graph.Lineage.LineageSinkType);

            if (created is not null && lineageFactory.CallerOwnsCreatedInstance(created))
                runOwned.Add(created);

            return created;
        }

        if (context.Lineage.LineageSink is not null)
            return context.Lineage.LineageSink;

        return null;
    }

    private static IPipelineLineageSink? ResolvePipelineLineageSink(PipelineGraph graph, ILineageFactory lineageFactory, PipelineContext context,
        List<object> runOwned)
    {
        if (graph.Lineage.PipelineLineageSink is not null)
            return graph.Lineage.PipelineLineageSink;

        if (graph.Lineage.PipelineLineageSinkType is not null)
        {
            var created = lineageFactory.CreatePipelineLineageSink(graph.Lineage.PipelineLineageSinkType);

            if (created is not null && lineageFactory.CallerOwnsCreatedInstance(created))
                runOwned.Add(created);

            return created;
        }

        if (context.Lineage.PipelineLineageSink is not null)
            return context.Lineage.PipelineLineageSink;

        // Provider-based default (no reflection):
        // When no explicit sink is configured, attempt to resolve a provider (supplied by optional packages
        // like NPipeline.Lineage) and let it create the default sink. A provider is only present when the
        // caller opted into lineage, so this does not turn reporting on for pipelines that never asked for it.
        //
        // Deliberately not gated on ItemLevelLineageEnabled: a PipelineLineageReport is derived purely from the
        // graph (nodes, edges, declared types), so it costs nothing to produce and needs no per-item tracking.
        var provider = lineageFactory.ResolvePipelineLineageSinkProvider();

        return provider?.Create(context);
    }
}
