using System.Collections.Immutable;
using System.Runtime.ExceptionServices;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Caching;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Plans;
using NPipeline.Execution.Pooling;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.State;

namespace NPipeline.Execution;

/// <summary>
///     Executes a pipeline defined by a <see cref="PipelineGraph" />.
///     <para>
///         The PipelineRunner is the main entry point for pipeline execution, coordinating all aspects
///         of pipeline processing including node instantiation, execution flow, error handling, and resource management.
///     </para>
///     <para>
///         Performance optimizations:
///         - Implements reflection-free steady state execution with pre-built execution plans
///         - Supports parallel sink execution when inputs are fully materialized
///     </para>
/// </summary>
/// <remarks>
///     This class is designed to be used with dependency injection or created via <see cref="Create" />.
/// </remarks>
public sealed class PipelineRunner(
    IPipelineFactory pipelineFactory,
    INodeFactory nodeFactory,
    IPipelineExecutionCoordinator executionCoordinator,
    IPipelineInfrastructureService infrastructureService,
    IObservabilitySurface observabilitySurface,
    IPipelineExecutionPlanCache? executionPlanCache = null) : IPipelineRunner
{
    private readonly IPipelineExecutionPlanCache _executionPlanCache = executionPlanCache ?? new InMemoryPipelineExecutionPlanCache();

    /// <inheritdoc />
    public async Task RunAsync<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        await RunAsync<TDefinition>(context, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    ///     Creates a new PipelineRunner with default factories for simple use cases.
    ///     This method provides the most straightforward way to execute pipelines without
    ///     requiring explicit configuration of all dependencies.
    /// </summary>
    /// <example>
    ///     <code>
    ///     var runner = PipelineRunner.Create();
    ///     await runner.RunAsync&lt;MyPipelineDefinition&gt;();
    ///     </code>
    /// </example>
    public static PipelineRunner Create()
    {
        return new PipelineRunnerBuilder().Build();
    }

    /// <summary>
    ///     Runs a pipeline defined by <typeparamref name="TDefinition" /> using a default context and cancellation token.
    ///     This is the simplest way to execute a pipeline when no custom context is needed.
    /// </summary>
    /// <typeparam name="TDefinition">Pipeline definition type implementing <see cref="IPipelineDefinition" />.</typeparam>
    /// <param name="cancellationToken">Optional cancellation token for the pipeline execution.</param>
    /// <exception cref="PipelineExecutionException">Thrown when pipeline execution fails.</exception>
    /// <exception cref="NodeExecutionException">Thrown when a specific node fails execution.</exception>
    /// <exception cref="CircuitBreakerTrippedException">Thrown when the circuit breaker trips due to too many failures.</exception>
    /// <exception cref="RetryExhaustedException">Thrown when all retry attempts are exhausted.</exception>
    /// <example>
    ///     <code>
    ///     var runner = PipelineRunner.Create();
    ///     await runner.RunAsync&lt;MyDataProcessingPipeline&gt;(cancellationToken);
    ///     </code>
    /// </example>
    public async Task RunAsync<TDefinition>(CancellationToken cancellationToken = default) where TDefinition : IPipelineDefinition, new()
    {
        await using var context = new PipelineContext(
            PipelineContextConfiguration.WithCancellation(cancellationToken));

        await RunAsync<TDefinition>(context, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Runs a pipeline defined by <typeparamref name="TDefinition" /> using the provided <paramref name="context" /> and <paramref name="cancellationToken" />.
    ///     This method provides full control over the execution environment through the context parameter.
    /// </summary>
    /// <typeparam name="TDefinition">Pipeline definition type implementing <see cref="IPipelineDefinition" />.</typeparam>
    /// <param name="context">Execution context providing logging, tracing, factories, and parameters.</param>
    /// <param name="cancellationToken">Cancellation token for the pipeline execution.</param>
    /// <exception cref="PipelineExecutionException">Thrown when pipeline execution fails.</exception>
    /// <exception cref="NodeExecutionException">Thrown when a specific node fails execution.</exception>
    /// <exception cref="CircuitBreakerTrippedException">Thrown when the circuit breaker trips due to too many failures.</exception>
    /// <exception cref="RetryExhaustedException">Thrown when all retry attempts are exhausted.</exception>
    /// <example>
    ///     <code>
    ///     var context = PipelineContext.Default;
    ///     context.Items["customSetting"] = "value";
    ///     var runner = PipelineRunner.Create();
    ///     await runner.RunAsync&lt;MyDataProcessingPipeline&gt;(context, cancellationToken);
    ///     </code>
    /// </example>
    /// <remarks>
    ///     The execution process follows these steps:
    ///     1. Creates the pipeline instance from the definition
    ///     2. Instantiates all nodes in the graph
    ///     3. Builds execution plans for optimized performance
    ///     4. Executes nodes in topological order with error handling
    ///     5. Manages resource cleanup and persistence
    /// </remarks>
    public async Task RunAsync<TDefinition>(PipelineContext context, CancellationToken cancellationToken) where TDefinition : IPipelineDefinition, new()
    {
        ArgumentNullException.ThrowIfNull(context);

        using var pipelineActivity = observabilitySurface.BeginPipeline<TDefinition>(context);
        PipelineGraph? graph = null;
        InitializeExecutionContext(context);

        // Rent pooled dictionaries for node outputs and instances
        var nodeOutputs = PipelineObjectPool.RentNodeOutputDictionary();
        Dictionary<string, INode>? nodeInstances = null;
        var pipelineCompleted = false;

        try
        {
            var pipeline = pipelineFactory.Create<TDefinition>(context);
            graph = pipeline.Graph;
            graph = ApplyRuntimeItemLevelLineageOverride(graph, context);
            graph = ApplyRuntimeLineageOptionsOverride(graph, context);
            nodeOutputs.EnsureCapacity(graph.Nodes.Length);

            await VisualizeIfConfiguredAsync(graph, cancellationToken).ConfigureAwait(false);
            ApplyRetryOptions(graph, context);
            ConfigureCircuitBreaker(graph, context);
            var pipelineLineageSink = ResolveAndApplyExecutionHandlers(graph, context);

            nodeInstances = executionCoordinator.InstantiateNodes(graph, nodeFactory);
            ApplyNodeExecutionStrategies(graph, nodeInstances);
            ApplyGlobalExecutionAnnotations(graph, context);
            ApplyGlobalServicesFromProperties(context);

            // Ensure NodeDefinitionMap is populated (for cases where graph is constructed directly without using builder)
            graph = graph.EnsureNodeDefinitionMapInitialized();
            var nodeDefinitionMap = graph.NodeDefinitionMap;

            var executionPlans = BuildExecutionPlans<TDefinition>(graph, nodeInstances);
            ApplyStatefulRegistryFromProperties(context);
            executionCoordinator.RegisterStatefulNodes(nodeInstances, context);

            await ExecuteNodesInOrderAsync(graph, context, nodeInstances, nodeDefinitionMap, executionPlans, nodeOutputs).ConfigureAwait(false);
            await RecordPipelineLineageAsync<TDefinition>(graph, context, pipelineLineageSink).ConfigureAwait(false);

            pipelineCompleted = true;
        }
        catch (Exception ex)
        {
            await HandlePipelineFailureAsync<TDefinition>(context, ex, pipelineActivity).ConfigureAwait(false);
        }
        finally
        {
            await CleanupResourcesAsync<TDefinition>(context, graph, pipelineActivity, nodeOutputs, nodeInstances, pipelineCompleted)
                .ConfigureAwait(false);
        }
    }

    private static void InitializeExecutionContext(PipelineContext context)
    {
        context.PipelineStartTimeUtc = DateTime.UtcNow;
        context.ProcessedItemsCounter = new StatsCounter();
        context.GlobalRetryOptions = context.RetryOptions;
        context.NodeRetryOverrides.Clear();
        context.NodeExecutionAnnotations.Clear();
        context.NodeObservabilityScopes.Clear();
        context.RuntimeAnnotations.Clear();
        context.IsParallelExecution = false;
        context.LastRetryExhaustedException = null;
    }

    private static async Task VisualizeIfConfiguredAsync(PipelineGraph graph, CancellationToken cancellationToken)
    {
        if (graph.ExecutionOptions.Visualizer is not null)
            await graph.ExecutionOptions.Visualizer.VisualizeAsync(graph, cancellationToken).ConfigureAwait(false);
    }

    private static void ApplyRetryOptions(PipelineGraph graph, PipelineContext context)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(PipelineRunner));

        if (graph.ErrorHandling.RetryOptions is not null)
        {
            PipelineRunnerLogMessages.StoringRetryOptions(logger, graph.ErrorHandling.RetryOptions.MaxItemRetries);
            context.GlobalRetryOptions = graph.ErrorHandling.RetryOptions;
        }
        else
        {
            PipelineRunnerLogMessages.RetryOptionsNull(logger);
            context.GlobalRetryOptions = context.RetryOptions;
        }

        if (graph.ErrorHandling.NodeRetryOverrides is not { Count: > 0 })
            return;

        foreach (var kvp in graph.ErrorHandling.NodeRetryOverrides)
        {
            context.NodeRetryOverrides[kvp.Key] = kvp.Value;
        }
    }

    private static void ConfigureCircuitBreaker(PipelineGraph graph, PipelineContext context)
    {
        if (graph.ErrorHandling.CircuitBreakerOptions is null)
        {
            context.CircuitBreakerOptions = null;
            context.CircuitBreakerManager = null;
            context.CircuitBreakerMemoryOptions = null;
            return;
        }

        context.CircuitBreakerOptions = graph.ErrorHandling.CircuitBreakerOptions;
        var memoryOptions = graph.ErrorHandling.CircuitBreakerMemoryOptions;
        context.CircuitBreakerMemoryOptions = memoryOptions;

        if (!graph.ErrorHandling.CircuitBreakerOptions.Enabled)
        {
            context.CircuitBreakerManager = null;
            context.CircuitBreakerMemoryOptions = null;
            return;
        }

        var managerLogger = context.LoggerFactory.CreateLogger(nameof(CircuitBreakerManager));
        var circuitBreakerManager = context.CreateAndRegister(new CircuitBreakerManager(managerLogger, memoryOptions));
        context.CircuitBreakerManager = circuitBreakerManager;
        PipelineRunnerLogMessages.CircuitBreakerManagerCreated(managerLogger);
    }

    private static PipelineGraph ApplyRuntimeLineageOptionsOverride(PipelineGraph graph, PipelineContext context)
    {
        if (!context.Properties.TryGetValue(PipelineContextKeys.LineageOptionsOverride, out var overrideObj) || overrideObj is null)
            return graph;

        LineageOptions? resolved = overrideObj switch
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
        {
            return graph;
        }

        var resolvedOptions = graph.Lineage.LineageOptions;
        if (enabled && resolvedOptions is null)
        {
            // Mirror PipelineBuilder.EnableItemLevelLineage() defaults for runtime enablement.
            resolvedOptions = new LineageOptions(SampleEvery: 1, RedactData: false);
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

    private static IPipelineLineageSink? ResolveAndApplyExecutionHandlers(PipelineGraph graph, PipelineContext context)
    {
        var errorHandler = ResolvePipelineErrorHandler(graph, context.ErrorHandlerFactory);
        var deadLetterSink = ResolveDeadLetterSink(graph, context.ErrorHandlerFactory);
        deadLetterSink = ApplyDeadLetterSinkDecorator(context, deadLetterSink);

        var lineageSink = graph.Lineage.ItemLevelLineageEnabled
            ? ResolveLineageSink(graph, context.LineageFactory, context)
            : null;

        lineageSink = ApplyLineageSinkDecorator(context, lineageSink);

        var pipelineLineageSink = ResolvePipelineLineageSink(graph, context.LineageFactory, context);

        context.LineageSink = lineageSink;
        context.PipelineLineageSink = pipelineLineageSink;

        if (errorHandler is not null)
            context.PipelineErrorHandler = errorHandler;

        if (deadLetterSink is not null)
            context.DeadLetterSink = deadLetterSink;

        return pipelineLineageSink;
    }

    private static IDeadLetterSink? ApplyDeadLetterSinkDecorator(PipelineContext context, IDeadLetterSink? deadLetterSink)
    {
        if (context.Properties.TryGetValue(PipelineContextKeys.DeadLetterSinkDecorator, out var decoratorObj) &&
            decoratorObj is Func<IDeadLetterSink?, IDeadLetterSink?> decorator)
        {
            return decorator(deadLetterSink);
        }

        return deadLetterSink;
    }

    private static ILineageSink? ApplyLineageSinkDecorator(PipelineContext context, ILineageSink? lineageSink)
    {
        if (context.Properties.TryGetValue(PipelineContextKeys.LineageSinkDecorator, out var decoratorObj) &&
            decoratorObj is Func<ILineageSink?, ILineageSink?> decorator)
        {
            return decorator(lineageSink);
        }

        return lineageSink;
    }

    private static void ApplyNodeExecutionStrategies(PipelineGraph graph, IReadOnlyDictionary<string, INode> nodeInstances)
    {
        foreach (var def in graph.Nodes)
        {
            if (def.ExecutionStrategy is not null && nodeInstances.TryGetValue(def.Id, out var inst) && inst is ITransformNode transformNode)
                transformNode.ExecutionStrategy = def.ExecutionStrategy;
        }
    }

    private static void ApplyGlobalExecutionAnnotations(PipelineGraph graph, PipelineContext context)
    {
        foreach (var kv in graph.ExecutionOptions.NodeExecutionAnnotations ?? ImmutableDictionary<string, object>.Empty)
        {
            if (!kv.Key.StartsWith(ExecutionAnnotationKeys.GlobalAnnotationPrefix, StringComparison.Ordinal))
                continue;

            var trimmed = kv.Key.Substring(ExecutionAnnotationKeys.GlobalAnnotationPrefix.Length);
            var newKey = $"{ExecutionAnnotationKeys.GlobalPropertyPrefix}{trimmed}";
            context.Properties[newKey] = kv.Value;
        }
    }

    private static void ApplyGlobalServicesFromProperties(PipelineContext context)
    {
        if (context.Properties.TryGetValue(ExecutionAnnotationKeys.GlobalPropertyPrefix + "NPipeline.StateManager", out var sm))
            context.StateManager = sm as IPipelineStateManager;

        if (context.Properties.TryGetValue(ExecutionAnnotationKeys.ExecutionObserverProperty, out var eo) && eo is IExecutionObserver execObs)
            context.ExecutionObserver = execObs;
    }

    private Dictionary<string, NodeExecutionPlan> BuildExecutionPlans<TDefinition>(PipelineGraph graph, Dictionary<string, INode> nodeInstances)
        where TDefinition : IPipelineDefinition, new()
    {
        return ShouldUseCache(graph)
            ? executionCoordinator.BuildPlansWithCache(typeof(TDefinition), graph, nodeInstances, _executionPlanCache)
            : executionCoordinator.BuildPlans(graph, nodeInstances);
    }

    private static void ApplyStatefulRegistryFromProperties(PipelineContext context)
    {
        if (context.Properties.TryGetValue("NPipeline.Global.NPipeline.State.StatefulRegistry", out var regObj))
            context.StatefulRegistry = regObj as IStatefulRegistry;
    }

    private async Task ExecuteNodesInOrderAsync(
        PipelineGraph graph,
        PipelineContext context,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap,
        IReadOnlyDictionary<string, NodeExecutionPlan> executionPlans,
        IDictionary<string, IDataStream?> nodeOutputs)
    {
        var inputLookup = executionCoordinator.BuildInputLookup(graph);
        var sortedNodes = executionCoordinator.TopologicalSort(graph);

        foreach (var nodeDef in sortedNodes.Select(id => nodeDefinitionMap[id]))
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            using var nodeScopeHandle = context.ScopedNode(nodeDef.Id);

            ApplyPerNodeExecutionAnnotation(graph, context, nodeDef.Id);

            var nodeInstance = nodeInstances[nodeDef.Id];
            var nodeScope = observabilitySurface.BeginNode(context, graph, nodeDef, nodeInstance);

            try
            {
                await ExecuteNodeWithRetriesAsync(
                    nodeDef,
                    nodeInstance,
                    graph,
                    context,
                    nodeScope,
                    executionPlans,
                    inputLookup,
                    nodeOutputs,
                    nodeInstances,
                    nodeDefinitionMap).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                context.Properties[$"NodeError_{nodeDef.Id}"] = true;
                var failedEvent = observabilitySurface.CompleteNodeFailure(context, nodeScope, ex);
                infrastructureService.TryPersistAfterNode(context, failedEvent);
                HandleNodeExecutionException(nodeDef, context, ex);
            }
        }
    }

    private static void ApplyPerNodeExecutionAnnotation(PipelineGraph graph, PipelineContext context, string nodeId)
    {
        if (graph.ExecutionOptions.NodeExecutionAnnotations != null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(nodeId, out var annotation))
            context.NodeExecutionAnnotations[nodeId] = annotation;
        else
            _ = context.NodeExecutionAnnotations.Remove(nodeId);
    }

    private async Task ExecuteNodeWithRetriesAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        PipelineGraph graph,
        PipelineContext context,
        NodeObservationScope nodeScope,
        IReadOnlyDictionary<string, NodeExecutionPlan> executionPlans,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap)
    {
        await infrastructureService.ExecuteWithRetriesAsync(
            nodeDef,
            nodeInstance,
            graph,
            context,
            async () =>
            {
                var plan = executionPlans[nodeDef.Id];

                await executionCoordinator.ExecuteNodeAsync(
                    plan,
                    graph,
                    context,
                    inputLookup,
                    nodeOutputs,
                    nodeInstances,
                    nodeDefinitionMap).ConfigureAwait(false);

                var completedEvent = observabilitySurface.CompleteNodeSuccess(context, nodeScope);
                context.Properties[$"NodeCompleted_{nodeDef.Id}"] = true;
                infrastructureService.TryPersistAfterNode(context, completedEvent);
            },
            context.CancellationToken).ConfigureAwait(false);
    }

    private static void HandleNodeExecutionException(NodeDefinition nodeDef, PipelineContext context, Exception ex)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(PipelineRunner));
        PipelineRunnerLogMessages.NodeFailed(logger, nodeDef.Id, ex.GetType().Name, ex.Message);

        if (context.PipelineErrorHandler != null && nodeDef.ExecutionStrategy?.GetType().Name == "ResilientExecutionStrategy")
        {
            var effectiveRetries = RetryOptionsResolver.Resolve(context, nodeDef.Id);

            if (effectiveRetries.MaxNodeRestartAttempts <= 0)
                PipelineRunnerLogMessages.ResilientStrategyWithoutRestartAttempts(logger, nodeDef.Id, effectiveRetries.MaxNodeRestartAttempts);

            if (effectiveRetries.MaxMaterializedItems == null)
                PipelineRunnerLogMessages.ResilientStrategyWithoutMaterializedItems(logger, nodeDef.Id);
        }

        if (context.IsParallelExecution)
        {
            PipelineRunnerLogMessages.PreservingExceptionForParallelExecution(logger, ex.GetType().Name, nodeDef.Id);
            ExceptionDispatchInfo.Capture(ex).Throw();
        }

        if (ex is OperationCanceledException)
        {
            PipelineRunnerLogMessages.PreservingCancellationException(logger, nodeDef.Id);
            ExceptionDispatchInfo.Capture(ex).Throw();
        }

        if (ex is not PipelineException)
        {
            PipelineRunnerLogMessages.WrappingException(logger, ex.GetType().Name, nodeDef.Id);
            throw new PipelineExecutionException(ErrorMessages.PipelineExecutionFailedAtNode(nodeDef.Id, ex), ex);
        }

        ExceptionDispatchInfo.Capture(ex).Throw();
    }

    private static async Task RecordPipelineLineageAsync<TDefinition>(
        PipelineGraph graph,
        PipelineContext context,
        IPipelineLineageSink? pipelineLineageSink)
        where TDefinition : IPipelineDefinition, new()
    {
        if (!graph.Lineage.ItemLevelLineageEnabled || pipelineLineageSink is null)
            return;

        var report = LineageGenerator.Generate(typeof(TDefinition).Name, graph, Guid.NewGuid());
        await pipelineLineageSink.RecordAsync(report, context.CancellationToken).ConfigureAwait(false);
    }

    private async Task HandlePipelineFailureAsync<TDefinition>(
        PipelineContext context,
        Exception ex,
        IPipelineActivity pipelineActivity)
        where TDefinition : IPipelineDefinition, new()
    {
        await observabilitySurface.FailPipeline<TDefinition>(context, ex, pipelineActivity).ConfigureAwait(false);

        if (context.IsParallelExecution)
            ExceptionDispatchInfo.Capture(ex).Throw();

        if (ex is OperationCanceledException)
            ExceptionDispatchInfo.Capture(ex).Throw();

        if (ex is not PipelineException)
            throw new PipelineExecutionException(ErrorMessages.PipelineExecutionFailed(typeof(TDefinition).Name, ex), ex);

        ExceptionDispatchInfo.Capture(ex).Throw();
    }

    private async Task CleanupResourcesAsync<TDefinition>(
        PipelineContext context,
        PipelineGraph? graph,
        IPipelineActivity pipelineActivity,
        Dictionary<string, IDataStream?> nodeOutputs,
        Dictionary<string, INode>? nodeInstances,
        bool pipelineCompleted)
        where TDefinition : IPipelineDefinition, new()
    {
        if (pipelineCompleted && graph is not null)
            await observabilitySurface.CompletePipeline<TDefinition>(context, graph, pipelineActivity).ConfigureAwait(false);

        foreach (var kvp in nodeOutputs)
        {
            if (kvp.Value is not null)
                await kvp.Value.DisposeAsync().ConfigureAwait(false);
        }

        nodeOutputs.Clear();
        PipelineObjectPool.Return(nodeOutputs);

        if (nodeInstances is null)
            return;

        if (!context.DiOwnedNodes)
        {
            foreach (var node in nodeInstances.Values)
            {
                await node.DisposeAsync().ConfigureAwait(false);
            }
        }

        nodeInstances.Clear();
        PipelineObjectPool.Return(nodeInstances);
    }

    private static IPipelineErrorHandler? ResolvePipelineErrorHandler(PipelineGraph graph, IErrorHandlerFactory errorHandlerFactory)
    {
        if (graph.ErrorHandling.PipelineErrorHandler is not null)
            return graph.ErrorHandling.PipelineErrorHandler;

        if (graph.ErrorHandling.PipelineErrorHandlerType is not null)
            return errorHandlerFactory.CreateErrorHandler(graph.ErrorHandling.PipelineErrorHandlerType);

        return null;
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

    /// <summary>
    ///     Determines whether execution plan caching should be used for the given pipeline graph.
    /// </summary>
    /// <remarks>
    ///     Caching is disabled when:
    ///     - The graph has preconfigured node instances (they may have runtime state)
    ///     - The cache is explicitly set to NullPipelineExecutionPlanCache
    ///     This ensures cache safety while maximizing performance for typical scenarios.
    /// </remarks>
    private bool ShouldUseCache(PipelineGraph graph)
    {
        // Don't cache if using NullCache (explicitly disabled)
        if (_executionPlanCache is NullPipelineExecutionPlanCache)
            return false;

        // Don't cache if graph has preconfigured instances (may have runtime state)
        if (graph.PreconfiguredNodeInstances.Count > 0)
            return false;

        return true;
    }
}
