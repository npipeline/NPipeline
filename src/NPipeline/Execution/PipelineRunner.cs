using System.Collections.Immutable;
using System.Runtime.ExceptionServices;
using NPipeline.Attributes;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Caching;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Plans;
using NPipeline.Execution.Pooling;
using NPipeline.Execution.Services;
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
    INodeExecutor nodeExecutor,
    ITopologyService topologyService,
    INodeInstantiationService nodeInstantiationService,
    IErrorHandlingService errorHandlingService,
    IPersistenceService persistenceService,
    IObservabilitySurface observabilitySurface,
    IPipelineExecutionPlanCache? executionPlanCache = null,
    IRuntimePipelineBinder? runtimePipelineBinder = null) : IPipelineRunner
{
    private readonly IPipelineExecutionPlanCache _executionPlanCache = executionPlanCache ?? new InMemoryPipelineExecutionPlanCache();
    private readonly IRuntimePipelineBinder _runtimePipelineBinder = runtimePipelineBinder ?? RuntimePipelineBinder.Instance;

    /// <inheritdoc />
    public async Task RunAsync<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        await RunAsync<TDefinition>(context, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RunAsync(IPipelineDefinition definition, PipelineContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(context);

        await RunAsyncCoreAsync(
                definition.GetType(),
                context,
                (factory, runtimeContext) => factory.Create(definition, runtimeContext),
                cancellationToken)
            .ConfigureAwait(false);
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

        await RunAsyncCoreAsync(
                typeof(TDefinition),
                context,
                static (pipelineFactory, runtimeContext) => pipelineFactory.Create<TDefinition>(runtimeContext),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task RunAsyncCoreAsync(
        Type definitionType,
        PipelineContext context,
        Func<IPipelineFactory, PipelineContext, Pipeline.Pipeline> createPipeline,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(createPipeline);

        if (string.IsNullOrWhiteSpace(context.PipelineName))
            context.PipelineName = PipelineAttributeHelper.GetPipelineName(definitionType);

        using var pipelineActivity = observabilitySurface.BeginPipeline(definitionType, context);
        PipelineGraph? graph = null;
        InitializeExecutionContext(context);

        // Rent pooled dictionaries for node outputs and instances
        var nodeOutputs = PipelineObjectPool.RentNodeOutputDictionary();
        Dictionary<string, INode>? nodeInstances = null;
        var pipelineCompleted = false;

        try
        {
            var pipeline = createPipeline(pipelineFactory, context);
            graph = pipeline.Graph;
            var runtimeBinding = await _runtimePipelineBinder.BindAsync(graph, context).ConfigureAwait(false);
            graph = runtimeBinding.Graph;
            ApplyRuntimeBindings(context, runtimeBinding);

            nodeOutputs.EnsureCapacity(graph.Nodes.Length);

            await VisualizeIfConfiguredAsync(graph, cancellationToken).ConfigureAwait(false);
            ApplyRetryOptions(graph, context);
            ConfigureCircuitBreaker(graph, context);
            var pipelineLineageSink = runtimeBinding.PipelineLineageSink;

            nodeInstances = nodeInstantiationService.InstantiateNodes(graph, nodeFactory);
            ApplyNodeExecutionStrategies(graph, nodeInstances);
            ApplyGlobalExecutionAnnotations(graph, context);
            ApplyGlobalServicesFromProperties(context);

            // Ensure NodeDefinitionMap is populated (for cases where graph is constructed directly without using builder)
            graph = graph.EnsureNodeDefinitionMapInitialized();
            var nodeDefinitionMap = graph.NodeDefinitionMap;

            var executionPlans = BuildExecutionPlans(definitionType, graph, nodeInstances);
            ApplyStatefulRegistryFromProperties(context);
            nodeInstantiationService.RegisterStatefulNodes(nodeInstances, context);

            await ExecuteNodesInOrderAsync(graph, context, nodeInstances, nodeDefinitionMap, executionPlans, nodeOutputs).ConfigureAwait(false);
            await RecordPipelineLineageAsync(definitionType, graph, context, pipelineLineageSink).ConfigureAwait(false);

            pipelineCompleted = true;
        }
        catch (Exception ex)
        {
            await HandlePipelineFailureAsync(definitionType, context, ex, pipelineActivity).ConfigureAwait(false);
        }
        finally
        {
            await CleanupResourcesAsync(definitionType, context, graph, pipelineActivity, nodeOutputs, nodeInstances, pipelineCompleted)
                .ConfigureAwait(false);
        }
    }

    private static void InitializeExecutionContext(PipelineContext context)
    {
        context.PipelineStartTimeUtc = DateTime.UtcNow;

        if (context.PipelineId == Guid.Empty)
            context.PipelineId = Guid.NewGuid();

        if (context.RunId == Guid.Empty)
            context.RunId = Guid.NewGuid();

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

    private static void ApplyRuntimeBindings(PipelineContext context, RuntimePipelineBindingResult runtimeBinding)
    {
        context.LineageSink = runtimeBinding.LineageSink;
        context.PipelineLineageSink = runtimeBinding.PipelineLineageSink;

        if (runtimeBinding.PipelineErrorHandler is not null)
            context.PipelineErrorHandler = runtimeBinding.PipelineErrorHandler;

        if (runtimeBinding.DeadLetterSink is not null)
            context.DeadLetterSink = runtimeBinding.DeadLetterSink;
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

    private Dictionary<string, NodeExecutionPlan> BuildExecutionPlans(
        Type definitionType,
        PipelineGraph graph,
        Dictionary<string, INode> nodeInstances)
    {
        return ShouldUseCache(graph)
            ? BuildPlansWithCache(definitionType, graph, nodeInstances)
            : nodeInstantiationService.BuildPlans(graph, nodeInstances);
    }

    private Dictionary<string, NodeExecutionPlan> BuildPlansWithCache(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        IReadOnlyDictionary<string, INode> nodeInstances)
    {
        ArgumentNullException.ThrowIfNull(pipelineDefinitionType);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(nodeInstances);

        if (_executionPlanCache.TryGetCachedPlans(pipelineDefinitionType, graph, out var cachedPlans) && cachedPlans is not null)
            return cachedPlans;

        var plans = nodeInstantiationService.BuildPlans(graph, nodeInstances);
        _executionPlanCache.CachePlans(pipelineDefinitionType, graph, plans);

        return plans;
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
        var inputLookup = topologyService.BuildInputLookup(graph);
        var sortedNodes = topologyService.TopologicalSort(graph);

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
                persistenceService.TryPersistAfterNode(context, failedEvent);
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
        await errorHandlingService.ExecuteWithRetriesAsync(
            nodeDef,
            nodeInstance,
            graph,
            context,
            async () =>
            {
                var plan = executionPlans[nodeDef.Id];

                await nodeExecutor.ExecuteAsync(
                    plan,
                    graph,
                    context,
                    inputLookup,
                    nodeOutputs,
                    nodeInstances,
                    nodeDefinitionMap).ConfigureAwait(false);

                var completedEvent = observabilitySurface.CompleteNodeSuccess(context, nodeScope);
                context.Properties[$"NodeCompleted_{nodeDef.Id}"] = true;
                persistenceService.TryPersistAfterNode(context, completedEvent);
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

    private static async Task RecordPipelineLineageAsync(
        Type definitionType,
        PipelineGraph graph,
        PipelineContext context,
        IPipelineLineageSink? pipelineLineageSink)
    {
        if (!graph.Lineage.ItemLevelLineageEnabled || pipelineLineageSink is null)
            return;

        var runId = context.RunId == Guid.Empty
            ? Guid.NewGuid()
            : context.RunId;

        var report = context.LineageFactory.CreateLineageReport(definitionType.Name, context.PipelineId, graph, runId);

        if (report is null)
            return;

        await pipelineLineageSink.RecordAsync(report, context.CancellationToken).ConfigureAwait(false);
    }

    private async Task HandlePipelineFailureAsync(
        Type definitionType,
        PipelineContext context,
        Exception ex,
        IPipelineActivity pipelineActivity)
    {
        await observabilitySurface.FailPipeline(definitionType, context, ex, pipelineActivity).ConfigureAwait(false);

        if (context.IsParallelExecution)
            ExceptionDispatchInfo.Capture(ex).Throw();

        if (ex is OperationCanceledException)
            ExceptionDispatchInfo.Capture(ex).Throw();

        if (ex is not PipelineException)
            throw new PipelineExecutionException(ErrorMessages.PipelineExecutionFailed(definitionType.Name, ex), ex);

        ExceptionDispatchInfo.Capture(ex).Throw();
    }

    private async Task CleanupResourcesAsync(
        Type definitionType,
        PipelineContext context,
        PipelineGraph? graph,
        IPipelineActivity pipelineActivity,
        Dictionary<string, IDataStream?> nodeOutputs,
        Dictionary<string, INode>? nodeInstances,
        bool pipelineCompleted)
    {
        if (pipelineCompleted && graph is not null)
            await observabilitySurface.CompletePipeline(definitionType, context, graph, pipelineActivity).ConfigureAwait(false);

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
