using System.Collections.Immutable;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

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
    IObservabilitySurface observabilitySurface) : IPipelineRunner
{
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
        var context = new PipelineContextBuilder()
            .WithCancellation(cancellationToken)
            .Build();

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

        // Initialize pipeline stats if missing
        if (!context.Items.ContainsKey(PipelineContextKeys.PipelineStartTimeUtc))
            context.Items[PipelineContextKeys.PipelineStartTimeUtc] = DateTime.UtcNow;

        if (!context.Items.TryGetValue(PipelineContextKeys.TotalProcessedItems, out var statsObj) || statsObj is not StatsCounter)
            context.Items[PipelineContextKeys.TotalProcessedItems] = new StatsCounter();

        // Initialize nodeOutputs dictionary once
        var nodeOutputs = new Dictionary<string, IDataPipe?>();
        var nodeInstances = new Dictionary<string, INode>();

        try
        {
            var pipeline = pipelineFactory.Create<TDefinition>(context);
            graph = pipeline.Graph;

            if (graph.ExecutionOptions.Visualizer is not null)
                await graph.ExecutionOptions.Visualizer.VisualizeAsync(graph, cancellationToken).ConfigureAwait(false);

            if (graph.ErrorHandling.RetryOptions is not null)
            {
                var logger = context.LoggerFactory.CreateLogger(nameof(PipelineRunner));

                logger.Log(LogLevel.Debug, "Storing retry options in context.Items: MaxItemRetries={MaxItemRetries}",
                    graph.ErrorHandling.RetryOptions.MaxItemRetries);

                context.Items[PipelineContextKeys.GlobalRetryOptions] = graph.ErrorHandling.RetryOptions;
            }
            else
            {
                var logger = context.LoggerFactory.CreateLogger(nameof(PipelineRunner));
                logger.Log(LogLevel.Debug, "graph.ErrorHandling.RetryOptions is null");
            }

            // Surface circuit breaker options to downstream execution strategies via context.Items.
            if (graph.ErrorHandling.CircuitBreakerOptions is not null)
            {
                context.Items[PipelineContextKeys.CircuitBreakerOptions] = graph.ErrorHandling.CircuitBreakerOptions;
                var memoryOptions = graph.ErrorHandling.CircuitBreakerMemoryOptions;

                if (memoryOptions is not null)
                    context.Items[PipelineContextKeys.CircuitBreakerMemoryOptions] = memoryOptions;
                else
                    _ = context.Items.Remove(PipelineContextKeys.CircuitBreakerMemoryOptions);

                if (graph.ErrorHandling.CircuitBreakerOptions.Enabled)
                {
                    var managerLogger = context.LoggerFactory.CreateLogger(nameof(CircuitBreakerManager));
                    var circuitBreakerManager = context.CreateAndRegister(new CircuitBreakerManager(managerLogger, memoryOptions));
                    context.Items[PipelineContextKeys.CircuitBreakerManager] = circuitBreakerManager;
                    managerLogger.Log(LogLevel.Debug, "CircuitBreakerManager created and stored in context");
                }
                else
                {
                    _ = context.Items.Remove(PipelineContextKeys.CircuitBreakerManager);
                    _ = context.Items.Remove(PipelineContextKeys.CircuitBreakerMemoryOptions);
                }
            }
            else
            {
                _ = context.Items.Remove(PipelineContextKeys.CircuitBreakerOptions);
                _ = context.Items.Remove(PipelineContextKeys.CircuitBreakerManager);
                _ = context.Items.Remove(PipelineContextKeys.CircuitBreakerMemoryOptions);
            }

            var errorHandler = ResolvePipelineErrorHandler(graph, context.ErrorHandlerFactory);
            var deadLetterSink = ResolveDeadLetterSink(graph, context.ErrorHandlerFactory);

            var lineageSink = graph.Lineage.ItemLevelLineageEnabled
                ? ResolveLineageSink(graph, context.LineageFactory, context)
                : null;

            var pipelineLineageSink = ResolvePipelineLineageSink(graph, context.LineageFactory, context);

            // Surface lineage sink into context.Items for downstream execution (NodeExecutor) via context.Items.
            // Some tests (e.g., AggregateLineageTests) rely on builder.AddLineageSink without manually seeding context.Items.
            if (lineageSink is not null)
                context.Items.TryAdd(PipelineContextKeys.LineageSink, lineageSink);

            // Propagate resolved handlers into the context so execution strategies see them.
            if (errorHandler is not null)
                context.PipelineErrorHandler = errorHandler;

            if (deadLetterSink is not null)
                context.DeadLetterSink = deadLetterSink;

            nodeInstances = executionCoordinator.InstantiateNodes(graph, nodeFactory);

            // Ensure node instance strategies reflect graph definitions (covers DI-created instances where builder-specified strategy wasn't applied).
            foreach (var def in graph.Nodes)
            {
                if (def.ExecutionStrategy is not null && nodeInstances.TryGetValue(def.Id, out var inst) && inst is ITransformNode tn)
                    tn.ExecutionStrategy = def.ExecutionStrategy;
            }

            // Copy global annotations into context.Properties (prefixed global::)
            foreach (var kv in graph.ExecutionOptions.NodeExecutionAnnotations ?? ImmutableDictionary<string, object>.Empty)
            {
                if (kv.Key.StartsWith(ExecutionAnnotationKeys.GlobalAnnotationPrefix, StringComparison.Ordinal))
                {
                    var trimmed = kv.Key.Substring(ExecutionAnnotationKeys.GlobalAnnotationPrefix.Length);
                    var newKey = $"{ExecutionAnnotationKeys.GlobalPropertyPrefix}{trimmed}";
                    context.Properties[newKey] = kv.Value;
                }
            }

            if (context.Properties.TryGetValue(ExecutionAnnotationKeys.GlobalPropertyPrefix + "NPipeline.StateManager", out var sm))
                context.Properties[PipelineContextKeys.StateManager] = sm; // alias without Global prefix for state package

            // Wire a globally provided execution observer if present
            if (context.Properties.TryGetValue(ExecutionAnnotationKeys.ExecutionObserverProperty, out var eo))
            {
                if (eo is IExecutionObserver execObs)
                    context.ExecutionObserver = execObs;
            }

            // Ensure NodeDefinitionMap is populated (for cases where graph is constructed directly without using builder)
            graph = graph.EnsureNodeDefinitionMapInitialized();

            // Use the cached node definition map from the graph for O(1) lookups
            var nodeDefinitionMap = graph.NodeDefinitionMap;

            // Build per-run execution plans (currently leveraged for source & transform nodes)
            // This enables reflection-free steady state execution for better performance
            var executionPlans = executionCoordinator.BuildPlans(graph, nodeInstances);

            // Surface stateful node registry (similar to state manager) and register instantiated nodes
            if (context.Properties.TryGetValue("NPipeline.Global.NPipeline.State.StatefulRegistry", out var regObj))
                context.Properties[PipelineContextKeys.StatefulRegistry] = regObj; // alias without Global prefix for state package

            // Register stateful nodes before any potential snapshot attempts
            executionCoordinator.RegisterStatefulNodes(nodeInstances, context);

            // Execute nodes: allow parallel sink execution when their inputs are fully materialized (branch/fan-out scenario)
            // Build input lookup and a topologically sorted execution order
            var inputLookup = executionCoordinator.BuildInputLookup(graph);
            var sortedNodes = executionCoordinator.TopologicalSort(graph);

            foreach (var nodeDef in sortedNodes.Select(id => nodeDefinitionMap[id]))
            {
                context.CancellationToken.ThrowIfCancellationRequested();
                using var _ = context.ScopedNode(nodeDef.Id);

                // Inject per-node execution annotations (e.g., parallel options) into context for execution strategies.
                if (graph.ExecutionOptions.NodeExecutionAnnotations != null &&
                    graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(nodeDef.Id, out var annotation))
                    context.Items[PipelineContextKeys.NodeExecutionOptions(nodeDef.Id)] = annotation;
                else if (context.Items.ContainsKey(PipelineContextKeys.NodeExecutionOptions(nodeDef.Id)))
                    context.Items.Remove(PipelineContextKeys.NodeExecutionOptions(nodeDef.Id));

                var nodeInstance = nodeInstances[nodeDef.Id];
                var nodeScope = observabilitySurface.BeginNode(context, nodeDef, nodeInstance);

                try
                {
                    await infrastructureService.ExecuteWithRetriesAsync(
                        nodeDef,
                        nodeInstance,
                        graph,
                        context,
                        async () =>
                        {
                            // Use pre-built execution plans for reflection-free steady state execution
                            var plan = executionPlans[nodeDef.Id];

                            await executionCoordinator.ExecuteNodeAsync(
                                plan,
                                graph,
                                context,
                                inputLookup,
                                nodeOutputs, // Pass the shared nodeOutputs dictionary
                                nodeInstances,
                                nodeDefinitionMap).ConfigureAwait(false);

                            var completedEvent = observabilitySurface.CompleteNodeSuccess(context, nodeScope);
                            context.Properties[$"NodeCompleted_{nodeDef.Id}"] = true;
                            infrastructureService.TryPersistAfterNode(context, completedEvent);
                        },
                        context.CancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    context.Properties[$"NodeError_{nodeDef.Id}"] = true;
                    var failedEvent = observabilitySurface.CompleteNodeFailure(context, nodeScope, ex);
                    infrastructureService.TryPersistAfterNode(context, failedEvent);

                    // Log the exception type and message for debugging
                    var logger = context.LoggerFactory.CreateLogger(nameof(PipelineRunner));

                    logger.Log(
                        LogLevel.Warning,
                        "Node {NodeId} failed with exception type {ExceptionType}: {ExceptionMessage}",
                        nodeDef.Id, ex.GetType().Name, ex.Message);

                    // Check if this is a parallel execution scenario where we want to preserve the original exception
                    var isParallelExecution = context.Items.TryGetValue(PipelineContextKeys.ParallelExecution, out var parallelValue) &&
                                              parallelValue is bool isParallel && isParallel;

                    if (isParallelExecution)
                    {
                        // For parallel execution, preserve the original exception type for correct exception propagation semantics
                        logger.Log(
                            LogLevel.Warning,
                            "Preserving original exception {ExceptionType} for parallel execution of node {NodeId}",
                            ex.GetType().Name, nodeDef.Id);

                        throw;
                    }

                    // Preserve cancellation semantics: if the operation was cancelled, rethrow OperationCanceledException
                    if (ex is OperationCanceledException)
                    {
                        logger.Log(
                            LogLevel.Warning,
                            "Preserving OperationCanceledException for node {NodeId}",
                            nodeDef.Id);

                        throw;
                    }

                    // Wrap the exception in a PipelineExecutionException if it's not already a PipelineException
                    // This maintains the exception hierarchy while preserving the original exception as inner
                    if (ex is not PipelineException)
                    {
                        logger.Log(
                            LogLevel.Warning,
                            "Wrapping non-PipelineException {ExceptionType} in PipelineExecutionException for node {NodeId}",
                            ex.GetType().Name, nodeDef.Id);

                        throw new PipelineExecutionException(ErrorMessages.PipelineExecutionFailedAtNode(nodeDef.Id, ex), ex);
                    }

                    throw;
                }
            }

            if (graph.Lineage.ItemLevelLineageEnabled && pipelineLineageSink is not null)
            {
                var report = LineageGenerator.Generate(typeof(TDefinition).Name, graph, Guid.NewGuid());
                await pipelineLineageSink.RecordAsync(report, context.CancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            // Check if this is a parallel execution scenario where we want to preserve the original exception
            var isParallelExecution = context.Items.TryGetValue(PipelineContextKeys.ParallelExecution, out var parallelValue) &&
                                      parallelValue is bool isParallel && isParallel;

            if (isParallelExecution)
            {
                // For parallel execution, preserve the original exception type for correct exception propagation semantics
                observabilitySurface.FailPipeline<TDefinition>(context, ex, pipelineActivity);
                throw;
            }

            // Preserve cancellation semantics: if the pipeline was cancelled, rethrow OperationCanceledException
            if (ex is OperationCanceledException)
            {
                observabilitySurface.FailPipeline<TDefinition>(context, ex, pipelineActivity);
                throw;
            }

            // Only wrap in PipelineExecutionException if it's not already a PipelineException
            // This preserves the specific exception types thrown by the pipeline infrastructure
            if (ex is not PipelineException)
            {
                observabilitySurface.FailPipeline<TDefinition>(context, ex, pipelineActivity);
                throw new PipelineExecutionException(ErrorMessages.PipelineExecutionFailed(typeof(TDefinition).Name, ex), ex);
            }

            observabilitySurface.FailPipeline<TDefinition>(context, ex, pipelineActivity);
            throw;
        }
        finally
        {
            if (graph is not null)
                observabilitySurface.CompletePipeline<TDefinition>(context, graph, pipelineActivity);

            // Dispose all data pipes
            // Iterate over the actual nodeOutputs dictionary
            foreach (var kvp in nodeOutputs)
            {
                if (kvp.Value is not null)
                    await kvp.Value.DisposeAsync().ConfigureAwait(false);
            }

            // Dispose all node instances unless DI owns their lifetime
            var diOwnsNodes = context.Items.TryGetValue(PipelineContextKeys.DiOwnedNodes, out var owned) && owned is bool b && b;

            if (!diOwnsNodes)
            {
                foreach (var node in nodeInstances.Values)
                {
                    await node.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
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

        if (context.Items.TryGetValue(PipelineContextKeys.LineageSink, out var sink))
            return (ILineageSink)sink;

        return null;
    }

    private static IPipelineLineageSink? ResolvePipelineLineageSink(PipelineGraph graph, ILineageFactory lineageFactory, PipelineContext context)
    {
        if (graph.Lineage.PipelineLineageSink is not null)
            return graph.Lineage.PipelineLineageSink;

        if (graph.Lineage.PipelineLineageSinkType is not null)
            return lineageFactory.CreatePipelineLineageSink(graph.Lineage.PipelineLineageSinkType);

        if (context.Items.TryGetValue(PipelineContextKeys.PipelineLineageSink, out var sink))
            return (IPipelineLineageSink)sink;

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
