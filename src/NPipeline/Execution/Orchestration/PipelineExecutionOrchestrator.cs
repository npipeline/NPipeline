using System.Runtime.ExceptionServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Caching;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineExecutionOrchestrator : IPipelineExecutionOrchestrator
{
    private readonly PipelineExecutionCleanupStage _cleanupStage;
    private readonly PipelineExecutionFailureStage _failureStage;
    private readonly ILineage _lineage;
    private readonly PipelineLineageRecordingStage _lineageRecordingStage;
    private readonly PipelineNodeExecutionStage _nodeExecutionStage;
    private readonly IObservabilitySurface _observabilitySurface;
    private readonly IPipelineFactory _pipelineFactory;
    private readonly PipelineExecutionSetupStage _setupStage;

    public PipelineExecutionOrchestrator(
        IPipelineFactory pipelineFactory,
        INodeFactory nodeFactory,
        INodeExecutor nodeExecutor,
        ITopologyService topologyService,
        INodeInstantiationService nodeInstantiationService,
        IErrorHandlingService errorHandlingService,
        IPersistenceService persistenceService,
        IObservabilitySurface observabilitySurface,
        ILineage lineage,
        IPipelineExecutionPlanCache executionPlanCache,
        IRuntimePipelineBinder runtimePipelineBinder)
    {
        ArgumentNullException.ThrowIfNull(pipelineFactory);
        ArgumentNullException.ThrowIfNull(nodeFactory);
        ArgumentNullException.ThrowIfNull(nodeExecutor);
        ArgumentNullException.ThrowIfNull(topologyService);
        ArgumentNullException.ThrowIfNull(nodeInstantiationService);
        ArgumentNullException.ThrowIfNull(errorHandlingService);
        ArgumentNullException.ThrowIfNull(persistenceService);
        ArgumentNullException.ThrowIfNull(observabilitySurface);
        ArgumentNullException.ThrowIfNull(lineage);
        ArgumentNullException.ThrowIfNull(executionPlanCache);
        ArgumentNullException.ThrowIfNull(runtimePipelineBinder);

        _pipelineFactory = pipelineFactory;
        _observabilitySurface = observabilitySurface;
        _lineage = lineage;

        _setupStage = new PipelineExecutionSetupStage(
            nodeFactory,
            nodeInstantiationService,
            executionPlanCache,
            runtimePipelineBinder);

        _nodeExecutionStage = new PipelineNodeExecutionStage(
            topologyService,
            nodeExecutor,
            errorHandlingService,
            persistenceService,
            observabilitySurface);

        _lineageRecordingStage = new PipelineLineageRecordingStage(lineage);
        _failureStage = new PipelineExecutionFailureStage(observabilitySurface);
        _cleanupStage = new PipelineExecutionCleanupStage(observabilitySurface);
    }

    public async Task RunAsync(
        Type definitionType,
        PipelineContext context,
        Func<IPipelineFactory, PipelineContext, Pipeline.Pipeline> createPipeline,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(createPipeline);

        // Link the runner's token into the context's token for the whole run, cleanup included: node execution
        // observes the context's token, so this is what lets the caller's token stop a running pipeline.
        using var runCancellation = context.LinkRunCancellation(cancellationToken);
        using var pipelineActivity = _observabilitySurface.BeginPipeline(definitionType, context);
        PipelineGraph? graph = null;
        InitializeExecutionContext(context);

        // Publish this runner's lineage module so the builder constructs adapters from the same instance that will
        // handle lineage at runtime.
        context.Lineage.Module = _lineage;

        Dictionary<string, IDataStream?> nodeOutputs = new();
        OwnedNodeInstances? ownedNodeInstances = null;
        var pipelineCompleted = false;
        Exception? failure = null;
        List<Exception>? cleanupErrors = null;

        try
        {
            var pipeline = createPipeline(_pipelineFactory, context);
            graph = pipeline.Graph;

            // The run owns the builder's disposable instances. They usually also arrive as preconfigured node
            // instances; the set is reference-distinct, so each is tracked once. Seeding it before setup means even a
            // throw before instantiation cannot leak them.
            ownedNodeInstances = new OwnedNodeInstances();
            ownedNodeInstances.AddRange(pipeline.BuilderDisposables);

            // A factory-built pipeline carries its definition's breakers, which outlive this run. Any other gets fresh ones.
            context.ExecutionConfiguration.CircuitBreakers = pipeline.CircuitBreakers ?? new CircuitBreakerRegistry();

            var setupResult = await _setupStage.PrepareAsync(definitionType, graph, context, ownedNodeInstances, context.CancellationToken)
                .ConfigureAwait(false);

            graph = setupResult.Graph;
            nodeOutputs.EnsureCapacity(graph.Nodes.Length);

            await _nodeExecutionStage.ExecuteAsync(setupResult, context, nodeOutputs).ConfigureAwait(false);
            await _lineageRecordingStage.RecordAsync(definitionType, setupResult, context).ConfigureAwait(false);

            pipelineCompleted = true;
        }
        catch (Exception ex)
        {
            failure = ex;

            try
            {
                await _failureStage.HandleAsync(definitionType, context, ex, pipelineActivity).ConfigureAwait(false);
            }
            catch (Exception wrapped)
            {
                // Reported after cleanup, so a cleanup failure cannot replace the run's real error.
                failure = wrapped;
            }
        }
        finally
        {
            cleanupErrors = await _cleanupStage.CleanupAsync(
                    definitionType,
                    context,
                    graph,
                    pipelineActivity,
                    nodeOutputs,
                    ownedNodeInstances,
                    pipelineCompleted)
                .ConfigureAwait(false);
        }

        if (cleanupErrors is { Count: > 0 })
        {
            // A cleanup failure while the run already failed is logged, not thrown: the real failure wins.
            if (failure is null)
                throw cleanupErrors.Count == 1 ? cleanupErrors[0] : new AggregateException(cleanupErrors);

            var logger = context.Observability.LoggerFactory.CreateLogger(nameof(PipelineRunner));

            foreach (var error in cleanupErrors)
                PipelineRunnerLogMessages.CleanupFailed(logger, error, error.GetType().Name);
        }

        if (failure is not null)
            ExceptionDispatchInfo.Capture(failure).Throw();
    }

    private static void InitializeExecutionContext(PipelineContext context)
    {
        var runIdentity = context.RunIdentity;
        var execution = context.ExecutionConfiguration;
        var observability = context.Observability;
        var nodeEnvironment = context.NodeEnvironment;

        runIdentity.PipelineStartTimeUtc = DateTime.UtcNow;

        if (runIdentity.PipelineId == Guid.Empty)
            runIdentity.PipelineId = Guid.NewGuid();

        if (runIdentity.RunId == Guid.Empty)
            runIdentity.RunId = Guid.NewGuid();

        observability.ProcessedItemsCounter = new StatsCounter();
        execution.ResetResilienceOptions();
        nodeEnvironment.NodeExecutionScopeRegistry.Clear();
    }
}
