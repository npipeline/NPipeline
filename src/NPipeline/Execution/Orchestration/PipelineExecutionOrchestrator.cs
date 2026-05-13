using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Caching;
using NPipeline.Execution.Pooling;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineExecutionOrchestrator : IPipelineExecutionOrchestrator
{
    private readonly PipelineExecutionCleanupStage _cleanupStage;
    private readonly PipelineExecutionFailureStage _failureStage;
    private readonly PipelineLineageRecordingStage _lineageRecordingStage;
    private readonly PipelineNodeExecutionStage _nodeExecutionStage;
    private readonly IPipelineFactory _pipelineFactory;
    private readonly IObservabilitySurface _observabilitySurface;
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
        ArgumentNullException.ThrowIfNull(executionPlanCache);
        ArgumentNullException.ThrowIfNull(runtimePipelineBinder);

        _pipelineFactory = pipelineFactory;
        _observabilitySurface = observabilitySurface;

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

        _lineageRecordingStage = new PipelineLineageRecordingStage();
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

        using var pipelineActivity = _observabilitySurface.BeginPipeline(definitionType, context);
        PipelineGraph? graph = null;
        InitializeExecutionContext(context);

        var nodeOutputs = PipelineObjectPool.RentNodeOutputDictionary();
        Dictionary<string, INode>? nodeInstances = null;
        var pipelineCompleted = false;

        try
        {
            var pipeline = createPipeline(_pipelineFactory, context);
            graph = pipeline.Graph;

            var setupResult = await _setupStage.PrepareAsync(definitionType, graph, context, cancellationToken).ConfigureAwait(false);
            graph = setupResult.Graph;
            nodeInstances = setupResult.NodeInstances;

            nodeOutputs.EnsureCapacity(graph.Nodes.Length);

            await _nodeExecutionStage.ExecuteAsync(setupResult, context, nodeOutputs).ConfigureAwait(false);
            await _lineageRecordingStage.RecordAsync(definitionType, setupResult, context).ConfigureAwait(false);

            pipelineCompleted = true;
        }
        catch (Exception ex)
        {
            await _failureStage.HandleAsync(definitionType, context, ex, pipelineActivity).ConfigureAwait(false);
        }
        finally
        {
            await _cleanupStage.CleanupAsync(
                    definitionType,
                    context,
                    graph,
                    pipelineActivity,
                    nodeOutputs,
                    nodeInstances,
                    pipelineCompleted)
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
        context.NodeExecutionScopeRegistry.Clear();
        context.IsParallelExecution = false;
        context.LastRetryExhaustedException = null;
    }
}
