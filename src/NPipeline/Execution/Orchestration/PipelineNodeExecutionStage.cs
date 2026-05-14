using System.Runtime.ExceptionServices;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineNodeExecutionStage(
    ITopologyService topologyService,
    INodeExecutor nodeExecutor,
    IErrorHandlingService errorHandlingService,
    IPersistenceService persistenceService,
    IObservabilitySurface observabilitySurface)
{
    public async Task ExecuteAsync(
        PipelineExecutionSetupResult setup,
        PipelineContext context,
        IDictionary<string, IDataStream?> nodeOutputs)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeOutputs);

        var inputLookup = topologyService.BuildInputLookup(setup.Graph);
        var sortedNodes = topologyService.TopologicalSort(setup.Graph);

        foreach (var nodeDef in sortedNodes.Select(id => setup.NodeDefinitionMap[id]))
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            using var nodeScopeHandle = context.ScopedNode(nodeDef.Id);

            ApplyPerNodeExecutionAnnotation(setup.Graph, context, nodeDef.Id);

            var nodeInstance = setup.NodeInstances[nodeDef.Id];
            var nodeScope = observabilitySurface.BeginNode(context, setup.Graph, nodeDef, nodeInstance);

            try
            {
                await ExecuteNodeWithRetriesAsync(
                    nodeDef,
                    nodeInstance,
                    setup,
                    context,
                    nodeScope,
                    inputLookup,
                    nodeOutputs).ConfigureAwait(false);
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
            context.NodeExecutionScopeRegistry.SetNodeExecutionAnnotation(nodeId, annotation);
        else
            _ = context.NodeExecutionScopeRegistry.RemoveNodeExecutionAnnotation(nodeId);

        if (graph.ExecutionOptions.NodeExecutionAnnotations != null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId), out var policyAnnotation) &&
            policyAnnotation is IResiliencePolicy nodePolicy)
            context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId), nodePolicy);
    }

    private async Task ExecuteNodeWithRetriesAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        PipelineExecutionSetupResult setup,
        PipelineContext context,
        NodeObservationScope nodeScope,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs)
    {
        await errorHandlingService.ExecuteWithRetriesAsync(
            nodeDef,
            nodeInstance,
            setup.Graph,
            context,
            async () =>
            {
                var plan = setup.ExecutionPlans[nodeDef.Id];

                await nodeExecutor.ExecuteAsync(
                    plan,
                    setup.Graph,
                    context,
                    inputLookup,
                    nodeOutputs,
                    setup.NodeInstances,
                    setup.NodeDefinitionMap).ConfigureAwait(false);

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

        if (context.ResiliencePolicy is not DefaultResiliencePolicy &&
            nodeDef.ExecutionStrategy?.GetType().Name == "ResilientExecutionStrategy")
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
}
