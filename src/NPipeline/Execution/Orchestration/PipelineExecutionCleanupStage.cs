using NPipeline.DataFlow;
using NPipeline.Execution.Pooling;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineExecutionCleanupStage(IObservabilitySurface observabilitySurface)
{
    public async Task CleanupAsync(
        Type definitionType,
        PipelineContext context,
        PipelineGraph? graph,
        IPipelineActivity pipelineActivity,
        Dictionary<string, IDataStream?> nodeOutputs,
        Dictionary<string, INode>? nodeInstances,
        bool pipelineCompleted)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(pipelineActivity);
        ArgumentNullException.ThrowIfNull(nodeOutputs);

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
}
