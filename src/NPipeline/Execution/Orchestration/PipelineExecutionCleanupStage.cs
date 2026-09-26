using NPipeline.DataFlow;
using NPipeline.Execution.Lineage;
using NPipeline.Graph;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineExecutionCleanupStage(IObservabilitySurface observabilitySurface)
{
    /// <summary>
    ///     Releases everything the run owns, recording rather than propagating each failure so one bad disposal cannot
    ///     stop the rest or replace the run's real error.
    /// </summary>
    /// <returns>The cleanup failures, or null when every disposal succeeded.</returns>
    public async Task<List<Exception>?> CleanupAsync(
        Type definitionType,
        PipelineContext context,
        PipelineGraph? graph,
        IPipelineActivity pipelineActivity,
        Dictionary<string, IDataStream?> nodeOutputs,
        OwnedNodeInstances? ownedNodeInstances,
        bool pipelineCompleted)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(pipelineActivity);
        ArgumentNullException.ThrowIfNull(nodeOutputs);

        List<Exception>? errors = null;

        if (pipelineCompleted && graph is not null)
            await Guard(() => new ValueTask(observabilitySurface.CompletePipeline(definitionType, context, graph, pipelineActivity))).ConfigureAwait(false);

        foreach (var kvp in nodeOutputs)
        {
            var output = kvp.Value;

            if (output is not null)
                await Guard(output.DisposeAsync).ConfigureAwait(false);
        }

        nodeOutputs.Clear();

        // A node whose output was never pulled never released its own lineage state, so the run drops all of it here.
        // Guarded like the other cleanup steps, so a failure here cannot replace the run's real error.
        if (graph?.Lineage.ItemLevelLineageEnabled == true)
        {
            await Guard(() =>
            {
                context.Lineage.Outcomes.Clear();
                return ValueTask.CompletedTask;
            }).ConfigureAwait(false);
        }

        if (ownedNodeInstances is not null)
        {
            // The node instances were already disposed on a setup failure. A repeat call is a no-op.
            var nodeErrors = await ownedNodeInstances.DisposeAllAsync().ConfigureAwait(false);

            if (nodeErrors is { Count: > 0 })
                (errors ??= []).AddRange(nodeErrors);
        }

        return errors;

        async ValueTask Guard(Func<ValueTask> action)
        {
            try
            {
                await action().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                (errors ??= []).Add(ex);
            }
        }
    }
}
