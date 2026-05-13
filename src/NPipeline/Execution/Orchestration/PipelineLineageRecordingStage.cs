using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineLineageRecordingStage
{
    public async Task RecordAsync(
        Type definitionType,
        PipelineExecutionSetupResult setup,
        PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);

        if (!setup.Graph.Lineage.ItemLevelLineageEnabled || setup.PipelineLineageSink is null)
            return;

        var runId = context.RunId == Guid.Empty
            ? Guid.NewGuid()
            : context.RunId;

        var report = context.LineageFactory.CreateLineageReport(definitionType.Name, context.PipelineId, setup.Graph, runId);

        if (report is null)
            return;

        await setup.PipelineLineageSink.RecordAsync(report, context.CancellationToken).ConfigureAwait(false);
    }
}
