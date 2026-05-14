using NPipeline.Lineage;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineLineageRecordingStage(ILineage lineage)
{
    public async Task RecordAsync(
        Type definitionType,
        PipelineExecutionSetupResult setup,
        PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);

        await lineage.RecordPipelineAsync(definitionType, setup.Graph, context, setup.PipelineLineageSink).ConfigureAwait(false);
    }
}
