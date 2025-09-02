namespace NPipeline.Lineage;

/// <summary>
///     Defines the contract for a sink that receives and processes the pipeline-level lineage report.
/// </summary>
public interface IPipelineLineageSink
{
    /// <summary>
    ///     Asynchronously records a pipeline lineage report.
    /// </summary>
    /// <param name="report">The pipeline lineage report.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken);
}
