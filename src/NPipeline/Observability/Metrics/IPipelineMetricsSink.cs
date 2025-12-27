namespace NPipeline.Observability.Metrics;

/// <summary>
///     Defines the contract for a sink that receives and processes pipeline-level metrics.
/// </summary>
public interface IPipelineMetricsSink
{
    /// <summary>
    ///     Asynchronously records pipeline metrics.
    /// </summary>
    /// <param name="pipelineMetrics">The pipeline metrics to record.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken);
}
