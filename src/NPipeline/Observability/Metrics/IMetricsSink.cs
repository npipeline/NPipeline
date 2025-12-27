namespace NPipeline.Observability.Metrics;

/// <summary>
///     Defines the contract for a sink that receives and processes node-level metrics.
/// </summary>
public interface IMetricsSink
{
    /// <summary>
    ///     Asynchronously records node metrics.
    /// </summary>
    /// <param name="nodeMetrics">The node metrics to record.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken);
}
