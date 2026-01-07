namespace NPipeline.Observability.Metrics;

/// <summary>
///     Represents performance and throughput metrics for an entire pipeline execution.
/// </summary>
/// <param name="PipelineName">The name of the pipeline.</param>
/// <param name="RunId">The unique identifier for this pipeline run.</param>
/// <param name="StartTime">The timestamp when the pipeline execution started.</param>
/// <param name="EndTime">The timestamp when the pipeline execution completed.</param>
/// <param name="DurationMs">The total duration of the pipeline execution in milliseconds.</param>
/// <param name="Success">Whether the pipeline execution was successful.</param>
/// <param name="TotalItemsProcessed">The total number of items processed by all nodes in the pipeline.</param>
/// <param name="NodeMetrics">Metrics for individual nodes in the pipeline.</param>
/// <param name="Exception">Any exception that occurred during execution.</param>
public sealed record PipelineMetrics(
    string PipelineName,
    Guid RunId,
    DateTimeOffset StartTime,
    DateTimeOffset? EndTime,
    long? DurationMs,
    bool Success,
    long TotalItemsProcessed,
    IReadOnlyList<INodeMetrics> NodeMetrics,
    Exception? Exception) : IPipelineMetrics;