namespace NPipeline.Observability.Metrics;

/// <summary>
///     Represents comprehensive performance metrics for a single node execution.
/// </summary>
/// <param name="NodeId">The unique identifier of the node.</param>
/// <param name="StartTime">The timestamp when the node execution started.</param>
/// <param name="EndTime">The timestamp when the node execution completed.</param>
/// <param name="DurationMs">The node-owned work duration in milliseconds. Breaking change: this parameter previously represented wall-clock elapsed time; use WallDurationMs for wall-clock comparisons.</param>
/// <param name="Success">Whether the node execution was successful.</param>
/// <param name="ItemsProcessed">The number of items processed by this node.</param>
/// <param name="ItemsEmitted">The number of items emitted by this node.</param>
/// <param name="Exception">Any exception that occurred during execution.</param>
/// <param name="RetryCount">The number of retries that occurred during processing.</param>
/// <param name="PeakMemoryUsageMb">The peak memory usage in megabytes during node execution.</param>
/// <param name="ProcessorTimeMs">The processor time used in milliseconds.</param>
/// <param name="ThroughputItemsPerSec">The throughput in items per second.</param>
/// <param name="AverageItemProcessingMs">The average time spent per processed item in milliseconds.</param>
/// <param name="ThreadId">The thread ID that primarily processed this node.</param>
/// <param name="PipelineId">The unique pipeline identity this node belongs to.</param>
/// <param name="PipelineName">The name of the pipeline this node belongs to. Null for top-level pipelines.</param>
/// <param name="WorkDurationMs">The node-owned work duration in milliseconds.</param>
/// <param name="InputWaitDurationMs">The upstream input wait duration in milliseconds.</param>
/// <param name="OutputBlockDurationMs">The downstream output block duration in milliseconds.</param>
/// <param name="WallDurationMs">The total elapsed wall-clock duration for this node in milliseconds.</param>
public sealed record NodeMetrics(
    string NodeId,
    DateTimeOffset? StartTime,
    DateTimeOffset? EndTime,
    double? DurationMs,
    bool Success,
    long ItemsProcessed,
    long ItemsEmitted,
    Exception? Exception,
    int RetryCount,
    double? PeakMemoryUsageMb,
    double? ProcessorTimeMs,
    double? ThroughputItemsPerSec,
    double? AverageItemProcessingMs,
    int? ThreadId,
    Guid PipelineId,
    string? PipelineName = null,
    double? WorkDurationMs = null,
    double? InputWaitDurationMs = null,
    double? OutputBlockDurationMs = null,
    double? WallDurationMs = null) : INodeMetrics;
