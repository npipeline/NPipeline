namespace NPipeline.Observability.Metrics;

/// <summary>
///     Represents comprehensive performance metrics for a single node execution.
/// </summary>
public interface INodeMetrics
{
    /// <summary>
    ///     The unique identifier of the node.
    /// </summary>
    string NodeId { get; }

    /// <summary>
    ///     The timestamp when the node execution started.
    /// </summary>
    DateTimeOffset? StartTime { get; }

    /// <summary>
    ///     The timestamp when the node execution completed.
    /// </summary>
    DateTimeOffset? EndTime { get; }

    /// <summary>
    ///     The duration of the node execution in milliseconds.
    /// </summary>
    double? DurationMs { get; }

    /// <summary>
    ///     Whether the node execution was successful.
    /// </summary>
    bool Success { get; }

    /// <summary>
    ///     The number of items processed by this node.
    /// </summary>
    long ItemsProcessed { get; }

    /// <summary>
    ///     The number of items emitted by this node.
    /// </summary>
    long ItemsEmitted { get; }

    /// <summary>
    ///     Any exception that occurred during execution.
    /// </summary>
    Exception? Exception { get; }

    /// <summary>
    ///     The number of retries that occurred during processing.
    /// </summary>
    int RetryCount { get; }

    /// <summary>
    ///     The peak memory usage in megabytes during node execution.
    /// </summary>
    double? PeakMemoryUsageMb { get; }

    /// <summary>
    ///     The processor time used in milliseconds.
    /// </summary>
    double? ProcessorTimeMs { get; }

    /// <summary>
    ///     The throughput in items per second.
    /// </summary>
    double? ThroughputItemsPerSec { get; }

    /// <summary>
    ///     The average time spent per processed item in milliseconds.
    /// </summary>
    double? AverageItemProcessingMs { get; }

    /// <summary>
    ///     The thread ID that primarily processed this node.
    /// </summary>
    int? ThreadId { get; }

    /// <summary>
    ///     The unique pipeline identity this node belongs to.
    /// </summary>
    Guid PipelineId { get; }

    /// <summary>
    ///     The name of the pipeline this node belongs to.
    ///     Null for top-level pipelines; set for child/nested pipeline nodes.
    /// </summary>
    string? PipelineName { get; }
}
