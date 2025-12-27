namespace Sample_ParallelProcessing;

/// <summary>
///     Represents a CPU-intensive work item for parallel processing demonstrations.
/// </summary>
/// <param name="Id">The unique identifier for the work item.</param>
/// <param name="DataSize">The size of data to process (in arbitrary units).</param>
/// <param name="Complexity">The complexity factor for processing (1-10).</param>
/// <param name="CreatedAt">The timestamp when the work item was created.</param>
public record CpuIntensiveWorkItem(string Id, int DataSize, int Complexity, DateTime CreatedAt);

/// <summary>
///     Represents the result of processing a CPU-intensive work item.
/// </summary>
/// <param name="Id">The unique identifier matching the original work item.</param>
/// <param name="Result">The computed result.</param>
/// <param name="ProcessingTimeMs">The time taken to process in milliseconds.</param>
/// <param name="ProcessedAt">The timestamp when processing completed.</param>
/// <param name="ThreadId">The ID of the thread that processed the item.</param>
public record ProcessedWorkItem(string Id, string Result, long ProcessingTimeMs, DateTime ProcessedAt, int ThreadId);

/// <summary>
///     Represents performance metrics for pipeline execution.
/// </summary>
/// <param name="WorkItemId">The ID of the work item.</param>
/// <param name="NodeName">The name of the node that recorded the metric.</param>
/// <param name="StartTime">The start time of processing.</param>
/// <param name="EndTime">The end time of processing.</param>
/// <param name="DurationMs">The duration in milliseconds.</param>
/// <param name="ThreadId">The thread ID that performed the work.</param>
public record PerformanceMetric(string WorkItemId, string NodeName, DateTime StartTime, DateTime EndTime, long DurationMs, int ThreadId);
