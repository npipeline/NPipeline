using NPipeline.Observability.Metrics;

namespace NPipeline.Observability;

/// <summary>
///     Defines the contract for collecting comprehensive observability metrics during pipeline execution.
/// </summary>
public interface IObservabilityCollector
{
    /// <summary>
    ///     Records the start of a node execution.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="timestamp">The timestamp when execution started.</param>
    /// <param name="threadId">The thread ID executing the node.</param>
    /// <param name="initialMemoryMb">The initial memory usage in megabytes.</param>
    void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, long? initialMemoryMb = null);

    /// <summary>
    ///     Records the completion of a node execution.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="timestamp">The timestamp when execution completed.</param>
    /// <param name="success">Whether the execution was successful.</param>
    /// <param name="exception">Any exception that occurred during execution.</param>
    /// <param name="peakMemoryMb">The peak memory usage in megabytes during execution.</param>
    /// <param name="processorTimeMs">The processor time used in milliseconds.</param>
    void RecordNodeEnd(string nodeId, DateTimeOffset timestamp, bool success, Exception? exception = null, long? peakMemoryMb = null,
        long? processorTimeMs = null);

    /// <summary>
    ///     Records item processing metrics for a node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="itemsProcessed">The number of items processed.</param>
    /// <param name="itemsEmitted">The number of items emitted.</param>
    void RecordItemMetrics(string nodeId, long itemsProcessed, long itemsEmitted);

    /// <summary>
    ///     Records a retry attempt for a node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="retryCount">The current retry attempt number.</param>
    /// <param name="reason">The reason for the retry.</param>
    void RecordRetry(string nodeId, int retryCount, string? reason = null);

    /// <summary>
    ///     Records performance metrics for a completed node execution.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <param name="throughputItemsPerSec">The throughput in items per second.</param>
    /// <param name="averageItemProcessingMs">The average time per item in milliseconds.</param>
    void RecordPerformanceMetrics(string nodeId, double throughputItemsPerSec, double averageItemProcessingMs);

    /// <summary>
    ///     Gets the collected metrics for all nodes.
    /// </summary>
    /// <returns>A collection of node metrics.</returns>
    IReadOnlyList<INodeMetrics> GetNodeMetrics();

    /// <summary>
    ///     Gets the collected metrics for a specific node.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the node.</param>
    /// <returns>The node metrics, or null if not found.</returns>
    INodeMetrics? GetNodeMetrics(string nodeId);

    /// <summary>
    ///     Creates pipeline-level metrics from the collected data.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="runId">The unique identifier for this pipeline run.</param>
    /// <param name="startTime">When the pipeline started.</param>
    /// <param name="endTime">When the pipeline ended.</param>
    /// <param name="success">Whether the pipeline execution was successful.</param>
    /// <param name="exception">Any exception that occurred during pipeline execution.</param>
    /// <returns>The pipeline metrics.</returns>
    IPipelineMetrics CreatePipelineMetrics(string pipelineName, Guid runId, DateTimeOffset startTime, DateTimeOffset? endTime, bool success,
        Exception? exception = null);
}
