namespace NPipeline.Execution;

/// <summary>
///     Observer for pipeline execution lifecycle and backpressure events.
///     Implementations should be thread-safe; methods are invoked on hot paths.
/// </summary>
public interface IExecutionObserver
{
    /// <summary>
    ///     Called when a node starts execution.
    /// </summary>
    /// <param name="e">The event containing node execution start information.</param>
    void OnNodeStarted(NodeExecutionStarted e);

    /// <summary>
    ///     Called when a node completes execution (successfully or with failure).
    /// </summary>
    /// <param name="e">The event containing node execution completion information.</param>
    void OnNodeCompleted(NodeExecutionCompleted e);

    /// <summary>
    ///     Called when a retry operation occurs.
    /// </summary>
    /// <param name="e">The event containing retry information.</param>
    void OnRetry(NodeRetryEvent e);

    /// <summary>
    ///     Called when items are dropped from a queue due to backpressure.
    /// </summary>
    /// <param name="e">The event containing queue drop information.</param>
    void OnDrop(QueueDropEvent e);

    /// <summary>
    ///     Called with queue metrics information.
    /// </summary>
    /// <param name="e">The event containing queue metrics.</param>
    void OnQueueMetrics(QueueMetricsEvent e);
}

/// <summary>
///     Event data for node execution start.
/// </summary>
/// <param name="NodeId">The unique identifier of the node.</param>
/// <param name="NodeType">The type name of the node.</param>
/// <param name="StartTime">The timestamp when execution started.</param>
public sealed record NodeExecutionStarted(string NodeId, string NodeType, DateTimeOffset StartTime);

/// <summary>
///     Event data for node execution completion.
/// </summary>
/// <param name="NodeId">The unique identifier of the node.</param>
/// <param name="NodeType">The type name of the node.</param>
/// <param name="Duration">The duration of the execution.</param>
/// <param name="Success">Whether the execution completed successfully.</param>
/// <param name="Error">The exception if execution failed, otherwise null.</param>
public sealed record NodeExecutionCompleted(string NodeId, string NodeType, TimeSpan Duration, bool Success, Exception? Error);

/// <summary>
///     Specifies the kind of retry operation.
/// </summary>
public enum RetryKind
{
    /// <summary>
    ///     A single item is being retried.
    /// </summary>
    ItemRetry,

    /// <summary>
    ///     The entire node is being restarted.
    /// </summary>
    NodeRestart,
}

/// <summary>
///     Event data for node retry operations.
/// </summary>
/// <param name="NodeId">The unique identifier of the node being retried.</param>
/// <param name="Kind">The kind of retry operation.</param>
/// <param name="Attempt">The current attempt number.</param>
/// <param name="LastException">The exception from the previous attempt, if any.</param>
public sealed record NodeRetryEvent(string NodeId, RetryKind Kind, int Attempt, Exception? LastException);

/// <summary>
///     Specifies how items are dropped from a queue.
/// </summary>
public enum QueueDropKind
{
    /// <summary>
    ///     Newest items are dropped to make room.
    /// </summary>
    Newest,

    /// <summary>
    ///     Oldest items are dropped to make room.
    /// </summary>
    Oldest,
}

/// <summary>
///     Event data for queue drop operations.
/// </summary>
/// <param name="NodeId">The unique identifier of the node with the queue.</param>
/// <param name="Policy">The name of the queue policy.</param>
/// <param name="DropKind">The kind of items being dropped.</param>
/// <param name="QueueCapacity">The maximum capacity of the queue, if limited.</param>
/// <param name="QueueDepthAfter">The queue depth after dropping items.</param>
/// <param name="DroppedNewestTotal">The total count of newest items dropped.</param>
/// <param name="DroppedOldestTotal">The total count of oldest items dropped.</param>
/// <param name="EnqueuedTotal">The total number of items ever enqueued.</param>
public sealed record QueueDropEvent(
    string NodeId,
    string Policy,
    QueueDropKind DropKind,
    int? QueueCapacity,
    int QueueDepthAfter,
    int DroppedNewestTotal,
    int DroppedOldestTotal,
    int EnqueuedTotal);

/// <summary>
///     Event data for queue metrics.
/// </summary>
/// <param name="NodeId">The unique identifier of the node with the queue.</param>
/// <param name="Policy">The name of the queue policy.</param>
/// <param name="QueueCapacity">The maximum capacity of the queue, if limited.</param>
/// <param name="QueueDepth">The current depth of the queue.</param>
/// <param name="DroppedNewestTotal">The total count of newest items dropped.</param>
/// <param name="DroppedOldestTotal">The total count of oldest items dropped.</param>
/// <param name="EnqueuedTotal">The total number of items ever enqueued.</param>
/// <param name="Timestamp">The timestamp when the metrics were collected.</param>
public sealed record QueueMetricsEvent(
    string NodeId,
    string Policy,
    int? QueueCapacity,
    int QueueDepth,
    int DroppedNewestTotal,
    int DroppedOldestTotal,
    int EnqueuedTotal,
    DateTimeOffset Timestamp);
