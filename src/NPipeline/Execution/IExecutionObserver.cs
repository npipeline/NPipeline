namespace NPipeline.Execution;

/// <summary>
///     Observer for pipeline execution lifecycle and backpressure events.
///     Implementations should be thread-safe; methods are invoked on hot paths.
/// </summary>
public interface IExecutionObserver
{
    void OnNodeStarted(NodeExecutionStarted e);
    void OnNodeCompleted(NodeExecutionCompleted e);
    void OnRetry(NodeRetryEvent e);
    void OnDrop(QueueDropEvent e);
    void OnQueueMetrics(QueueMetricsEvent e);
}

public sealed record NodeExecutionStarted(string NodeId, string NodeType, DateTimeOffset StartTime);

public sealed record NodeExecutionCompleted(string NodeId, string NodeType, TimeSpan Duration, bool Success, Exception? Error);

public enum RetryKind
{
    ItemRetry,
    NodeRestart,
}

public sealed record NodeRetryEvent(string NodeId, RetryKind Kind, int Attempt, Exception? LastException);

public enum QueueDropKind
{
    Newest,
    Oldest,
}

public sealed record QueueDropEvent(
    string NodeId,
    string Policy,
    QueueDropKind DropKind,
    int? QueueCapacity,
    int QueueDepthAfter,
    int DroppedNewestTotal,
    int DroppedOldestTotal,
    int EnqueuedTotal);

public sealed record QueueMetricsEvent(
    string NodeId,
    string Policy,
    int? QueueCapacity,
    int QueueDepth,
    int DroppedNewestTotal,
    int DroppedOldestTotal,
    int EnqueuedTotal,
    DateTimeOffset Timestamp);
