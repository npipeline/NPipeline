using NPipeline.Execution.Annotations;
using NPipeline.Pipeline;

namespace NPipeline.DataFlow.Branching;

/// <summary>
///     Metrics captured for a branch (multicast) wrapper.
///     Stored in PipelineContext.Items with key prefix ExecutionAnnotationKeys.BranchMetricsPrefix and node id suffix.
/// </summary>
public sealed class BranchMetrics
{
    private int _anyBacklogObserved; // presence flag
    private int _faulted;
    private int _maxAggregateBacklog; // sum of per-channel pending item counts high-water
    private int[]? _perSubscriberMaxBacklog; // high-water per subscriber
    private int _subscriberCompleted;

    /// <summary>Total declared subscribers (downstream edges).</summary>
    public int SubscriberCount { get; private set; }

    /// <summary>Configured per-subscriber buffer capacity (null = unbounded).</summary>
    public int? PerSubscriberCapacity { get; private set; }

    /// <summary>1 if any backlog was observed on any subscriber channel (approximation placeholder until precise counts implemented).</summary>
    public int ApproxBacklogObserved => _anyBacklogObserved;

    public int MaxAggregateBacklog => _maxAggregateBacklog;

    /// <summary>Per-subscriber high-water backlog counts (index corresponds to subscriber ordinal). Empty when not yet initialized.</summary>
    public IReadOnlyList<int> PerSubscriberMaxBacklog => (IReadOnlyList<int>?)_perSubscriberMaxBacklog ?? [];

    /// <summary>Number of subscribers that completed consumption.</summary>
    public int SubscribersCompleted => _subscriberCompleted;

    /// <summary>1 if a pump fault occurred.</summary>
    public int Faulted => _faulted;

    internal void SetSubscriberCount(int value)
    {
        SubscriberCount = value;
    }

    internal void SetPerSubscriberCapacity(int value)
    {
        PerSubscriberCapacity = value;
    }

    internal void EnsurePerSubscriberArrays()
    {
        if (_perSubscriberMaxBacklog is null)
            _perSubscriberMaxBacklog = new int[SubscriberCount];
    }

    internal void ObservePending(int aggregatePending)
    {
        if (aggregatePending <= 0)
            return;

        Interlocked.Exchange(ref _anyBacklogObserved, 1);

        // Track simple high-water aggregate backlog
        while (true)
        {
            var current = _maxAggregateBacklog;

            if (aggregatePending <= current)
                break;

            if (Interlocked.CompareExchange(ref _maxAggregateBacklog, aggregatePending, current) == current)
                break;
        }
    }

    internal void ObservePerSubscriberPending(int subscriberIndex, int pending)
    {
        var arr = _perSubscriberMaxBacklog;

        if (arr is null)
            return;

        while (true)
        {
            var current = arr[subscriberIndex];

            if (pending <= current)
                break;

            if (Interlocked.CompareExchange(ref arr[subscriberIndex], pending, current) == current)
                break;
        }
    }

    internal void MarkSubscriberCompleted()
    {
        Interlocked.Increment(ref _subscriberCompleted);
    }

    internal void MarkFault()
    {
        Interlocked.Exchange(ref _faulted, 1);
    }
}

public static class BranchMetricsContextExtensions
{
    public static BranchMetrics? GetBranchMetrics(this PipelineContext context, string nodeId)
    {
        if (context.Items.TryGetValue(ExecutionAnnotationKeys.BranchMetricsForNode(nodeId), out var m) && m is BranchMetrics fm)
            return fm;

        return null;
    }
}
