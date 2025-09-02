namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Metrics emitted by <see cref="ParallelExecutionStrategy" /> when using bounded queues.
/// </summary>
public sealed class ParallelExecutionMetrics
{
    private long _droppedNewest;
    private long _droppedOldest;
    private long _enqueued;
    private long _itemsWithRetry;
    private long _maxItemRetryAttempts;
    private long _processed;
    private long _retryEvents;
    public long DroppedNewest => Interlocked.Read(ref _droppedNewest);
    public long DroppedOldest => Interlocked.Read(ref _droppedOldest);
    public long Processed => Interlocked.Read(ref _processed);
    public long Enqueued => Interlocked.Read(ref _enqueued);
    public long RetryEvents => Interlocked.Read(ref _retryEvents); // total retry attempts (each failed attempt that caused a retry)
    public long ItemsWithRetry => Interlocked.Read(ref _itemsWithRetry); // number of items that were retried at least once
    public long MaxItemRetryAttempts => Interlocked.Read(ref _maxItemRetryAttempts); // highest retry attempt count for any single item

    internal long IncrementDroppedNewest()
    {
        return Interlocked.Increment(ref _droppedNewest);
    }

    internal long IncrementDroppedOldest()
    {
        return Interlocked.Increment(ref _droppedOldest);
    }

    internal long IncrementProcessed()
    {
        return Interlocked.Increment(ref _processed);
    }

    internal long IncrementEnqueued()
    {
        return Interlocked.Increment(ref _enqueued);
    }

    internal void RecordRetry(int attemptNumber)
    {
        // attemptNumber is the 1-based retry count for the current item (first retry => 1)
        Interlocked.Increment(ref _retryEvents);

        if (attemptNumber == 1)
            Interlocked.Increment(ref _itemsWithRetry);

        while (true)
        {
            var current = Interlocked.Read(ref _maxItemRetryAttempts);

            if (attemptNumber <= current)
                break;

            if (Interlocked.CompareExchange(ref _maxItemRetryAttempts, attemptNumber, current) == current)
                break;
        }
    }
}
