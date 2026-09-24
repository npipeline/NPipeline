namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Counts successes and failures over a sliding period, for a breaker's failure rate.
/// </summary>
/// <remarks>
///     The period is split into a fixed number of buckets, so recording an outcome is an increment, not an allocation,
///     and old outcomes expire a bucket at a time. Counts are approximate at a bucket boundary, which a failure rate
///     can tolerate. Thread-safe.
/// </remarks>
internal sealed class RollingWindow
{
    private const int BucketCount = 10;
    private readonly long _bucketTicks;

    private readonly Bucket[] _buckets;
    private readonly TimeProvider _time;

    /// <param name="window">The period to count over. Must be positive.</param>
    /// <param name="time">The clock.</param>
    public RollingWindow(TimeSpan window, TimeProvider time)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(window, TimeSpan.Zero);
        _time = time ?? throw new ArgumentNullException(nameof(time));

        // Timestamp ticks per bucket, at least one so a tiny window still advances.
        _bucketTicks = Math.Max(1, (long)(window.TotalSeconds * time.TimestampFrequency / BucketCount));
        _buckets = new Bucket[BucketCount];

        for (var i = 0; i < BucketCount; i++)
        {
            _buckets[i] = new Bucket { Epoch = long.MinValue };
        }
    }

    public void RecordSuccess()
    {
        _ = Interlocked.Increment(ref Current().Successes);
    }

    public void RecordFailure()
    {
        _ = Interlocked.Increment(ref Current().Failures);
    }

    /// <summary>
    ///     The attempts and failures recorded over the period.
    /// </summary>
    public (int Total, int Failures) Read()
    {
        var epoch = CurrentEpoch();
        int successes = 0, failures = 0;

        foreach (var bucket in _buckets)
        {
            var age = epoch - Volatile.Read(ref bucket.Epoch);

            if (age is < 0 or >= BucketCount)
                continue;

            successes += Volatile.Read(ref bucket.Successes);
            failures += Volatile.Read(ref bucket.Failures);
        }

        return (successes + failures, failures);
    }

    public void Clear()
    {
        foreach (var bucket in _buckets)
        {
            lock (bucket)
            {
                bucket.Epoch = long.MinValue;
                bucket.Successes = 0;
                bucket.Failures = 0;
            }
        }
    }

    private long CurrentEpoch() => _time.GetTimestamp() / _bucketTicks;

    private Bucket Current()
    {
        var epoch = CurrentEpoch();
        var bucket = _buckets[(int)((epoch % BucketCount + BucketCount) % BucketCount)];

        if (Volatile.Read(ref bucket.Epoch) != epoch)
        {
            lock (bucket)
            {
                if (bucket.Epoch != epoch)
                {
                    bucket.Successes = 0;
                    bucket.Failures = 0;
                    Volatile.Write(ref bucket.Epoch, epoch);
                }
            }
        }

        return bucket;
    }

    private sealed class Bucket
    {
        public long Epoch;
        public int Failures;
        public int Successes;
    }
}
