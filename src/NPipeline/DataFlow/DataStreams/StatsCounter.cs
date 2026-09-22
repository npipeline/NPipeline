namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Stats holder used for total item counting across a run.
///     Stored in PipelineContext.Items under the key "stats.totalProcessedItems".
/// </summary>
/// <remarks>
///     Counting streams accumulate into a local, non-atomic counter and fold the batch in through
///     <see cref="Add" /> when their enumeration ends. That keeps the per-item path free of atomics
///     and of the false sharing that a single shared cache line causes under parallel execution.
/// </remarks>
public sealed class StatsCounter
{
    private long _total;

    /// <summary>
    ///     Gets the total count of items processed. Counts contributed by a stream become visible
    ///     once that stream's enumeration completes.
    /// </summary>
    public long Total => Interlocked.Read(ref _total);

    /// <summary>
    ///     Folds a locally accumulated count into the shared total.
    /// </summary>
    /// <param name="count">The number of items to add. Zero and negative values are ignored.</param>
    public void Add(long count)
    {
        if (count > 0)
            _ = Interlocked.Add(ref _total, count);
    }
}
