namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     Stats holder used for atomic total item counting.
///     Stored in PipelineContext.Items under the key "stats.totalProcessedItems".
/// </summary>
public sealed class StatsCounter
{
    private long _total;

    /// <summary>
    ///     Gets the total count of items processed.
    /// </summary>
    public long Total => _total;

    /// <summary>
    ///     Gets a reference to the internal total counter for atomic operations.
    ///     This is exposed for use with Interlocked operations.
    /// </summary>
    /// <returns>A reference to the total counter.</returns>
    public ref long GetTotalRef()
    {
        return ref _total;
    }
}
