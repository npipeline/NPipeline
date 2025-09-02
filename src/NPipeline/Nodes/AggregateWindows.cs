using NPipeline.DataFlow.Windowing;

namespace NPipeline.Nodes;

/// <summary>
///     Non-generic window factory helpers for aggregate nodes (to satisfy CA1000).
/// </summary>
public static class AggregateWindows
{
    /// <summary>
    ///     Creates a tumbling window assigner with the specified window size.
    /// </summary>
    /// <param name="windowSize">The size of each window.</param>
    /// <returns>A new <see cref="TumblingWindowAssigner" /> instance.</returns>
    public static TumblingWindowAssigner Tumbling(TimeSpan windowSize)
    {
        return new TumblingWindowAssigner(windowSize);
    }

    /// <summary>
    ///     Creates a sliding window assigner with the specified window size and slide interval.
    /// </summary>
    /// <param name="windowSize">The size of each window.</param>
    /// <param name="slide">The slide interval between windows.</param>
    /// <returns>A new <see cref="SlidingWindowAssigner" /> instance.</returns>
    public static SlidingWindowAssigner Sliding(TimeSpan windowSize, TimeSpan slide)
    {
        return new SlidingWindowAssigner(windowSize, slide);
    }
}
