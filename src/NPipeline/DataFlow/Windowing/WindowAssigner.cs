using NPipeline.DataFlow.Timestamping;

namespace NPipeline.DataFlow.Windowing;

/// <summary>
///     Represents a strategy for assigning data items to time windows.
/// </summary>
public abstract class WindowAssigner
{
    /// <summary>
    ///     Assigns a data item to one or more windows based on the windowing strategy.
    /// </summary>
    /// <typeparam name="T">The type of the data item.</typeparam>
    /// <param name="item">The data item to assign to windows.</param>
    /// <param name="timestamp">The timestamp of the data item.</param>
    /// <param name="extractor">An optional timestamp extractor for items that don't implement <see cref="ITimestamped" />.</param>
    /// <returns>An enumerable of windows that the item belongs to.</returns>
    public abstract IEnumerable<IWindow> AssignWindows<T>(T item, DateTimeOffset timestamp, TimestampExtractor<T>? extractor = null);

    /// <summary>
    ///     Creates a tumbling window assigner with the specified window size.
    ///     Tumbling windows are fixed-size, non-overlapping, contiguous time intervals.
    /// </summary>
    /// <param name="windowSize">The size of each window.</param>
    /// <returns>A new <see cref="TumblingWindowAssigner" /> instance.</returns>
    public static TumblingWindowAssigner Tumbling(TimeSpan windowSize)
    {
        return new TumblingWindowAssigner(windowSize);
    }

    /// <summary>
    ///     Creates a sliding window assigner with the specified window size and slide interval.
    ///     Sliding windows are fixed-size windows that slide by a specified time interval.
    /// </summary>
    /// <param name="windowSize">The size of each window.</param>
    /// <param name="slide">The slide interval between windows.</param>
    /// <returns>A new <see cref="SlidingWindowAssigner" /> instance.</returns>
    public static SlidingWindowAssigner Sliding(TimeSpan windowSize, TimeSpan slide)
    {
        return new SlidingWindowAssigner(windowSize, slide);
    }
}

/// <summary>
///     Assigns data items to tumbling windows (fixed-size, non-overlapping windows).
/// </summary>
/// <param name="windowSize">The size of each window.</param>
public sealed class TumblingWindowAssigner(TimeSpan windowSize) : WindowAssigner
{
    /// <inheritdoc />
    public override IEnumerable<IWindow> AssignWindows<T>(T item, DateTimeOffset timestamp, TimestampExtractor<T>? extractor = null)
    {
        var window = TimeWindow.ForTimestamp(timestamp, windowSize);
        yield return window;
    }
}

/// <summary>
///     Assigns data items to sliding windows (fixed-size windows that slide by a specified interval).
/// </summary>
/// <param name="windowSize">The size of each window.</param>
/// <param name="slide">The slide interval between windows.</param>
public sealed class SlidingWindowAssigner(TimeSpan windowSize, TimeSpan slide) : WindowAssigner
{
    /// <inheritdoc />
    public override IEnumerable<IWindow> AssignWindows<T>(T item, DateTimeOffset timestamp, TimestampExtractor<T>? extractor = null)
    {
        var windowStart = TimeWindow.GetWindowStart(timestamp, slide);
        var windows = new List<IWindow>();

        // Generate all windows that contain this timestamp
        var currentStart = windowStart;

        while (currentStart + windowSize > timestamp)
        {
            if (currentStart <= timestamp)
                windows.Add(new TimeWindow(currentStart, windowSize));

            currentStart -= slide;
        }

        return windows;
    }
}
