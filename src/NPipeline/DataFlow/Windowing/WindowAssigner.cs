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
    public static TumblingWindowAssigner Tumbling(TimeSpan windowSize) => new(windowSize);

    /// <summary>
    ///     Creates a sliding window assigner with the specified window size and slide interval.
    ///     Sliding windows are fixed-size windows that slide by a specified time interval.
    /// </summary>
    /// <param name="windowSize">The size of each window.</param>
    /// <param name="slide">The slide interval between windows.</param>
    /// <returns>A new <see cref="SlidingWindowAssigner" /> instance.</returns>
    public static SlidingWindowAssigner Sliding(TimeSpan windowSize, TimeSpan slide) => new(windowSize, slide);
}

/// <summary>
///     Assigns data items to tumbling windows (fixed-size, non-overlapping windows).
/// </summary>
public sealed class TumblingWindowAssigner : WindowAssigner
{
    private readonly TimeSpan _windowSize;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TumblingWindowAssigner" /> class.
    /// </summary>
    /// <param name="windowSize">The size of each window. Must be positive.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="windowSize" /> is zero or negative.</exception>
    public TumblingWindowAssigner(TimeSpan windowSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(windowSize, TimeSpan.Zero);
        _windowSize = windowSize;
    }

    /// <summary>
    ///     Gets the size of each window.
    /// </summary>
    public TimeSpan WindowSize => _windowSize;

    /// <inheritdoc />
    public override IEnumerable<IWindow> AssignWindows<T>(T item, DateTimeOffset timestamp, TimestampExtractor<T>? extractor = null)
    {
        var window = TimeWindow.ForTimestamp(timestamp, _windowSize);
        yield return window;
    }
}

/// <summary>
///     Assigns data items to sliding windows (fixed-size windows that slide by a specified interval).
/// </summary>
public sealed class SlidingWindowAssigner : WindowAssigner
{
    private readonly TimeSpan _slide;
    private readonly TimeSpan _windowSize;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SlidingWindowAssigner" /> class.
    /// </summary>
    /// <param name="windowSize">The size of each window. Must be positive.</param>
    /// <param name="slide">The slide interval between windows. Must be positive.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="windowSize" /> or <paramref name="slide" /> is zero or negative.
    /// </exception>
    public SlidingWindowAssigner(TimeSpan windowSize, TimeSpan slide)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(windowSize, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(slide, TimeSpan.Zero);
        _windowSize = windowSize;
        _slide = slide;
    }

    /// <summary>
    ///     Gets the size of each window.
    /// </summary>
    public TimeSpan WindowSize => _windowSize;

    /// <summary>
    ///     Gets the slide interval between windows.
    /// </summary>
    public TimeSpan Slide => _slide;

    /// <inheritdoc />
    public override IEnumerable<IWindow> AssignWindows<T>(T item, DateTimeOffset timestamp, TimestampExtractor<T>? extractor = null)
    {
        var windowStart = TimeWindow.GetWindowStart(timestamp, _slide);
        var windows = new List<IWindow>();

        // Generate all windows that contain this timestamp
        var currentStart = windowStart;

        while (currentStart + _windowSize > timestamp)
        {
            if (currentStart <= timestamp)
                windows.Add(new TimeWindow(currentStart, _windowSize));

            currentStart -= _slide;
        }

        return windows;
    }
}
