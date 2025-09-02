namespace NPipeline.DataFlow.Windowing;

/// <summary>
///     Represents a time window with a start and end time.
///     Windows are typically used for grouping events that occur within a specific time range.
/// </summary>
public interface IWindow
{
    /// <summary>
    ///     Gets the start time of the window (inclusive).
    /// </summary>
    DateTimeOffset Start { get; }

    /// <summary>
    ///     Gets the end time of the window (exclusive).
    /// </summary>
    DateTimeOffset End { get; }

    /// <summary>
    ///     Gets the duration of the window.
    /// </summary>
    TimeSpan Duration { get; }

    /// <summary>
    ///     Determines whether the specified timestamp falls within this window.
    /// </summary>
    /// <param name="timestamp">The timestamp to check.</param>
    /// <returns><c>true</c> if the timestamp is within the window; otherwise, <c>false</c>.</returns>
    bool Contains(DateTimeOffset timestamp);
}

/// <summary>
///     Represents a fixed time window with a specific start time and duration.
/// </summary>
/// <param name="Start">The start time of the window (inclusive).</param>
/// <param name="Duration">The duration of the window.</param>
public sealed record TimeWindow(DateTimeOffset Start, TimeSpan Duration) : IWindow
{
    /// <inheritdoc />
    public DateTimeOffset End => Start + Duration;

    /// <inheritdoc />
    public bool Contains(DateTimeOffset timestamp)
    {
        return timestamp >= Start && timestamp < End;
    }

    /// <summary>
    ///     Creates a new time window that starts at the specified time and has the given duration.
    /// </summary>
    /// <param name="start">The start time of the window.</param>
    /// <param name="duration">The duration of the window.</param>
    /// <returns>A new <see cref="TimeWindow" /> instance.</returns>
    public static TimeWindow Create(DateTimeOffset start, TimeSpan duration)
    {
        return new TimeWindow(start, duration);
    }

    /// <summary>
    ///     Creates a time window that contains the specified timestamp.
    /// </summary>
    /// <param name="timestamp">The timestamp to find a window for.</param>
    /// <param name="windowSize">The size of each window.</param>
    /// <returns>A <see cref="TimeWindow" /> that contains the specified timestamp.</returns>
    public static TimeWindow ForTimestamp(DateTimeOffset timestamp, TimeSpan windowSize)
    {
        var windowStart = GetWindowStart(timestamp, windowSize);
        return new TimeWindow(windowStart, windowSize);
    }

    /// <summary>
    ///     Calculates the start time of the window that contains the specified timestamp.
    /// </summary>
    /// <param name="timestamp">The timestamp to find the window start for.</param>
    /// <param name="windowSize">The size of each window.</param>
    /// <returns>The start time of the window that contains the timestamp.</returns>
    public static DateTimeOffset GetWindowStart(DateTimeOffset timestamp, TimeSpan windowSize)
    {
        var ticksSinceEpoch = timestamp.Ticks;
        var windowTicks = windowSize.Ticks;
        var windowNumber = ticksSinceEpoch / windowTicks;
        return new DateTimeOffset(windowNumber * windowTicks, timestamp.Offset);
    }
}
