namespace NPipeline.DataFlow.Watermarks;

/// <summary>
///     Represents a watermark in a stream processing system.
///     A watermark is a special timestamp that indicates that no events with timestamps
///     earlier than this watermark are expected to arrive.
/// </summary>
public sealed record Watermark(DateTimeOffset Timestamp)
{
    /// <summary>
    ///     Gets the timestamp of the watermark.
    /// </summary>
    public DateTimeOffset Timestamp { get; } = Timestamp;

    /// <summary>
    ///     Creates a watermark with the current timestamp.
    /// </summary>
    /// <returns>A new <see cref="Watermark" /> instance.</returns>
    public static Watermark Now()
    {
        return new Watermark(DateTimeOffset.UtcNow);
    }

    /// <summary>
    ///     Creates a watermark with the specified timestamp.
    /// </summary>
    /// <param name="timestamp">The timestamp for the watermark.</param>
    /// <returns>A new <see cref="Watermark" /> instance.</returns>
    public static Watermark Create(DateTimeOffset timestamp)
    {
        return new Watermark(timestamp);
    }

    /// <summary>
    ///     Determines whether the specified timestamp is earlier than this watermark.
    /// </summary>
    /// <param name="timestamp">The timestamp to check.</param>
    /// <returns><c>true</c> if the timestamp is earlier than the watermark; otherwise, <c>false</c>.</returns>
    public bool IsEarlierThan(DateTimeOffset timestamp)
    {
        return timestamp < Timestamp;
    }

    /// <summary>
    ///     Determines whether the specified timestamp is later than or equal to this watermark.
    /// </summary>
    /// <param name="timestamp">The timestamp to check.</param>
    /// <returns><c>true</c> if the timestamp is later than or equal to the watermark; otherwise, <c>false</c>.</returns>
    public bool IsLaterThanOrEqual(DateTimeOffset timestamp)
    {
        return timestamp >= Timestamp;
    }

    /// <inheritdoc />
    public override string ToString()
    {
        return $"Watermark({Timestamp:O})";
    }
}

/// <summary>
///     Represents a generator that produces watermarks based on event timestamps.
/// </summary>
/// <typeparam name="T">The type of the data items.</typeparam>
public abstract class WatermarkGenerator<T>
{
    /// <summary>
    ///     Updates the watermark generator with a new event timestamp.
    /// </summary>
    /// <param name="timestamp">The timestamp of the event.</param>
    public abstract void Update(DateTimeOffset timestamp);

    /// <summary>
    ///     Gets the current watermark.
    /// </summary>
    /// <returns>The current watermark.</returns>
    public abstract Watermark GetCurrentWatermark();
}

/// <summary>
///     Factory methods for creating watermark generators.
/// </summary>
public static class WatermarkGenerators
{
    /// <summary>
    ///     Creates a bounded out-of-orderness watermark generator.
    /// </summary>
    /// <param name="maxOutOfOrderness">The maximum allowed out-of-orderness.</param>
    /// <returns>A new <see cref="BoundedOutOfOrdernessWatermarkGenerator{T}" /> instance.</returns>
    public static BoundedOutOfOrdernessWatermarkGenerator<T> BoundedOutOfOrderness<T>(TimeSpan maxOutOfOrderness)
    {
        return new BoundedOutOfOrdernessWatermarkGenerator<T>(maxOutOfOrderness);
    }

    /// <summary>
    ///     Creates a periodic watermark generator.
    /// </summary>
    /// <param name="interval">The interval at which to emit watermarks.</param>
    /// <param name="maxOutOfOrderness">The maximum allowed out-of-orderness.</param>
    /// <returns>A new <see cref="PeriodicWatermarkGenerator{T}" /> instance.</returns>
    public static PeriodicWatermarkGenerator<T> Periodic<T>(TimeSpan interval, TimeSpan maxOutOfOrderness)
    {
        return new PeriodicWatermarkGenerator<T>(interval, maxOutOfOrderness);
    }
}

/// <summary>
///     A watermark generator that uses bounded out-of-orderness to determine watermarks.
/// </summary>
/// <typeparam name="T">The type of the data items.</typeparam>
/// <param name="maxOutOfOrderness">The maximum allowed out-of-orderness.</param>
public sealed class BoundedOutOfOrdernessWatermarkGenerator<T>(TimeSpan maxOutOfOrderness) : WatermarkGenerator<T>
{
    private DateTimeOffset _maxTimestamp = DateTimeOffset.MinValue;

    /// <inheritdoc />
    public override void Update(DateTimeOffset timestamp)
    {
        if (timestamp > _maxTimestamp)
            _maxTimestamp = timestamp;
    }

    /// <inheritdoc />
    public override Watermark GetCurrentWatermark()
    {
        // Prevent underflow when _maxTimestamp is DateTimeOffset.MinValue (no events yet)
        return _maxTimestamp == DateTimeOffset.MinValue
            ? new Watermark(DateTimeOffset.MinValue)
            : new Watermark(SafeSubtract(_maxTimestamp, maxOutOfOrderness));
    }

    /// <summary>
    ///     Safely subtracts a TimeSpan from a DateTimeOffset, preventing underflow to DateTimeOffset.MinValue.
    /// </summary>
    private static DateTimeOffset SafeSubtract(DateTimeOffset ts, TimeSpan delta)
    {
        if (ts == DateTimeOffset.MinValue)
            return DateTimeOffset.MinValue;

        // If delta is larger than the distance to MinValue, just return MinValue
        var distanceToMin = ts - DateTimeOffset.MinValue;

        if (delta >= distanceToMin)
            return DateTimeOffset.MinValue;

        return ts - delta;
    }
}

/// <summary>
///     A watermark generator that emits watermarks periodically.
/// </summary>
/// <typeparam name="T">The type of the data items.</typeparam>
/// <param name="interval">The interval at which to emit watermarks.</param>
/// <param name="maxOutOfOrderness">The maximum allowed out-of-orderness.</param>
public sealed class PeriodicWatermarkGenerator<T>(TimeSpan interval, TimeSpan maxOutOfOrderness) : WatermarkGenerator<T>
{
    private DateTimeOffset _lastEmittedWatermark = DateTimeOffset.MinValue;
    private DateTimeOffset _maxTimestamp = DateTimeOffset.MinValue;

    /// <inheritdoc />
    public override void Update(DateTimeOffset timestamp)
    {
        if (timestamp > _maxTimestamp)
            _maxTimestamp = timestamp;
    }

    /// <inheritdoc />
    public override Watermark GetCurrentWatermark()
    {
        var currentTime = DateTimeOffset.UtcNow;

        if (currentTime - _lastEmittedWatermark >= interval)
        {
            _lastEmittedWatermark = currentTime;

            return _maxTimestamp == DateTimeOffset.MinValue
                ? new Watermark(DateTimeOffset.MinValue)
                : new Watermark(SafeSubtract(_maxTimestamp, maxOutOfOrderness));
        }

        return new Watermark(_lastEmittedWatermark == DateTimeOffset.MinValue
            ? DateTimeOffset.MinValue
            : _lastEmittedWatermark);
    }

    /// <summary>
    ///     Safely subtracts a TimeSpan from a DateTimeOffset, preventing underflow to DateTimeOffset.MinValue.
    /// </summary>
    private static DateTimeOffset SafeSubtract(DateTimeOffset ts, TimeSpan delta)
    {
        if (ts == DateTimeOffset.MinValue)
            return DateTimeOffset.MinValue;

        // If delta is larger than the distance to MinValue, just return MinValue
        var distanceToMin = ts - DateTimeOffset.MinValue;

        if (delta >= distanceToMin)
            return DateTimeOffset.MinValue;

        return ts - delta;
    }
}
