using NPipeline.Utils;

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
    public static Watermark Now() => new(DateTimeOffset.UtcNow);

    /// <summary>
    ///     Creates a watermark with the specified timestamp.
    /// </summary>
    /// <param name="timestamp">The timestamp for the watermark.</param>
    /// <returns>A new <see cref="Watermark" /> instance.</returns>
    public static Watermark Create(DateTimeOffset timestamp) => new(timestamp);

    /// <summary>
    ///     Determines whether the specified timestamp is earlier than this watermark.
    /// </summary>
    /// <param name="timestamp">The timestamp to check.</param>
    /// <returns><c>true</c> if the timestamp is earlier than the watermark; otherwise, <c>false</c>.</returns>
    public bool IsEarlierThan(DateTimeOffset timestamp) => timestamp < Timestamp;

    /// <summary>
    ///     Determines whether the specified timestamp is later than or equal to this watermark.
    /// </summary>
    /// <param name="timestamp">The timestamp to check.</param>
    /// <returns><c>true</c> if the timestamp is later than or equal to the watermark; otherwise, <c>false</c>.</returns>
    public bool IsLaterThanOrEqual(DateTimeOffset timestamp) => timestamp >= Timestamp;

    /// <inheritdoc />
    public override string ToString() => $"Watermark({Timestamp:O})";
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
    public static BoundedOutOfOrdernessWatermarkGenerator<T> BoundedOutOfOrderness<T>(TimeSpan maxOutOfOrderness) => new(maxOutOfOrderness);

    /// <summary>
    ///     Creates a periodic watermark generator.
    /// </summary>
    /// <param name="interval">The interval at which to emit watermarks.</param>
    /// <param name="maxOutOfOrderness">The maximum allowed out-of-orderness.</param>
    /// <returns>A new <see cref="PeriodicWatermarkGenerator{T}" /> instance.</returns>
    public static PeriodicWatermarkGenerator<T> Periodic<T>(TimeSpan interval, TimeSpan maxOutOfOrderness) => new(interval, maxOutOfOrderness);
}

/// <summary>
///     A watermark generator that uses bounded out-of-orderness to determine watermarks.
/// </summary>
/// <typeparam name="T">The type of the data items.</typeparam>
public sealed class BoundedOutOfOrdernessWatermarkGenerator<T> : WatermarkGenerator<T>
{
    private readonly TimeSpan _maxOutOfOrderness;
    private DateTimeOffset _maxTimestamp = DateTimeOffset.MinValue;

    /// <summary>
    ///     Initializes a new instance of the <see cref="BoundedOutOfOrdernessWatermarkGenerator{T}" /> class.
    /// </summary>
    /// <param name="maxOutOfOrderness">The maximum allowed out-of-orderness. Must not be negative.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="maxOutOfOrderness" /> is negative.</exception>
    public BoundedOutOfOrdernessWatermarkGenerator(TimeSpan maxOutOfOrderness)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxOutOfOrderness, TimeSpan.Zero);
        _maxOutOfOrderness = maxOutOfOrderness;
    }

    /// <inheritdoc />
    public override void Update(DateTimeOffset timestamp)
    {
        if (timestamp > _maxTimestamp)
            _maxTimestamp = timestamp;
    }

    /// <inheritdoc />
    public override Watermark GetCurrentWatermark() =>
        _maxTimestamp == DateTimeOffset.MinValue
            ? new Watermark(DateTimeOffset.MinValue)
            : new Watermark(TimestampUtils.SafeSubtract(_maxTimestamp, _maxOutOfOrderness));
}

/// <summary>
///     A watermark generator that emits watermarks periodically.
/// </summary>
/// <typeparam name="T">The type of the data items.</typeparam>
public sealed class PeriodicWatermarkGenerator<T> : WatermarkGenerator<T>
{
    private readonly TimeSpan _interval;
    private readonly TimeSpan _maxOutOfOrderness;
    private Watermark _current = new(DateTimeOffset.MinValue);
    private DateTimeOffset _lastEmitTime = DateTimeOffset.MinValue;
    private DateTimeOffset _maxTimestamp = DateTimeOffset.MinValue;

    /// <summary>
    ///     Initializes a new instance of the <see cref="PeriodicWatermarkGenerator{T}" /> class.
    /// </summary>
    /// <param name="interval">The interval at which watermarks are emitted. Must be positive.</param>
    /// <param name="maxOutOfOrderness">The maximum allowed out-of-orderness. Must not be negative.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="interval" /> is zero or negative, or <paramref name="maxOutOfOrderness" /> is negative.
    /// </exception>
    public PeriodicWatermarkGenerator(TimeSpan interval, TimeSpan maxOutOfOrderness)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxOutOfOrderness, TimeSpan.Zero);
        _interval = interval;
        _maxOutOfOrderness = maxOutOfOrderness;
    }

    /// <inheritdoc />
    public override void Update(DateTimeOffset timestamp)
    {
        if (timestamp > _maxTimestamp)
            _maxTimestamp = timestamp;
    }

    /// <inheritdoc />
    public override Watermark GetCurrentWatermark()
    {
        var now = DateTimeOffset.UtcNow;

        if (now - _lastEmitTime >= _interval)
        {
            _lastEmitTime = now;

            if (_maxTimestamp != DateTimeOffset.MinValue)
            {
                var candidate = TimestampUtils.SafeSubtract(_maxTimestamp, _maxOutOfOrderness);

                if (candidate > _current.Timestamp)
                    _current = new Watermark(candidate);
            }
        }

        return _current;
    }
}
