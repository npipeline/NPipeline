using System.Diagnostics.CodeAnalysis;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Timestamping;

namespace NPipeline.Utils;

/// <summary>
///     Provides utility methods for extracting timestamps from data items.
/// </summary>
public static class TimestampUtils
{
    /// <summary>
    ///     Extracts a timestamp from a data item using the provided extractor function.
    ///     If the item implements <see cref="ITimestamped" />, the extractor is not used.
    /// </summary>
    /// <typeparam name="T">The type of the data item.</typeparam>
    /// <param name="item">The data item to extract the timestamp from.</param>
    /// <param name="extractor">An optional extractor function for items that don't implement <see cref="ITimestamped" />.</param>
    /// <returns>The extracted timestamp.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when the item doesn't implement <see cref="ITimestamped" /> and no extractor was provided.
    ///     Use <c>ResolveEventTime</c> for an arrival-time fallback instead of throwing.
    /// </exception>
    public static DateTimeOffset ExtractTimestamp<T>(T item, TimestampExtractor<T>? extractor = null)
    {
        if (item is ITimestamped timestamped)
            return timestamped.Timestamp;

        if (extractor is not null)
            return extractor(item);

        throw new InvalidOperationException(
            $"Cannot extract timestamp from item of type '{typeof(T).Name}'. " +
            $"Either implement {nameof(ITimestamped)} or provide a {nameof(TimestampExtractor<T>)}.");
    }

    /// <summary>
    ///     Attempts to extract a timestamp from a data item using the provided extractor function.
    ///     If the item implements <see cref="ITimestamped" />, the extractor is not used.
    /// </summary>
    /// <typeparam name="T">The type of the data item.</typeparam>
    /// <param name="item">The data item to extract the timestamp from.</param>
    /// <param name="timestamp">The extracted timestamp, if successful.</param>
    /// <param name="extractor">An optional extractor function for items that don't implement <see cref="ITimestamped" />.</param>
    /// <returns><c>true</c> if the timestamp was successfully extracted; otherwise, <c>false</c>.</returns>
    public static bool TryExtractTimestamp<T>(T item, [NotNullWhen(true)] out DateTimeOffset? timestamp, TimestampExtractor<T>? extractor = null)
    {
        timestamp = null;

        if (item is ITimestamped timestamped)
        {
            timestamp = timestamped.Timestamp;
            return true;
        }

        if (extractor is not null)
        {
            timestamp = extractor(item);
            return true;
        }

        return false;
    }

    /// <summary>
    ///     Resolves an item's event time: <see cref="ITimestamped.Timestamp" />, else the extractor, else arrival time.
    ///     Unlike <see cref="ExtractTimestamp{T}(T, TimestampExtractor{T})" />, this never throws for items that are not
    ///     <see cref="ITimestamped" />.
    /// </summary>
    /// <typeparam name="T">The type of the data item.</typeparam>
    /// <param name="item">The data item to resolve the event time for.</param>
    /// <param name="extractor">An optional extractor function for items that don't implement <see cref="ITimestamped" />.</param>
    /// <returns>The resolved event time, or arrival time when neither source is available.</returns>
    internal static DateTimeOffset ResolveEventTime<T>(T item, TimestampExtractor<T>? extractor) =>
        item is ITimestamped t ? t.Timestamp
        : extractor is not null ? extractor(item)
        : DateTimeOffset.UtcNow;

    /// <summary>
    ///     Subtracts <paramref name="delta" /> from <paramref name="timestamp" />, clamping at
    ///     <see cref="DateTimeOffset.MinValue" /> to prevent underflow.
    /// </summary>
    /// <param name="timestamp">The timestamp to subtract from.</param>
    /// <param name="delta">The delta to subtract.</param>
    /// <returns>The clamped result of the subtraction.</returns>
    internal static DateTimeOffset SafeSubtract(DateTimeOffset timestamp, TimeSpan delta) =>
        timestamp == DateTimeOffset.MinValue || delta >= timestamp - DateTimeOffset.MinValue
            ? DateTimeOffset.MinValue
            : timestamp - delta;
}
