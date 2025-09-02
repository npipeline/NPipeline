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
}
