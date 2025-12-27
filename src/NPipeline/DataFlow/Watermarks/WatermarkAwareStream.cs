using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.Watermarks;

/// <summary>
///     Represents an item in a stream that can be either a data item or a watermark.
/// </summary>
/// <typeparam name="T">The type of the data items.</typeparam>
public abstract record StreamItem<T>
{
    private StreamItem()
    {
    }

    /// <summary>
    ///     Represents a data item in the stream.
    /// </summary>
    /// <param name="Value">The data value.</param>
    public sealed record DataItem(T Value) : StreamItem<T>;

    /// <summary>
    ///     Represents a watermark in the stream.
    /// </summary>
    /// <param name="Watermark">The watermark.</param>
    public sealed record WatermarkItem(Watermark Watermark) : StreamItem<T>;
}

/// <summary>
///     Provides extension methods for working with watermark-aware streams.
/// </summary>
public static class WatermarkAwareStreamExtensions
{
    /// <summary>
    ///     Converts a regular async enumerable to a watermark-aware stream by injecting watermarks.
    /// </summary>
    /// <typeparam name="T">The type of the data items.</typeparam>
    /// <param name="source">The source stream.</param>
    /// <param name="watermarkGenerator">The watermark generator to use.</param>
    /// <param name="watermarkInterval">The interval at which to emit watermarks.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>A watermark-aware stream.</returns>
    public static async IAsyncEnumerable<StreamItem<T>> WithWatermarks<T>(
        this IAsyncEnumerable<T> source,
        WatermarkGenerator<T> watermarkGenerator,
        TimeSpan watermarkInterval,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var lastWatermarkTime = DateTimeOffset.UtcNow;

        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            // Extract timestamp from the item
            DateTimeOffset timestamp;

            if (item is ITimestamped timestamped)
                timestamp = timestamped.Timestamp;
            else
                timestamp = DateTimeOffset.UtcNow;

            watermarkGenerator.Update(timestamp);

            // Check if it's time to emit a watermark
            var currentTime = DateTimeOffset.UtcNow;

            if (currentTime - lastWatermarkTime >= watermarkInterval)
            {
                lastWatermarkTime = currentTime;
                var watermark = watermarkGenerator.GetCurrentWatermark();
                yield return new StreamItem<T>.WatermarkItem(watermark);
            }

            yield return new StreamItem<T>.DataItem(item);
        }

        // Emit final watermark at the end of the stream
        var finalWatermark = watermarkGenerator.GetCurrentWatermark();
        yield return new StreamItem<T>.WatermarkItem(finalWatermark);
    }

    /// <summary>
    ///     Processes a watermark-aware stream, handling both data items and watermarks.
    /// </summary>
    /// <typeparam name="T">The type of the data items.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The watermark-aware stream.</param>
    /// <param name="dataProcessor">Function to process data items.</param>
    /// <param name="watermarkProcessor">Function to process watermarks.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>An async enumerable of processed results.</returns>
    public static async IAsyncEnumerable<TResult> ProcessWithWatermarks<T, TResult>(
        this IAsyncEnumerable<StreamItem<T>> source,
        Func<T, TResult> dataProcessor,
        Func<Watermark, TResult> watermarkProcessor,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            var result = item switch
            {
                StreamItem<T>.DataItem data => dataProcessor(data.Value),
                StreamItem<T>.WatermarkItem watermark => watermarkProcessor(watermark.Watermark),
                _ => throw new InvalidOperationException("Unknown stream item type"),
            };

            yield return result;
        }
    }

    /// <summary>
    ///     Filters out late data based on the latest watermark.
    /// </summary>
    /// <typeparam name="T">The type of the data items.</typeparam>
    /// <param name="source">The watermark-aware stream.</param>
    /// <param name="cancellationToken"></param>
    /// <returns>A stream with late data filtered out.</returns>
    public static async IAsyncEnumerable<StreamItem<T>> FilterLateData<T>(
        this IAsyncEnumerable<StreamItem<T>> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Watermark? latestWatermark = null;

        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            if (item is StreamItem<T>.WatermarkItem watermarkItem)
            {
                latestWatermark = watermarkItem.Watermark;
                yield return item;
            }
            else if (item is StreamItem<T>.DataItem dataItem)
            {
                if (latestWatermark is null)
                {
                    // No watermark yet, process all data
                    yield return item;
                }
                else
                {
                    // Check if data is late
                    DateTimeOffset timestamp;

                    if (dataItem.Value is ITimestamped timestamped)
                        timestamp = timestamped.Timestamp;
                    else
                        timestamp = DateTimeOffset.UtcNow;

                    if (!latestWatermark.IsEarlierThan(timestamp))
                        yield return item;

                    // Else: data is late, filter it out
                }
            }
        }
    }
}
