using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Watermarks;
using NPipeline.DataFlow.Windowing;
using NPipeline.Utils;

namespace NPipeline.Nodes;

/// <summary>
///     An abstract base class for creating advanced nodes that perform aggregations on a stream of data
///     within specific time windows. This class provides full control over the accumulator and result types.
/// </summary>
/// <remarks>
///     This is an advanced base class that requires implementing four type parameters. For simpler
///     aggregation scenarios where the accumulator and result types are the same, use <see cref="AggregateNode{TIn, TKey, TResult}" /> instead.
/// </remarks>
/// <typeparam name="TIn">The type of the input data.</typeparam>
/// <typeparam name="TKey">The type of the key used for grouping. Must be not-null.</typeparam>
/// <typeparam name="TAccumulate">The type of the accumulator value.</typeparam>
/// <typeparam name="TResult">The type of the aggregation result.</typeparam>
public abstract class AdvancedAggregateNode<TIn, TKey, TAccumulate, TResult> : IAggregateNode where TKey : notnull
{
    private readonly ConcurrentDictionary<(IWindow Window, TKey Key), TAccumulate> _accumulators = new();
    private readonly TimeSpan _maxOutOfOrderness;
    private readonly TimestampExtractor<TIn>? _timestampExtractor;
    private readonly TimeSpan _watermarkInterval;
    private readonly WindowAssigner _windowAssigner;
    private long _maxConcurrentWindows;
    private long _totalWindowsClosed;
    private long _totalWindowsProcessed;

    /// <summary>
    ///     Initializes a new instance of <see cref="AdvancedAggregateNode{TIn, TKey, TAccumulate, TResult}" /> class.
    /// </summary>
    /// <param name="windowAssigner">The window assigner strategy to use.</param>
    /// <param name="timestampExtractor">Optional timestamp extractor for input type.</param>
    /// <param name="maxOutOfOrderness">Maximum time span for out-of-order items. Defaults to 5 minutes.</param>
    /// <param name="watermarkInterval">Interval for watermark updates. Defaults to 30 seconds.</param>
    protected AdvancedAggregateNode(
        WindowAssigner windowAssigner,
        TimestampExtractor<TIn>? timestampExtractor = null,
        TimeSpan? maxOutOfOrderness = null,
        TimeSpan? watermarkInterval = null)
    {
        ArgumentNullException.ThrowIfNull(windowAssigner);
        _windowAssigner = windowAssigner;
        _timestampExtractor = timestampExtractor;
        _maxOutOfOrderness = maxOutOfOrderness ?? TimeSpan.FromMinutes(5);
        _watermarkInterval = watermarkInterval ?? TimeSpan.FromSeconds(30);
    }

    /// <summary>
    ///     Asynchronously disposes of the node. This can be overridden by derived classes to release resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<object?> ExecuteAsync(
        IAsyncEnumerable<object?> inputStream,
        CancellationToken cancellationToken = default)
    {
        var typedInputStream = ConvertToTypedAsyncEnumerable(inputStream);

        var watermarkAwareStream = typedInputStream.WithWatermarks(
            new BoundedOutOfOrdernessWatermarkGenerator<TIn>(_maxOutOfOrderness),
            _watermarkInterval,
            cancellationToken);

        var outputStream = AggregateStreamAsync(watermarkAwareStream, cancellationToken);

        return ValueTask.FromResult<object?>(ConvertStream(outputStream));

        // Convert TResult stream to object? stream for the interface
        async IAsyncEnumerable<object?> ConvertStream(IAsyncEnumerable<TResult> source)
        {
            await foreach (var item in source.WithCancellation(cancellationToken))
            {
                yield return item;
            }
        }
    }

    /// <summary>
    ///     Extracts the key from an input item for grouping.
    /// </summary>
    /// <param name="item">The input item.</param>
    /// <returns>The key for grouping.</returns>
    public abstract TKey GetKey(TIn item);

    /// <summary>
    ///     Creates an initial accumulator value for a new group.
    /// </summary>
    /// <returns>The initial accumulator value.</returns>
    public abstract TAccumulate CreateAccumulator();

    /// <summary>
    ///     Accumulates an input item into an accumulator.
    /// </summary>
    /// <param name="accumulator">The current accumulator value.</param>
    /// <param name="item">The input item to accumulate.</param>
    /// <returns>The updated accumulator value.</returns>
    public abstract TAccumulate Accumulate(TAccumulate accumulator, TIn item);

    /// <summary>
    ///     Produces the final result from an accumulator.
    /// </summary>
    /// <param name="accumulator">The final accumulator value.</param>
    /// <returns>The aggregation result.</returns>
    public abstract TResult GetResult(TAccumulate accumulator);

    /// <summary>
    ///     Gets metrics about the node's operation.
    /// </summary>
    /// <returns>A tuple containing metrics about windows processed, closed, and maximum concurrency.</returns>
    public (long TotalWindowsProcessed, long TotalWindowsClosed, long MaxConcurrentWindows) GetMetrics()
    {
        return (Interlocked.Read(ref _totalWindowsProcessed), Interlocked.Read(ref _totalWindowsClosed), Interlocked.Read(ref _maxConcurrentWindows));
    }

    /// <summary>
    ///     Gets the current number of active windows being tracked.
    /// </summary>
    /// <returns>The current number of active windows.</returns>
    public int GetActiveWindowCount()
    {
        return _accumulators.Count;
    }

    private async IAsyncEnumerable<TResult> AggregateStreamAsync(IAsyncEnumerable<StreamItem<TIn>> inputStream,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var streamItem in inputStream.WithCancellation(cancellationToken))
        {
            if (streamItem is StreamItem<TIn>.DataItem dataItem)
            {
                var item = dataItem.Value;
                var timestamp = TimestampUtils.ExtractTimestamp(item, _timestampExtractor);
                var key = GetKey(item);
                var windows = _windowAssigner.AssignWindows(item, timestamp, _timestampExtractor);

                foreach (var window in windows)
                {
                    var windowKey = (window, key);
                    var isNewWindow = false;

                    _accumulators.AddOrUpdate(
                        windowKey,
                        _ =>
                        {
                            isNewWindow = true;
                            return Accumulate(CreateAccumulator(), item);
                        },
                        (_, Current) => Accumulate(Current, item));

                    if (isNewWindow)
                    {
                        Interlocked.Increment(ref _totalWindowsProcessed);
                        var currentCount = _accumulators.Count;
                        var maxCount = Interlocked.Read(ref _maxConcurrentWindows);

                        if (currentCount > maxCount)
                            _ = Interlocked.Exchange(ref _maxConcurrentWindows, currentCount);
                    }
                }
            }
            else if (streamItem is StreamItem<TIn>.WatermarkItem watermarkItem)
            {
                var watermark = watermarkItem.Watermark;

                // Close windows that end before this watermark
                var windowsToClose = _accumulators.Keys
                    .Where(k => k.Window.End <= watermark.Timestamp)
                    .ToList();

                foreach (var windowKey in windowsToClose)
                {
                    if (_accumulators.TryRemove(windowKey, out var accumulator))
                    {
                        _ = Interlocked.Increment(ref _totalWindowsClosed);
                        yield return GetResult(accumulator);
                    }
                }
            }
        }

        // At end of stream, emit all remaining results
        foreach (var kvp in _accumulators)
        {
            yield return GetResult(kvp.Value);
        }
    }

    private async IAsyncEnumerable<TIn> ConvertToTypedAsyncEnumerable(IAsyncEnumerable<object?> asyncEnumerable)
    {
        await foreach (var item in asyncEnumerable)
        {
            if (item is TIn typedItem)
                yield return typedItem;
        }
    }
}
