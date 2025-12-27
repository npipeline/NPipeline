using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using NPipeline.Configuration;
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
    private readonly IDictionary<(IWindow Window, TKey Key), TAccumulate> _accumulators;
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
    /// <param name="config">Configuration controlling windowing and watermark behavior.</param>
    protected AdvancedAggregateNode(AggregateNodeConfiguration<TIn> config)
    {
        ArgumentNullException.ThrowIfNull(config);
        ArgumentNullException.ThrowIfNull(config.WindowAssigner);

        _windowAssigner = config.WindowAssigner;
        _timestampExtractor = config.TimestampExtractor;
        _maxOutOfOrderness = config.EffectiveMaxOutOfOrderness;
        _watermarkInterval = config.EffectiveWatermarkInterval;

        _accumulators = config.UseThreadSafeAccumulator
            ? new ConcurrentDictionary<(IWindow Window, TKey Key), TAccumulate>()
            : new Dictionary<(IWindow Window, TKey Key), TAccumulate>();
    }

    /// <summary>
    ///     Asynchronously disposes of the node. This can be overridden by derived classes to release resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public virtual ValueTask DisposeAsync()
    {
        _accumulators.Clear();
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

                    if (_accumulators is ConcurrentDictionary<(IWindow, TKey), TAccumulate> concurrentDict)
                    {
                        concurrentDict.AddOrUpdate(
                            windowKey,
                            _ =>
                            {
                                isNewWindow = true;
                                return Accumulate(CreateAccumulator(), item);
                            },
                            (_, current) => Accumulate(current, item));
                    }
                    else
                    {
                        if (_accumulators.TryGetValue(windowKey, out var current))
                            _accumulators[windowKey] = Accumulate(current, item);
                        else
                        {
                            isNewWindow = true;
                            _accumulators[windowKey] = Accumulate(CreateAccumulator(), item);
                        }
                    }

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
                    var removed = false;
                    TAccumulate? accumulator = default;

                    if (_accumulators is ConcurrentDictionary<(IWindow, TKey), TAccumulate> concurrentDict)
                    {
                        removed = concurrentDict.TryRemove(windowKey, out var value);
                        accumulator = value;
                    }
                    else
                    {
                        if (_accumulators.TryGetValue(windowKey, out var value))
                        {
                            removed = _accumulators.Remove(windowKey);
                            accumulator = value;
                        }
                    }

                    if (removed && accumulator != null)
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
