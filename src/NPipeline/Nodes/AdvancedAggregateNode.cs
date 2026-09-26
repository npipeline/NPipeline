using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NPipeline.Configuration;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Windowing;
using NPipeline.Utils;

namespace NPipeline.Nodes;

/// <summary>
///     An abstract base class for creating advanced nodes that perform aggregations on a stream of data
///     within specific time windows. This class provides full control over the accumulator and result types.
/// </summary>
/// <remarks>
///     <para>
///         This is an advanced base class that requires implementing four type parameters. For simpler
///         aggregation scenarios where the accumulator and result types are the same, use <see cref="AggregateNode{TIn, TKey, TResult}" /> instead.
///     </para>
///     <para>
///         Windows are assigned in event time and closed by a watermark computed from the same timestamps, so
///         historical, replayed or back-filled data aggregates correctly. Items that arrive after their window has
///         been emitted are dropped and counted in <see cref="LateItemsDropped" />.
///     </para>
/// </remarks>
/// <typeparam name="TIn">The type of the input data.</typeparam>
/// <typeparam name="TKey">The type of the key used for grouping. Must be not-null.</typeparam>
/// <typeparam name="TAccumulate">The type of the accumulator value.</typeparam>
/// <typeparam name="TResult">The type of the aggregation result.</typeparam>
public abstract class AdvancedAggregateNode<TIn, TKey, TAccumulate, TResult> : IAggregateNode where TKey : notnull
{
    private readonly TimeSpan _maxOutOfOrderness;
    private readonly TimestampExtractor<TIn>? _timestampExtractor;
    private readonly TimeSpan _watermarkInterval;
    private readonly WindowAssigner _windowAssigner;
    private long _activeGroups;
    private long _lateItemsDropped;
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
    }

    /// <inheritdoc />
    public ValueTask<object?> ExecuteAsync(
        IAsyncEnumerable<object?> inputStream,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(inputStream);
        var output = AggregateStreamAsync(inputStream, cancellationToken);

        // IAsyncEnumerable<T> is covariant: a reference-type TResult needs no re-yielding wrapper.
        return ValueTask.FromResult<object?>(output as IAsyncEnumerable<object?> ?? Box(output, cancellationToken));

        static async IAsyncEnumerable<object?> Box(IAsyncEnumerable<TResult> source,
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
                yield return item;
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
    ///     Gets the number of items dropped because their window had already been emitted when they arrived.
    /// </summary>
    public long LateItemsDropped => Interlocked.Read(ref _lateItemsDropped);

    /// <summary>
    ///     Gets metrics about the node's operation.
    /// </summary>
    /// <returns>A tuple containing metrics about windows processed, closed, and maximum concurrency.</returns>
    public (long TotalWindowsProcessed, long TotalWindowsClosed, long MaxConcurrentWindows) GetMetrics() => (Interlocked.Read(ref _totalWindowsProcessed),
        Interlocked.Read(ref _totalWindowsClosed), Interlocked.Read(ref _maxConcurrentWindows));

    /// <summary>
    ///     Gets the current number of active windows being tracked.
    /// </summary>
    /// <returns>The current number of active windows.</returns>
    public int GetActiveWindowCount() => (int)Interlocked.Read(ref _activeGroups);

    private async IAsyncEnumerable<TResult> AggregateStreamAsync(IAsyncEnumerable<object?> input,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // All state is local to one execution, so a node retry or a second run starts clean.
        var windows = new Dictionary<IWindow, Dictionary<TKey, TAccumulate>>();
        var expiry = new PriorityQueue<IWindow, DateTimeOffset>();
        var maxTimestamp = DateTimeOffset.MinValue;
        var watermark = DateTimeOffset.MinValue;
        var lastWatermarkCheck = Stopwatch.GetTimestamp();

        await foreach (var obj in input.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (obj is not TIn item)
                continue;

            var timestamp = TimestampUtils.ResolveEventTime(item, _timestampExtractor);
            if (timestamp > maxTimestamp)
                maxTimestamp = timestamp;

            var key = GetKey(item);

            if (_windowAssigner.TryGetSingleWindow(timestamp, out var singleWindow))
            {
                if (singleWindow.End <= watermark)
                {
                    // Late: the window was already emitted. Reopening it would emit a second, partial result.
                    _ = Interlocked.Increment(ref _lateItemsDropped);
                }
                else
                {
                    AccumulateWindow(singleWindow, key, item);
                }
            }
            else
            {
                foreach (var window in _windowAssigner.AssignWindows(item, timestamp, _timestampExtractor))
                {
                    if (window.End <= watermark)
                    {
                        // Late: the window was already emitted. Reopening it would emit a second, partial result.
                        _ = Interlocked.Increment(ref _lateItemsDropped);
                        continue;
                    }

                    AccumulateWindow(window, key, item);
                }
            }

            if (Stopwatch.GetElapsedTime(lastWatermarkCheck) < _watermarkInterval)
                continue;

            lastWatermarkCheck = Stopwatch.GetTimestamp();
            var candidate = TimestampUtils.SafeSubtract(maxTimestamp, _maxOutOfOrderness);
            if (candidate <= watermark)
                continue;

            watermark = candidate;

            while (expiry.TryPeek(out var closing, out var end) && end <= watermark)
            {
                _ = expiry.Dequeue();
                if (!windows.Remove(closing, out var groups))
                    continue;

                foreach (var accumulator in groups.Values)
                {
                    _ = Interlocked.Increment(ref _totalWindowsClosed);
                    _ = Interlocked.Decrement(ref _activeGroups);
                    yield return GetResult(accumulator!);
                }
            }
        }

        // End of stream: flush the remaining windows in window order. The state is then released.
        while (expiry.TryDequeue(out var closing, out _))
        {
            if (!windows.Remove(closing, out var groups))
                continue;

            foreach (var accumulator in groups.Values)
            {
                _ = Interlocked.Increment(ref _totalWindowsClosed);
                _ = Interlocked.Decrement(ref _activeGroups);
                yield return GetResult(accumulator!);
            }
        }

        void AccumulateWindow(IWindow window, TKey key, TIn item)
        {
            ref var perKey = ref CollectionsMarshal.GetValueRefOrAddDefault(windows, window, out var windowExists);
            if (!windowExists)
            {
                perKey = new Dictionary<TKey, TAccumulate>();
                expiry.Enqueue(window, window.End);
            }

            ref var accumulator = ref CollectionsMarshal.GetValueRefOrAddDefault(perKey!, key, out var keyExists);
            accumulator = Accumulate(keyExists ? accumulator! : CreateAccumulator(), item);

            if (!keyExists)
            {
                _ = Interlocked.Increment(ref _totalWindowsProcessed);
                var active = Interlocked.Increment(ref _activeGroups);
                if (active > Interlocked.Read(ref _maxConcurrentWindows))
                    _ = Interlocked.Exchange(ref _maxConcurrentWindows, active);
            }
        }
    }
}