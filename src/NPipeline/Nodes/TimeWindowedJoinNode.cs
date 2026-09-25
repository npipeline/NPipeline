using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes.Internal;
using NPipeline.Pipeline;
using NPipeline.Utils;

namespace NPipeline.Nodes;

/// <summary>
///     An abstract base class for creating a node that performs a keyed join on two input streams
///     within specific time windows. Within each window, every item is paired with every item on the other input
///     that shares its key.
/// </summary>
/// <remarks>
///     <para>
///         Windows are assigned in event time and closed by a watermark computed from the same timestamps, one per
///         input: the join's watermark advances only when both inputs have produced an item, and follows the slower
///         input. An input that never produces holds all state until the end of the stream.
///     </para>
///     <para>
///         This node is stateful: items are held in memory until the watermark passes the end of their window. When a
///         window expires, outer joins emit the items in that window that never matched. Items that arrive after
///         their window has been emitted are dropped and counted in <see cref="LateItemsDropped" />, except that an
///         outer join emits them at once as unmatched when their side is preserved.
///     </para>
///     <para>
///         Windows are evaluated independently: an item that lives in several sliding windows participates in each of
///         them, so a pair is emitted once per shared window and an item can be emitted as unmatched from one window
///         even though it matched in another (per-window semantics, as in Flink).
///     </para>
/// </remarks>
/// <typeparam name="TKey">The type of the key used for joining. Must be not-null.</typeparam>
/// <typeparam name="TIn1">The type of the data from the first input stream.</typeparam>
/// <typeparam name="TIn2">The type of the data from the second input stream.</typeparam>
/// <typeparam name="TOut">The type of the output data after the join.</typeparam>
public abstract class TimeWindowedJoinNode<TKey, TIn1, TIn2, TOut> : BaseJoinNode<TKey, TIn1, TIn2, TOut> where TKey : notnull
{
    private readonly TimeSpan _maxOutOfOrderness;
    private readonly TimestampExtractor<TIn1>? _timestampExtractor1;
    private readonly TimestampExtractor<TIn2>? _timestampExtractor2;
    private readonly TimeSpan _watermarkInterval;
    private readonly WindowAssigner _windowAssigner;
    private long _lateItemsDropped;
    private long _maxWaitingItems1;
    private long _maxWaitingItems2;
    private int _waitingItems1;
    private int _waitingItems2;

    /// <summary>
    ///     Initializes a new instance of <see cref="TimeWindowedJoinNode{TKey, TIn1, TIn2, TOut}" /> class.
    /// </summary>
    /// <param name="windowAssigner">The window assigner strategy to use.</param>
    /// <param name="timestampExtractor1">Optional timestamp extractor for first input type.</param>
    /// <param name="timestampExtractor2">Optional timestamp extractor for second input type.</param>
    /// <param name="maxOutOfOrderness">
    ///     The maximum allowed lateness for out-of-order events. Events arriving later than this relative to current watermark may be
    ///     treated as late.
    /// </param>
    /// <param name="watermarkInterval">The frequency at which watermarks are emitted to advance event time and trigger window cleanup.</param>
    protected TimeWindowedJoinNode(
        WindowAssigner windowAssigner,
        TimestampExtractor<TIn1>? timestampExtractor1 = null,
        TimestampExtractor<TIn2>? timestampExtractor2 = null,
        TimeSpan? maxOutOfOrderness = null,
        TimeSpan? watermarkInterval = null)
    {
        ArgumentNullException.ThrowIfNull(windowAssigner);
        _windowAssigner = windowAssigner;
        _timestampExtractor1 = timestampExtractor1;
        _timestampExtractor2 = timestampExtractor2;
        _maxOutOfOrderness = maxOutOfOrderness ?? TimeSpan.FromMinutes(5);
        _watermarkInterval = watermarkInterval ?? TimeSpan.FromSeconds(30);
        JoinType = JoinType.Inner; // Time-windowed joins typically use inner join semantics
    }

    /// <summary>
    ///     Gets or sets the type of join to perform. Defaults to <see cref="JoinType.Inner" />.
    /// </summary>
    public JoinType JoinType { get; set; }

    /// <summary>
    ///     Gets the number of items dropped because their window had already been emitted when they arrived.
    /// </summary>
    public long LateItemsDropped => Interlocked.Read(ref _lateItemsDropped);

    /// <inheritdoc />
    protected override async IAsyncEnumerable<TOut> ExecuteJoinAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // All state is local to one execution, so a node retry or a second run starts clean.
        var windows = new Dictionary<IWindow, WindowState>();
        var expiry = new PriorityQueue<IWindow, DateTimeOffset>();

        // Per-input watermark state: the join's watermark follows the slower input, and stays at MinValue
        // until both inputs have produced an item.
        var maxTs1 = DateTimeOffset.MinValue;
        var maxTs2 = DateTimeOffset.MinValue;
        var sawLeft = false;
        var sawRight = false;
        var watermark = DateTimeOffset.MinValue;
        var lastWatermarkCheck = Stopwatch.GetTimestamp();
        Volatile.Write(ref _waitingItems1, 0);
        Volatile.Write(ref _waitingItems2, 0);

        var emitUnmatchedLeft = JoinType is JoinType.LeftOuter or JoinType.FullOuter;
        var emitUnmatchedRight = JoinType is JoinType.RightOuter or JoinType.FullOuter;

        var (getKey1, getKey2) = GetKeySelectors();

        await foreach (var item in inputStream.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (item is TIn1 item1)
            {
                var timestamp1 = TimestampUtils.ResolveEventTime(item1, _timestampExtractor1);
                sawLeft = true;
                if (timestamp1 > maxTs1)
                    maxTs1 = timestamp1;

                var key1 = getKey1(item1);

                // Null keys never match (C31). An outer join emits the row at once when its side is preserved.
                if (key1 is null)
                {
                    if (emitUnmatchedLeft)
                        yield return CreateOutputFromLeft(item1);
                }
                else
                {
                    var assigned1 = _windowAssigner.AssignWindows(item1, timestamp1, _timestampExtractor1);

                    var landedInLiveWindow = false;

                    foreach (var window in assigned1)
                    {
                        // Late: the window was already emitted. Pairing with a recreated window would emit a
                        // second partial result, so the item is dropped (or emitted at once when preserved).
                        if (window.End <= watermark)
                        {
                            _ = Interlocked.Increment(ref _lateItemsDropped);
                            continue;
                        }

                        landedInLiveWindow = true;

                        var state = GetOrAddWindow(windows, expiry, window);
                        var matched = false;

                        if (state.Right.TryMatch(key1, out var matches))
                        {
                            matched = true;

                            for (var i = 0; i < matches.Count; i++)
                            {
                                yield return CreateOutput(item1, matches[i]);
                            }
                        }

                        state.Left.Add(key1, item1, matched);
                        TrackMax(ref _maxWaitingItems1, Interlocked.Increment(ref _waitingItems1));
                    }

                    // An item that lands in no live window is emitted at once as unmatched when its side is
                    // preserved by the join type, and dropped otherwise.
                    if (!landedInLiveWindow && emitUnmatchedLeft)
                        yield return CreateOutputFromLeft(item1);
                }
            }
            else if (item is TIn2 item2)
            {
                var timestamp2 = TimestampUtils.ResolveEventTime(item2, _timestampExtractor2);
                sawRight = true;
                if (timestamp2 > maxTs2)
                    maxTs2 = timestamp2;

                var key2 = getKey2(item2);

                if (key2 is null)
                {
                    if (emitUnmatchedRight)
                        yield return CreateOutputFromRight(item2);
                }
                else
                {
                    var assigned2 = _windowAssigner.AssignWindows(item2, timestamp2, _timestampExtractor2);

                    var landedInLiveWindow = false;

                    foreach (var window in assigned2)
                    {
                        if (window.End <= watermark)
                        {
                            _ = Interlocked.Increment(ref _lateItemsDropped);
                            continue;
                        }

                        landedInLiveWindow = true;

                        var state = GetOrAddWindow(windows, expiry, window);
                        var matched = false;

                        if (state.Left.TryMatch(key2, out var matches))
                        {
                            matched = true;

                            for (var i = 0; i < matches.Count; i++)
                            {
                                yield return CreateOutput(matches[i], item2);
                            }
                        }

                        state.Right.Add(key2, item2, matched);
                        TrackMax(ref _maxWaitingItems2, Interlocked.Increment(ref _waitingItems2));
                    }

                    if (!landedInLiveWindow && emitUnmatchedRight)
                        yield return CreateOutputFromRight(item2);
                }
            }
            else
            {
                continue;
            }

            if (Stopwatch.GetElapsedTime(lastWatermarkCheck) < _watermarkInterval)
                continue;

            lastWatermarkCheck = Stopwatch.GetTimestamp();
            var candidate = sawLeft && sawRight
                ? TimestampUtils.SafeSubtract(maxTs1 <= maxTs2 ? maxTs1 : maxTs2, _maxOutOfOrderness)
                : DateTimeOffset.MinValue;
            if (candidate <= watermark)
                continue;

            watermark = candidate;

            // Release state for windows that have passed, emitting their unmatched items for outer joins
            while (expiry.TryPeek(out var window, out var windowEnd) && windowEnd <= watermark)
            {
                _ = expiry.Dequeue();
                if (!windows.Remove(window, out var state))
                    continue;

                Interlocked.Add(ref _waitingItems1, -state.Left.Count);
                Interlocked.Add(ref _waitingItems2, -state.Right.Count);

                if (emitUnmatchedLeft)
                {
                    foreach (var unmatchedLeft in state.Left.Unmatched())
                    {
                        yield return CreateOutputFromLeft(unmatchedLeft);
                    }
                }

                if (emitUnmatchedRight)
                {
                    foreach (var unmatchedRight in state.Right.Unmatched())
                    {
                        yield return CreateOutputFromRight(unmatchedRight);
                    }
                }
            }
        }

        // Handle unmatched items for outer joins at the end of the streams. The state is then released.
        if (emitUnmatchedLeft)
        {
            foreach (var state in windows.Values)
            {
                foreach (var unmatchedLeft in state.Left.Unmatched())
                {
                    yield return CreateOutputFromLeft(unmatchedLeft);
                }
            }
        }

        if (emitUnmatchedRight)
        {
            foreach (var state in windows.Values)
            {
                foreach (var unmatchedRight in state.Right.Unmatched())
                {
                    yield return CreateOutputFromRight(unmatchedRight);
                }
            }
        }

        windows.Clear();
    }

    private static WindowState GetOrAddWindow(Dictionary<IWindow, WindowState> windows,
        PriorityQueue<IWindow, DateTimeOffset> expiry, IWindow window)
    {
        ref var state = ref CollectionsMarshal.GetValueRefOrAddDefault(windows, window, out var exists);

        if (!exists)
        {
            state = new WindowState();
            expiry.Enqueue(window, window.End);
        }

        return state!;
    }

    private static void TrackMax(ref long max, int currentCount)
    {
        if (currentCount > Interlocked.Read(ref max))
            Interlocked.Exchange(ref max, currentCount);
    }

    /// <summary>
    ///     Gets metrics about the node's current state.
    /// </summary>
    /// <returns>A tuple containing the number of waiting items in each stream and maximum counts observed.</returns>
    public (int WaitingList1Count, int WaitingList2Count, long MaxWaitingList1, long MaxWaitingList2) GetStateMetrics() => (Volatile.Read(ref _waitingItems1),
        Volatile.Read(ref _waitingItems2), Interlocked.Read(ref _maxWaitingItems1), Interlocked.Read(ref _maxWaitingItems2));

    /// <summary>
    ///     The retained items for one window, split by input.
    /// </summary>
    private sealed class WindowState
    {
        public JoinSide<TKey, TIn1> Left { get; } = new();

        public JoinSide<TKey, TIn2> Right { get; } = new();
    }
}