using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Watermarks;
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
///     This node is stateful: items are held in memory until the watermark passes the end of their window. When a window
///     expires, outer joins emit the items in that window that never matched.
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
    private readonly PriorityQueue<IWindow, DateTimeOffset> _windowExpiry = new();
    private readonly Dictionary<IWindow, WindowState> _windows = [];
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

    /// <inheritdoc />
    protected override async IAsyncEnumerable<TOut> ExecuteJoinAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Convert to watermark-aware stream
        var watermarkAwareStream = inputStream.WithWatermarks(
            new BoundedOutOfOrdernessWatermarkGenerator<object?>(_maxOutOfOrderness),
            _watermarkInterval,
            cancellationToken);

        // Clear previous state for new execution
        _windows.Clear();
        _windowExpiry.Clear();
        _waitingItems1 = 0;
        _waitingItems2 = 0;
        _maxWaitingItems1 = 0;
        _maxWaitingItems2 = 0;

        var emitUnmatchedLeft = JoinType is JoinType.LeftOuter or JoinType.FullOuter;
        var emitUnmatchedRight = JoinType is JoinType.RightOuter or JoinType.FullOuter;

        var (getKey1, getKey2) = GetKeySelectors();

        await foreach (var streamItem in watermarkAwareStream.ConfigureAwait(false))
        {
            if (streamItem is StreamItem<object?>.DataItem dataItem)
            {
                var item = dataItem.Value;

                if (item is TIn1 item1)
                {
                    var timestamp = TimestampUtils.ExtractTimestamp(item1, _timestampExtractor1);
                    var key = getKey1(item1);
                    var windows = _windowAssigner.AssignWindows(item1, timestamp, _timestampExtractor1);

                    foreach (var window in windows)
                    {
                        var state = GetOrAddWindow(window);
                        var matched = false;

                        if (state.Right.TryMatch(key, out var matches))
                        {
                            matched = true;

                            for (var i = 0; i < matches.Count; i++)
                            {
                                yield return CreateOutput(item1, matches[i]);
                            }
                        }

                        state.Left.Add(key, item1, matched);
                        TrackMax(ref _maxWaitingItems1, ++_waitingItems1);
                    }
                }
                else if (item is TIn2 item2)
                {
                    var timestamp = TimestampUtils.ExtractTimestamp(item2, _timestampExtractor2);
                    var key = getKey2(item2);
                    var windows = _windowAssigner.AssignWindows(item2, timestamp, _timestampExtractor2);

                    foreach (var window in windows)
                    {
                        var state = GetOrAddWindow(window);
                        var matched = false;

                        if (state.Left.TryMatch(key, out var matches))
                        {
                            matched = true;

                            for (var i = 0; i < matches.Count; i++)
                            {
                                yield return CreateOutput(matches[i], item2);
                            }
                        }

                        state.Right.Add(key, item2, matched);
                        TrackMax(ref _maxWaitingItems2, ++_waitingItems2);
                    }
                }
            }
            else if (streamItem is StreamItem<object?>.WatermarkItem watermarkItem)
            {
                var watermarkTimestamp = watermarkItem.Watermark.Timestamp;

                // Release state for windows that have passed, emitting their unmatched items for outer joins
                while (_windowExpiry.TryPeek(out var window, out var windowEnd) && windowEnd <= watermarkTimestamp)
                {
                    _windowExpiry.Dequeue();
                    _windows.Remove(window, out var state);
                    _waitingItems1 -= state!.Left.Count;
                    _waitingItems2 -= state.Right.Count;

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
        }

        // Handle unmatched items for outer joins at the end of the streams
        if (emitUnmatchedLeft)
        {
            foreach (var state in _windows.Values)
            {
                foreach (var unmatchedLeft in state.Left.Unmatched())
                {
                    yield return CreateOutputFromLeft(unmatchedLeft);
                }
            }
        }

        if (emitUnmatchedRight)
        {
            foreach (var state in _windows.Values)
            {
                foreach (var unmatchedRight in state.Right.Unmatched())
                {
                    yield return CreateOutputFromRight(unmatchedRight);
                }
            }
        }
    }

    private WindowState GetOrAddWindow(IWindow window)
    {
        ref var state = ref CollectionsMarshal.GetValueRefOrAddDefault(_windows, window, out var exists);

        if (!exists)
        {
            state = new WindowState();
            _windowExpiry.Enqueue(window, window.End);
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
