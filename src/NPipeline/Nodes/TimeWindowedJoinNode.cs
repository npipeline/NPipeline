using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Watermarks;
using NPipeline.DataFlow.Windowing;
using NPipeline.Pipeline;
using NPipeline.Utils;

namespace NPipeline.Nodes;

/// <summary>
///     An abstract base class for creating a node that performs a keyed join on two input streams
///     within specific time windows. This node is stateful and will hold items in memory until
///     a match is found based on both the specified key and time window.
/// </summary>
/// <typeparam name="TKey">The type of the key used for joining. Must be not-null.</typeparam>
/// <typeparam name="TIn1">The type of the data from the first input stream.</typeparam>
/// <typeparam name="TIn2">The type of the data from the second input stream.</typeparam>
/// <typeparam name="TOut">The type of the output data after the join.</typeparam>
public abstract class TimeWindowedJoinNode<TKey, TIn1, TIn2, TOut> : BaseJoinNode<TKey, TIn1, TIn2, TOut> where TKey : notnull
{
    private readonly TimeSpan _maxOutOfOrderness;
    private readonly TimestampExtractor<TIn1>? _timestampExtractor1;
    private readonly TimestampExtractor<TIn2>? _timestampExtractor2;
    private readonly ConcurrentDictionary<(IWindow Window, TKey Key), TIn1> _waitingList1 = new();
    private readonly ConcurrentDictionary<(IWindow Window, TKey Key), TIn2> _waitingList2 = new();
    private readonly TimeSpan _watermarkInterval;
    private readonly WindowAssigner _windowAssigner;
    private long _maxWaitingItems1;
    private long _maxWaitingItems2;

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
        _waitingList1.Clear();
        _waitingList2.Clear();
        _maxWaitingItems1 = 0;
        _maxWaitingItems2 = 0;

        var (getKey1, getKey2) = GetKeySelectors();

        await foreach (var streamItem in watermarkAwareStream)
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
                        var windowKey = (window, key);

                        if (_waitingList2.TryRemove(windowKey, out var item2))
                            yield return CreateOutput(item1, item2);
                        else
                        {
                            _waitingList1.TryAdd(windowKey, item1);
                            var currentCount = _waitingList1.Count;
                            var maxCount = Interlocked.Read(ref _maxWaitingItems1);

                            if (currentCount > maxCount)
                                Interlocked.Exchange(ref _maxWaitingItems1, currentCount);
                        }
                    }
                }
                else if (item is TIn2 item2)
                {
                    var timestamp = TimestampUtils.ExtractTimestamp(item2, _timestampExtractor2);
                    var key = getKey2(item2);
                    var windows = _windowAssigner.AssignWindows(item2, timestamp, _timestampExtractor2);

                    foreach (var window in windows)
                    {
                        var windowKey = (window, key);

                        if (_waitingList1.TryRemove(windowKey, out var matchedItem1))
                            yield return CreateOutput(matchedItem1, item2);
                        else
                        {
                            _waitingList2.TryAdd(windowKey, item2);
                            var currentCount = _waitingList2.Count;
                            var maxCount = Interlocked.Read(ref _maxWaitingItems2);

                            if (currentCount > maxCount)
                                Interlocked.Exchange(ref _maxWaitingItems2, currentCount);
                        }
                    }
                }
            }
            else if (streamItem is StreamItem<object?>.WatermarkItem watermarkItem)
            {
                var watermark = watermarkItem.Watermark;

                // Clean up state for windows that have passed
                CleanupExpiredWindows(_waitingList1, watermark.Timestamp);
                CleanupExpiredWindows(_waitingList2, watermark.Timestamp);
            }
        }

        // Handle unmatched items for outer joins at the end of the streams
        if (JoinType is JoinType.LeftOuter or JoinType.FullOuter)
        {
            foreach (var unmatchedLeft in _waitingList1.Values)
            {
                yield return CreateOutputFromLeft(unmatchedLeft);
            }
        }

        if (JoinType is JoinType.RightOuter or JoinType.FullOuter)
        {
            foreach (var unmatchedRight in _waitingList2.Values)
            {
                yield return CreateOutputFromRight(unmatchedRight);
            }
        }
    }

    private void CleanupExpiredWindows<T>(ConcurrentDictionary<(IWindow Window, TKey Key), T> dictionary, DateTimeOffset watermarkTimestamp)
    {
        var expiredKeys = dictionary.Keys
            .Where(k => k.Window.End <= watermarkTimestamp)
            .ToList();

        foreach (var key in expiredKeys)
        {
            dictionary.TryRemove(key, out _);
        }
    }

    /// <summary>
    ///     Gets metrics about the node's current state.
    /// </summary>
    /// <returns>A tuple containing the number of waiting items in each stream and maximum counts observed.</returns>
    public (int WaitingList1Count, int WaitingList2Count, long MaxWaitingList1, long MaxWaitingList2) GetStateMetrics()
    {
        return (_waitingList1.Count, _waitingList2.Count, Interlocked.Read(ref _maxWaitingItems1), Interlocked.Read(ref _maxWaitingItems2));
    }
}
