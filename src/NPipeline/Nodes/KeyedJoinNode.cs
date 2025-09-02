using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     An abstract base class for creating a node that performs a keyed join on two input streams.
///     This node is stateful and will hold items in memory until a match is found based on the specified key.
/// </summary>
/// <typeparam name="TKey">The type of the key used for joining. Must be not-null.</typeparam>
/// <typeparam name="TIn1">The type of the data from the first input stream.</typeparam>
/// <typeparam name="TIn2">The type of the data from the second input stream.</typeparam>
/// <typeparam name="TOut">The type of the output data after the join.</typeparam>
public abstract class KeyedJoinNode<TKey, TIn1, TIn2, TOut> : BaseJoinNode<TKey, TIn1, TIn2, TOut> where TKey : notnull
{
    /// <summary>
    ///     Gets or sets the type of join to perform. Defaults to <see cref="JoinType.Inner" />.
    /// </summary>
    public JoinType JoinType { get; set; } = JoinType.Inner;

    /// <inheritdoc />
    protected override async IAsyncEnumerable<TOut> ExecuteJoinAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var waitingList1 = new ConcurrentDictionary<TKey, TIn1>();
        var waitingList2 = new ConcurrentDictionary<TKey, TIn2>();

        var (getKey1, getKey2) = GetKeySelectors();

        await foreach (var item in inputStream.WithCancellation(cancellationToken))
        {
            if (item is TIn1 item1)
            {
                var key = getKey1(item1);

                if (waitingList2.TryRemove(key, out var item2))
                    yield return CreateOutput(item1, item2);
                else
                    waitingList1.TryAdd(key, item1);
            }
            else if (item is TIn2 item2)
            {
                var key = getKey2(item2);

                if (waitingList1.TryRemove(key, out var matchedItem1))
                    yield return CreateOutput(matchedItem1, item2);
                else
                    waitingList2.TryAdd(key, item2);
            }
        }

        // Handle unmatched items for outer joins at the end of the streams
        if (JoinType is JoinType.LeftOuter or JoinType.FullOuter)
        {
            foreach (var unmatchedLeft in waitingList1.Values)
            {
                yield return CreateOutputFromLeft(unmatchedLeft);
            }
        }

        if (JoinType is JoinType.RightOuter or JoinType.FullOuter)
        {
            foreach (var unmatchedRight in waitingList2.Values)
            {
                yield return CreateOutputFromRight(unmatchedRight);
            }
        }
    }
}
