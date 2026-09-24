using System.Runtime.CompilerServices;
using NPipeline.Nodes.Internal;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     An abstract base class for creating a node that performs a keyed join on two input streams.
///     Every item is paired with every item on the other input that shares its key, so one-to-many and many-to-many
///     relationships produce one output per matching pair.
/// </summary>
/// <remarks>
///     This node is stateful: because a matching item can arrive on the other input at any time, items from both inputs are held
///     in memory until the input completes. To bound memory, configure <see cref="MaxCapacity" /> to limit the number of items
///     retained per input.
/// </remarks>
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

    /// <summary>
    ///     Gets or sets the maximum number of items retained for each input.
    ///     <c>null</c> indicates unlimited capacity (default). Set to a positive value to prevent unbounded memory growth.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Once an input reaches capacity, its new items are still joined with the items already retained from the other
    ///         input, but are not retained themselves, so they cannot match items that arrive later. If such an item matches
    ///         nothing and its side is preserved by the join type (for example, a left item in a left outer join), it is emitted
    ///         immediately as an unmatched item. Otherwise it is discarded.
    ///     </para>
    ///     <para>
    ///         Setting this to a reasonable value (e.g., 10000) can help prevent memory exhaustion when streams are large or
    ///         unbalanced, at the cost of missing matches once the limit is reached.
    ///     </para>
    /// </remarks>
    public int? MaxCapacity { get; set; }

    /// <inheritdoc />
    /// <remarks>
    ///     Items from both inputs are retained for the lifetime of the stream so that each item is paired with every item on the
    ///     other side that shares its key, including items that arrive later. When the input completes, outer joins emit the
    ///     retained items that never matched.
    /// </remarks>
    protected override async IAsyncEnumerable<TOut> ExecuteJoinAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var left = new JoinSide<TKey, TIn1>();
        var right = new JoinSide<TKey, TIn2>();
        var emitUnmatchedLeft = JoinType is JoinType.LeftOuter or JoinType.FullOuter;
        var emitUnmatchedRight = JoinType is JoinType.RightOuter or JoinType.FullOuter;

        var (getKey1, getKey2) = ResolveKeySelectors();

        await foreach (var item in inputStream.WithCancellation(cancellationToken))
        {
            if (item is TIn1 item1)
            {
                var key = getKey1(item1);
                var matched = false;

                if (right.TryMatch(key, out var matches))
                {
                    matched = true;

                    for (var i = 0; i < matches.Count; i++)
                    {
                        yield return CreateOutput(item1, matches[i]);
                    }
                }

                if (CanRetain(left))
                    left.Add(key, item1, matched);
                else if (!matched && emitUnmatchedLeft)
                    yield return CreateOutputFromLeft(item1);
            }
            else if (item is TIn2 item2)
            {
                var key = getKey2(item2);
                var matched = false;

                if (left.TryMatch(key, out var matches))
                {
                    matched = true;

                    for (var i = 0; i < matches.Count; i++)
                    {
                        yield return CreateOutput(matches[i], item2);
                    }
                }

                if (CanRetain(right))
                    right.Add(key, item2, matched);
                else if (!matched && emitUnmatchedRight)
                    yield return CreateOutputFromRight(item2);
            }
        }

        // Handle unmatched items for outer joins at the end of the streams
        if (emitUnmatchedLeft)
        {
            foreach (var unmatchedLeft in left.Unmatched())
            {
                yield return CreateOutputFromLeft(unmatchedLeft);
            }
        }

        if (emitUnmatchedRight)
        {
            foreach (var unmatchedRight in right.Unmatched())
            {
                yield return CreateOutputFromRight(unmatchedRight);
            }
        }
    }

    /// <summary>
    ///     Resolves the key selectors used to extract join keys from each input.
    /// </summary>
    private protected virtual (Func<TIn1, TKey> GetKey1, Func<TIn2, TKey> GetKey2) ResolveKeySelectors() => GetKeySelectors();

    /// <summary>
    ///     Determines whether another item can be retained on the specified side based on capacity constraints.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanRetain<T>(JoinSide<TKey, T> side)
    {
        if (MaxCapacity is null)
            return true;

        return side.Count < MaxCapacity;
    }
}
