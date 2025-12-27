using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Pipeline;

namespace NPipeline.Nodes.Internal;

/// <summary>
///     Internal join node for self-join scenarios that handles the actual join logic with wrapped types.
///     This node receives wrapped items and unwraps them
///     before applying the join logic, solving the "BaseJoinNode Secondary Input Type Erasure" issue.
/// </summary>
/// <typeparam name="TKey">The type of the key used for joining. Must be not-null.</typeparam>
/// <typeparam name="TLeft">The type of the data from the left input stream (wrapper type).</typeparam>
/// <typeparam name="TRight">The type of the data from the right input stream (wrapper type).</typeparam>
/// <typeparam name="TOut">The type of the output data after the join.</typeparam>
internal sealed class SelfJoinNode<TKey, TLeft, TRight, TOut> : KeyedJoinNode<TKey, TLeft, TRight, TOut>
    where TKey : notnull
{
    private static readonly Lazy<MethodInfo> _tryCreateProjectionMethod = new(() =>
        typeof(BaseJoinNode<TKey, TLeft, TRight, TOut>).GetMethod("TryCreateProjection", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("Unable to locate projection builder for self-join fallbacks."));

    private static readonly ConcurrentDictionary<Type, Func<object?, TOut>> _projectionCache = new();

    /// <summary>
    ///     Gets or sets the function to create output from matched items.
    ///     Receives unwrapped items as objects.
    /// </summary>
    public Func<object, object, TOut>? OutputFactory { get; set; }

    /// <summary>
    ///     Gets or sets the key selector for the left stream.
    ///     Receives an unwrapped item as an object and returns the key.
    /// </summary>
    public Func<object, TKey>? LeftKeySelector { get; set; }

    /// <summary>
    ///     Gets or sets the key selector for the right stream.
    ///     Receives an unwrapped item as an object and returns the key.
    /// </summary>
    public Func<object, TKey>? RightKeySelector { get; set; }

    /// <summary>
    ///     Gets or sets the optional fallback for unmatched left items.
    ///     Receives an unwrapped item as an object and returns the output.
    ///     If not set, the base implementation is used.
    /// </summary>
    public Func<object, TOut>? LeftFallback { get; set; }

    /// <summary>
    ///     Gets or sets the optional fallback for unmatched right items.
    ///     Receives an unwrapped item as an object and returns the output.
    ///     If not set, the base implementation is used.
    /// </summary>
    public Func<object, TOut>? RightFallback { get; set; }

    /// <summary>
    ///     Gets or sets the type of join to perform.
    ///     Defaults to <see cref="JoinType.Inner" />.
    /// </summary>
    public new JoinType JoinType { get; set; } = JoinType.Inner;

    /// <summary>
    ///     Creates the output item from the two joined input items.
    ///     Unwraps both items using dynamic to access the .Item property before calling OutputFactory.
    /// </summary>
    /// <param name="leftItem">The left input item (wrapped type).</param>
    /// <param name="rightItem">The right input item (wrapped type).</param>
    /// <returns>The output item created from the joined items.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when OutputFactory is not set.
    /// </exception>
    public override TOut CreateOutput(TLeft leftItem, TRight rightItem)
    {
        if (OutputFactory is null)
        {
            throw new InvalidOperationException(
                $"{nameof(OutputFactory)} must be set before calling {nameof(CreateOutput)}.");
        }

        var leftUnwrapped = UnwrapItem(leftItem, "left");
        var rightUnwrapped = UnwrapItem(rightItem, "right");

        return OutputFactory(leftUnwrapped!, rightUnwrapped!);
    }

    /// <summary>
    ///     Creates an output item from a left item when there's no right match.
    ///     If LeftFallback is set, unwraps the item and calls it; otherwise calls the base implementation.
    ///     Used for left outer and full outer joins.
    /// </summary>
    /// <param name="leftItem">The left input item (wrapped type).</param>
    /// <returns>The output item created from the left item.</returns>
    public override TOut CreateOutputFromLeft(TLeft leftItem)
    {
        var unwrapped = UnwrapItem(leftItem, "left");

        return LeftFallback is not null
            ? LeftFallback(unwrapped!)
            : ProjectUnwrappedItem(unwrapped!, "left");
    }

    /// <summary>
    ///     Creates an output item from a right item when there's no left match.
    ///     If RightFallback is set, unwraps the item and calls it; otherwise calls the base implementation.
    ///     Used for right outer and full outer joins.
    /// </summary>
    /// <param name="rightItem">The right input item (wrapped type).</param>
    /// <returns>The output item created from the right item.</returns>
    public override TOut CreateOutputFromRight(TRight rightItem)
    {
        var unwrapped = UnwrapItem(rightItem, "right");

        return RightFallback is not null
            ? RightFallback(unwrapped!)
            : ProjectUnwrappedItem(unwrapped!, "right");
    }

    /// <summary>
    ///     Executes the join operation using runtime-configured key selectors instead of attributes.
    ///     This custom implementation bypasses the base class's attribute-based key selector mechanism.
    /// </summary>
    /// <param name="inputStream">The combined input stream from both sources.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An async enumerable of joined output items.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when LeftKeySelector or RightKeySelector is not set.
    /// </exception>
    protected override async IAsyncEnumerable<TOut> ExecuteJoinAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (LeftKeySelector is null)
        {
            throw new InvalidOperationException(
                $"{nameof(LeftKeySelector)} must be set before executing join.");
        }

        if (RightKeySelector is null)
        {
            throw new InvalidOperationException(
                $"{nameof(RightKeySelector)} must be set before executing join.");
        }

        var leftBuckets = new Dictionary<TKey, List<StoredItem<TLeft>>>();
        var rightBuckets = new Dictionary<TKey, List<StoredItem<TRight>>>();

        await foreach (var item in inputStream.WithCancellation(cancellationToken))
        {
            if (item is TLeft item1)
            {
                var leftUnwrapped = UnwrapItem(item1, "left");
                var key = LeftKeySelector(leftUnwrapped!);

                var storedLeft = AddToBucket(leftBuckets, key, item1);

                if (rightBuckets.TryGetValue(key, out var matchingRights))
                {
                    foreach (var rightItem in matchingRights)
                    {
                        yield return CreateOutput(item1, rightItem.Value);

                        storedLeft.MarkMatched();
                        rightItem.MarkMatched();
                    }
                }
            }
            else if (item is TRight item2)
            {
                var rightUnwrapped = UnwrapItem(item2, "right");
                var key = RightKeySelector(rightUnwrapped!);

                var storedRight = AddToBucket(rightBuckets, key, item2);

                if (leftBuckets.TryGetValue(key, out var matchingLefts))
                {
                    foreach (var leftItem in matchingLefts)
                    {
                        yield return CreateOutput(leftItem.Value, item2);

                        storedRight.MarkMatched();
                        leftItem.MarkMatched();
                    }
                }
            }
        }

        // Handle unmatched items for outer joins at the end of the streams
        if (JoinType is JoinType.LeftOuter or JoinType.FullOuter)
        {
            foreach (var unmatchedLeft in EnumerateUnmatched(leftBuckets))
            {
                yield return CreateOutputFromLeft(unmatchedLeft);
            }
        }

        if (JoinType is JoinType.RightOuter or JoinType.FullOuter)
        {
            foreach (var unmatchedRight in EnumerateUnmatched(rightBuckets))
            {
                yield return CreateOutputFromRight(unmatchedRight);
            }
        }
    }

    private static object? UnwrapItem<TWrapper>(TWrapper wrapper, string wrapperRole)
    {
        return wrapper is ISelfJoinWrapper joinWrapper
            ? joinWrapper.Item
            : throw new InvalidOperationException(
                $"Self-join {wrapperRole} wrapper of type '{typeof(TWrapper).FullName}' must implement {nameof(ISelfJoinWrapper)}.");
    }

    private static StoredItem<TItem> AddToBucket<TItem>(Dictionary<TKey, List<StoredItem<TItem>>> buckets, TKey key, TItem value)
    {
        if (!buckets.TryGetValue(key, out var list))
        {
            list = [];
            buckets[key] = list;
        }

        var stored = new StoredItem<TItem>(value);
        list.Add(stored);
        return stored;
    }

    private static IEnumerable<TItem> EnumerateUnmatched<TItem>(Dictionary<TKey, List<StoredItem<TItem>>> buckets)
    {
        foreach (var bucket in buckets.Values)
        {
            foreach (var item in bucket)
            {
                if (!item.HasMatched)
                    yield return item.Value;
            }
        }
    }

    private static TOut ProjectUnwrappedItem(object value, string wrapperRole)
    {
        var projector = GetProjection(value.GetType(), wrapperRole);
        return projector(value);
    }

    private static Func<object?, TOut> GetProjection(Type sourceType, string wrapperRole)
    {
        try
        {
            return _projectionCache.GetOrAdd(sourceType, CreateProjection);
        }
        catch (NotSupportedException)
        {
            throw;
        }
        catch (TargetInvocationException ex) when (ex.InnerException is NotSupportedException notSupported)
        {
            throw new NotSupportedException(notSupported.Message, notSupported);
        }
        catch (Exception ex)
        {
            throw new NotSupportedException(
                $"Unable to infer how to project {sourceType.Name} into {typeof(TOut).Name} for {wrapperRole}-only outputs.", ex);
        }
    }

    private static Func<object?, TOut> CreateProjection(Type sourceType)
    {
        var method = _tryCreateProjectionMethod.Value.MakeGenericMethod(sourceType);

        var result = method.Invoke(null, null)
                     ?? throw new InvalidOperationException("Projection builder returned null result.");

        var tupleType = result.GetType();

        var projection = (Delegate?)(tupleType.GetProperty("Item1")?.GetValue(result)
                                     ?? tupleType.GetField("Item1")?.GetValue(result));

        var failureMessage = (string?)(tupleType.GetProperty("Item2")?.GetValue(result)
                                       ?? tupleType.GetField("Item2")?.GetValue(result));

        if (projection is null)
        {
            throw new NotSupportedException(failureMessage ??
                                            $"Unable to infer how to project {sourceType.Name} into {typeof(TOut).Name}.");
        }

        var sourceParameter = Expression.Parameter(typeof(object), "source");
        var invoke = Expression.Invoke(Expression.Constant(projection), Expression.Convert(sourceParameter, sourceType));
        var convertedResult = Expression.Convert(invoke, typeof(TOut));

        return Expression.Lambda<Func<object?, TOut>>(convertedResult, sourceParameter).Compile();
    }

    private sealed class StoredItem<TItem>(TItem value)
    {
        public TItem Value { get; } = value;

        public bool HasMatched { get; private set; }

        public void MarkMatched()
        {
            HasMatched = true;
        }
    }
}
