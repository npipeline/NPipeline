using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace NPipeline.Nodes.Internal;

/// <summary>
///     Internal join node for self-join scenarios that handles the actual join logic with wrapped types.
///     This node receives wrapped items and unwraps them
///     before applying the join logic, solving the "BaseJoinNode Secondary Input Type Erasure" issue.
/// </summary>
/// <typeparam name="TKey">The type of the key used for joining. Must be not-null.</typeparam>
/// <typeparam name="TItem">The type of the wrapped item in both input streams.</typeparam>
/// <typeparam name="TOut">The type of the output data after the join.</typeparam>
internal sealed class SelfJoinNode<TKey, TItem, TOut> : KeyedJoinNode<TKey, LeftWrapper<TItem>, RightWrapper<TItem>, TOut>
    where TKey : notnull
{
    private static readonly Lazy<MethodInfo> _tryCreateProjectionMethod = new(() =>
        typeof(BaseJoinNode<TKey, LeftWrapper<TItem>, RightWrapper<TItem>, TOut>).GetMethod("TryCreateProjection",
            BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("Unable to locate projection builder for self-join fallbacks."));

    private static readonly ConcurrentDictionary<Type, Func<object?, TOut>> _projectionCache = new();

    /// <summary>
    ///     Gets or sets the function to create output from matched items.
    ///     Receives the unwrapped items.
    /// </summary>
    public Func<TItem, TItem, TOut>? OutputFactory { get; set; }

    /// <summary>
    ///     Gets or sets the key selector for the left stream.
    ///     Receives an unwrapped item and returns the key.
    /// </summary>
    public Func<TItem, TKey>? LeftKeySelector { get; set; }

    /// <summary>
    ///     Gets or sets the key selector for the right stream.
    ///     Receives an unwrapped item and returns the key.
    /// </summary>
    public Func<TItem, TKey>? RightKeySelector { get; set; }

    /// <summary>
    ///     Gets or sets the optional fallback for unmatched left items.
    ///     Receives an unwrapped item and returns the output.
    ///     If not set, the base implementation is used.
    /// </summary>
    public Func<TItem, TOut>? LeftFallback { get; set; }

    /// <summary>
    ///     Gets or sets the optional fallback for unmatched right items.
    ///     Receives an unwrapped item and returns the output.
    ///     If not set, the base implementation is used.
    /// </summary>
    public Func<TItem, TOut>? RightFallback { get; set; }

    /// <summary>
    ///     Creates the output item from the two joined input items.
    ///     Unwraps both items and calls the output factory.
    /// </summary>
    /// <param name="leftItem">The left input item (wrapped type).</param>
    /// <param name="rightItem">The right input item (wrapped type).</param>
    /// <returns>The output item created from the joined items.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when OutputFactory is not set.
    /// </exception>
    public override TOut CreateOutput(LeftWrapper<TItem> leftItem, RightWrapper<TItem> rightItem)
    {
        if (OutputFactory is null)
        {
            throw new InvalidOperationException(
                $"{nameof(OutputFactory)} must be set before calling {nameof(CreateOutput)}.");
        }

        return OutputFactory(leftItem.Item, rightItem.Item);
    }

    /// <summary>
    ///     Creates an output item from a left item when there's no right match.
    ///     If LeftFallback is set, unwraps the item and calls it; otherwise calls the base implementation.
    ///     Used for left outer and full outer joins.
    /// </summary>
    /// <param name="leftItem">The left input item (wrapped type).</param>
    /// <returns>The output item created from the left item.</returns>
    public override TOut CreateOutputFromLeft(LeftWrapper<TItem> leftItem)
    {
        return LeftFallback is not null
            ? LeftFallback(leftItem.Item)
            : ProjectUnwrappedItem(leftItem.Item!, "left");
    }

    /// <summary>
    ///     Creates an output item from a right item when there's no left match.
    ///     If RightFallback is set, unwraps the item and calls it; otherwise calls the base implementation.
    ///     Used for right outer and full outer joins.
    /// </summary>
    /// <param name="rightItem">The right input item (wrapped type).</param>
    /// <returns>The output item created from the right item.</returns>
    public override TOut CreateOutputFromRight(RightWrapper<TItem> rightItem)
    {
        return RightFallback is not null
            ? RightFallback(rightItem.Item)
            : ProjectUnwrappedItem(rightItem.Item!, "right");
    }

    /// <summary>
    ///     Resolves key selectors from the runtime-configured <see cref="LeftKeySelector" /> and <see cref="RightKeySelector" />
    ///     instead of the base class's attribute-based mechanism.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when LeftKeySelector or RightKeySelector is not set.
    /// </exception>
    private protected override (Func<LeftWrapper<TItem>, TKey> GetKey1, Func<RightWrapper<TItem>, TKey> GetKey2) ResolveKeySelectors()
    {
        var leftKeySelector = LeftKeySelector ?? throw new InvalidOperationException(
            $"{nameof(LeftKeySelector)} must be set before executing join.");

        var rightKeySelector = RightKeySelector ?? throw new InvalidOperationException(
            $"{nameof(RightKeySelector)} must be set before executing join.");

        // The typed Item property is read directly, so a value-type item is never boxed on the key path.
        return (
            left => leftKeySelector(left.Item),
            right => rightKeySelector(right.Item));
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
}
