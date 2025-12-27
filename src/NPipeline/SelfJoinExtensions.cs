using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Nodes.Internal;
using NPipeline.Pipeline;

namespace NPipeline;

/// <summary>
///     Provides extension methods for adding self-join nodes to a pipeline.
/// </summary>
public static class SelfJoinExtensions
{
    /// <summary>
    ///     Adds a self-join node to the pipeline, allowing you to join two streams of the same item type from different sources.
    ///     This is useful when you need to join items of the same type but from different data sources, such as joining orders from
    ///     different regions or events from different time periods.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A self-join is a join operation where both input streams contain items of the same type. This method solves the
    ///         "BaseJoinNode Secondary Input Type Erasure" issue by internally wrapping items from each stream with distinct
    ///         wrapper types (LeftWrapper&lt;T&gt; and RightWrapper&lt;T&gt;), allowing the join node to distinguish between
    ///         items from the left and right streams even though they have the same underlying type.
    ///     </para>
    ///     <para>
    ///         The method automatically creates wrapper transform nodes for both streams, a join node, and connects them together.
    ///         The join node unwraps items before applying the join logic, so your output factory receives the original unwrapped items.
    ///     </para>
    ///     <para>
    ///         <b>Limitation:</b> This method is specifically designed for joining same-type items from different sources.
    ///         If you need to join different types, use the standard <see cref="KeyedJoinNode{TKey, TLeft, TRight, TOut}" /> instead.
    ///     </para>
    /// </remarks>
    /// <typeparam name="TItem">The type of items in both input streams.</typeparam>
    /// <typeparam name="TKey">The type of the key used for joining. Must be not-null.</typeparam>
    /// <typeparam name="TOut">The type of the output items after the join.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="leftSource">The handle to the left source node.</param>
    /// <param name="rightSource">The handle to the right source node.</param>
    /// <param name="nodeName">The name for the join node.</param>
    /// <param name="outputFactory">
    ///     A function that creates an output item from matched left and right items.
    ///     Receives the original unwrapped items from both streams.
    /// </param>
    /// <param name="leftKeySelector">A function to extract the join key from left stream items.</param>
    /// <param name="rightKeySelector">
    ///     Optional function to extract the join key from right stream items.
    ///     If null, uses the same selector as the left stream.
    /// </param>
    /// <param name="joinType">The type of join to perform. Defaults to <see cref="JoinType.Inner" />.</param>
    /// <param name="leftFallback">
    ///     Optional function to create output from unmatched left items.
    ///     Used for left outer and full outer joins. If null, uses default behavior.
    /// </param>
    /// <param name="rightFallback">
    ///     Optional function to create output from unmatched right items.
    ///     Used for right outer and full outer joins. If null, uses default behavior.
    /// </param>
    /// <returns>A handle to the newly added join node.</returns>
    /// <exception cref="ArgumentNullException">
    ///     Thrown when <paramref name="builder" />, <paramref name="leftSource" />, <paramref name="rightSource" />,
    ///     <paramref name="nodeName" />, <paramref name="outputFactory" />, or <paramref name="leftKeySelector" /> is null.
    /// </exception>
    /// <example>
    ///     <code>
    /// // Create a pipeline with two sources of the same type
    /// var builder = new PipelineBuilder();
    /// 
    /// var orders2024 = builder.AddSource(() => GetOrders(2024), "orders_2024");
    /// var orders2023 = builder.AddSource(() => GetOrders(2023), "orders_2023");
    /// 
    /// // Join orders from both years by customer ID
    /// var joinedOrders = builder.AddSelfJoin(
    ///     leftSource: orders2024,
    ///     rightSource: orders2023,
    ///     nodeName: "customer_orders_join",
    ///     outputFactory: (order2024, order2023) => new
    ///     {
    ///         CustomerId = order2024.CustomerId,
    ///         Total2024 = order2024.Total,
    ///         Total2023 = order2023.Total,
    ///         Growth = order2024.Total - order2023.Total
    ///     },
    ///     leftKeySelector: order => order.CustomerId,
    ///     rightKeySelector: order => order.CustomerId,
    ///     joinType: JoinType.Inner);
    /// 
    /// var sink = builder.AddSink(result => Console.WriteLine(result), "output");
    /// builder.Connect(joinedOrders, sink);
    /// </code>
    /// </example>
    public static IOutputNodeHandle<TOut> AddSelfJoin<TItem, TKey, TOut>(
        this PipelineBuilder builder,
        IOutputNodeHandle<TItem> leftSource,
        IOutputNodeHandle<TItem> rightSource,
        string nodeName,
        Func<TItem, TItem, TOut> outputFactory,
        Func<TItem, TKey> leftKeySelector,
        Func<TItem, TKey>? rightKeySelector = null,
        JoinType joinType = JoinType.Inner,
        Func<TItem, TOut>? leftFallback = null,
        Func<TItem, TOut>? rightFallback = null)
        where TKey : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(leftSource);
        ArgumentNullException.ThrowIfNull(rightSource);
        ArgumentNullException.ThrowIfNull(nodeName);
        ArgumentNullException.ThrowIfNull(outputFactory);
        ArgumentNullException.ThrowIfNull(leftKeySelector);

        // Call the internal helper with the generic types properly resolved
        return AddSelfJoinInternal(
            builder,
            leftSource,
            rightSource,
            nodeName,
            outputFactory,
            leftKeySelector,
            rightKeySelector,
            joinType,
            leftFallback,
            rightFallback);
    }

    /// <summary>
    ///     Internal helper method that handles the self-join logic using compile-time type resolution.
    /// </summary>
    private static IOutputNodeHandle<TOut> AddSelfJoinInternal<TItem, TKey, TOut>(
        PipelineBuilder builder,
        IOutputNodeHandle<TItem> leftSource,
        IOutputNodeHandle<TItem> rightSource,
        string nodeName,
        Func<TItem, TItem, TOut> outputFactory,
        Func<TItem, TKey> leftKeySelector,
        Func<TItem, TKey>? rightKeySelector,
        JoinType joinType,
        Func<TItem, TOut>? leftFallback,
        Func<TItem, TOut>? rightFallback)
        where TKey : notnull
    {
        // Create and register wrapper transform instances explicitly
        var leftWrapTransform = new SelfJoinTransform<TItem, LeftWrapper<TItem>>();
        var leftWrapHandle = builder.AddTransform<SelfJoinTransform<TItem, LeftWrapper<TItem>>, TItem, LeftWrapper<TItem>>($"{nodeName}_leftWrap");
        _ = builder.AddPreconfiguredNodeInstance(leftWrapHandle.Id, leftWrapTransform);

        var rightWrapTransform = new SelfJoinTransform<TItem, RightWrapper<TItem>>();
        var rightWrapHandle = builder.AddTransform<SelfJoinTransform<TItem, RightWrapper<TItem>>, TItem, RightWrapper<TItem>>($"{nodeName}_rightWrap");
        _ = builder.AddPreconfiguredNodeInstance(rightWrapHandle.Id, rightWrapTransform);

        // Create and configure the join node instance
        var joinNode = new SelfJoinNode<TKey, LeftWrapper<TItem>, RightWrapper<TItem>, TOut>
        {
            OutputFactory = (left, right) => outputFactory((TItem)left, (TItem)right),
            LeftKeySelector = item => leftKeySelector((TItem)item),
            RightKeySelector = item => (rightKeySelector ?? leftKeySelector)((TItem)item),
            JoinType = joinType,
            LeftFallback = leftFallback is not null
                ? item => leftFallback((TItem)item)
                : null,
            RightFallback = rightFallback is not null
                ? item => rightFallback((TItem)item)
                : null,
        };

        // Register join node with preconfigured instance
        var joinHandle =
            builder.AddJoin<SelfJoinNode<TKey, LeftWrapper<TItem>, RightWrapper<TItem>, TOut>, LeftWrapper<TItem>, RightWrapper<TItem>, TOut>(nodeName);

        _ = builder.AddPreconfiguredNodeInstance(joinHandle.Id, joinNode);

        // Connect the pipeline
        _ = builder.Connect(leftSource, leftWrapHandle);
        _ = builder.Connect(rightSource, rightWrapHandle);
        _ = builder.Connect(leftWrapHandle, joinHandle);
        _ = builder.Connect(rightWrapHandle, joinHandle);

        return joinHandle;
    }
}
