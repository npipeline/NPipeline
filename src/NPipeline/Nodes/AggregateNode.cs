using NPipeline.DataFlow.Timestamping;
using NPipeline.DataFlow.Windowing;

namespace NPipeline.Nodes;

/// <summary>
///     An abstract base class for creating nodes that perform aggregations on a stream of data
///     within specific time windows. This simplified class is for scenarios where the accumulator
///     and result types are the same.
/// </summary>
/// <remarks>
///     This base class simplifies the common case where you aggregate items into a result of the same type
///     (e.g., accumulating numbers into a sum, grouping items into a collection, etc.).
///     The accumulator is both built up incrementally and used as the final result.
///     For more complex scenarios where the accumulator and result types differ,
///     use <see cref="AdvancedAggregateNode{TIn, TKey, TAccumulate, TResult}" /> instead.
/// </remarks>
/// <typeparam name="TIn">The type of the input data.</typeparam>
/// <typeparam name="TKey">The type of the key used for grouping. Must be not-null.</typeparam>
/// <typeparam name="TResult">The type of the aggregation result (also the accumulator type).</typeparam>
/// <example>
///     <para>Example: Count items by category</para>
///     <code>
///         public class CategoryCounter : AggregateNode&lt;Product, string, int&gt;
///         {
///             public CategoryCounter() : base(WindowAssigner.Tumbling(TimeSpan.FromMinutes(5))) { }
/// 
///             public override string GetKey(Product item) => item.Category;
///             public override int CreateAccumulator() => 0;
///             public override int Accumulate(int count, Product item) => count + 1;
///         }
///     </code>
///     <para>Example: Sum prices by category</para>
///     <code>
///         public class CategorySum : AggregateNode&lt;Product, string, decimal&gt;
///         {
///             public CategorySum() : base(WindowAssigner.Tumbling(TimeSpan.FromMinutes(5))) { }
/// 
///             public override string GetKey(Product item) => item.Category;
///             public override decimal CreateAccumulator() => 0m;
///             public override decimal Accumulate(decimal sum, Product item) => sum + item.Price;
///         }
///     </code>
/// </example>
public abstract class AggregateNode<TIn, TKey, TResult> : AdvancedAggregateNode<TIn, TKey, TResult, TResult>
    where TKey : notnull
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="AggregateNode{TIn, TKey, TResult}" /> class.
    /// </summary>
    /// <param name="windowAssigner">The window assigner strategy to use.</param>
    /// <param name="timestampExtractor">Optional timestamp extractor for the input type.</param>
    /// <param name="maxOutOfOrderness">Maximum time span for out-of-order items. Defaults to 5 minutes.</param>
    /// <param name="watermarkInterval">Interval for watermark updates. Defaults to 30 seconds.</param>
    protected AggregateNode(
        WindowAssigner windowAssigner,
        TimestampExtractor<TIn>? timestampExtractor = null,
        TimeSpan? maxOutOfOrderness = null,
        TimeSpan? watermarkInterval = null)
        : base(windowAssigner, timestampExtractor, maxOutOfOrderness, watermarkInterval)
    {
    }

    /// <summary>
    ///     Produces the final result from the accumulator.
    ///     In the simplified node, the accumulator IS the result, so this returns it as-is.
    /// </summary>
    /// <param name="accumulator">The final accumulator value.</param>
    /// <returns>The accumulator (which is the final result).</returns>
    public sealed override TResult GetResult(TResult accumulator)
    {
        return accumulator;
    }
}
