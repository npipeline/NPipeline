using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace Sample_10_ComplexDataTransformations.Nodes;

/// <summary>
///     Aggregation node that performs complex sales aggregations by product category.
///     This node demonstrates complex aggregations in NPipeline using the AggregateNode base class
///     with time-windowed processing for real-time analytics.
/// </summary>
public class SalesByCategoryAggregator : AggregateNode<EnrichedOrder, string, SalesByCategory>
{
    /// <summary>
    ///     Initializes a new instance of the SalesByCategoryAggregator.
    /// </summary>
    public SalesByCategoryAggregator()
        : base(WindowAssigner.Tumbling(TimeSpan.FromMinutes(5)))
    {
        Console.WriteLine("SalesByCategoryAggregator initialized with 5-minute tumbling windows");
    }

    /// <summary>
    ///     Extracts the category key from an enriched order for grouping.
    /// </summary>
    /// <param name="item">The enriched order item.</param>
    /// <returns>The category name for grouping.</returns>
    public override string GetKey(EnrichedOrder item)
    {
        // Extract category from the first product in the order
        var category = item.Items.FirstOrDefault()?.ProductId switch
        {
            <= 5 => "Electronics",
            <= 10 => "Computers",
            <= 15 => "Accessories",
            _ => "Other",
        };

        Console.WriteLine($"Grouping order by category: {category}");
        return category;
    }

    /// <summary>
    ///     Creates the initial accumulator value for a new category group.
    /// </summary>
    /// <returns>The initial accumulator with zero values.</returns>
    public override SalesByCategory CreateAccumulator()
    {
        return new SalesByCategory(
            string.Empty, // Will be set when first item is processed
            0,
            0m,
            0m,
            DateTime.MinValue,
            DateTime.MaxValue
        );
    }

    /// <summary>
    ///     Accumulates an enriched order into the category aggregator.
    /// </summary>
    /// <param name="accumulator">The current sales aggregation.</param>
    /// <param name="item">The enriched order to accumulate.</param>
    /// <returns>The updated sales aggregation.</returns>
    public override SalesByCategory Accumulate(SalesByCategory accumulator, EnrichedOrder item)
    {
        var category = GetKey(item);
        var totalOrders = accumulator.TotalOrders + 1;
        var totalRevenue = accumulator.TotalRevenue + item.TotalValue;

        var averageOrderValue = totalOrders > 0
            ? totalRevenue / totalOrders
            : 0m;

        Console.WriteLine($"Accumulating order for category '{category}': Orders={totalOrders}, Revenue=${totalRevenue:F2}");

        return new SalesByCategory(
            category,
            totalOrders,
            totalRevenue,
            averageOrderValue,
            accumulator.WindowStart == DateTime.MinValue
                ? DateTime.UtcNow
                : accumulator.WindowStart,
            accumulator.WindowEnd == DateTime.MaxValue
                ? DateTime.UtcNow.AddMinutes(5)
                : accumulator.WindowEnd
        );
    }
}
