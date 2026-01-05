using System;
using NPipeline.Configuration;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace Sample_SelfJoinNode.Nodes;

/// <summary>
///     Aggregation node that summarizes year-over-year comparisons by category.
///     This node aggregates growth statistics for each product category using tumbling windows.
/// </summary>
public class CategoryAggregator : AggregateNode<YearOverYearComparison, string, CategorySummary>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="CategoryAggregator" /> class.
    ///     Uses 30-second tumbling windows for aggregation.
    /// </summary>
    public CategoryAggregator()
        : base(new AggregateNodeConfiguration<YearOverYearComparison>(
            WindowAssigner.Tumbling(TimeSpan.FromSeconds(30))))
    {
    }

    /// <summary>
    ///     Extracts the category key from a year-over-year comparison for grouping.
    /// </summary>
    /// <param name="item">The year-over-year comparison item.</param>
    /// <returns>The category name for grouping.</returns>
    public override string GetKey(YearOverYearComparison item)
    {
        return item.CurrentYearSales.Category;
    }

    /// <summary>
    ///     Creates an initial accumulator for a new category group.
    /// </summary>
    /// <returns>A new CategorySummary with default values.</returns>
    public override CategorySummary CreateAccumulator()
    {
        return new CategorySummary(string.Empty, 0, 0, 0, 0, 0m);
    }

    /// <summary>
    ///     Accumulates a year-over-year comparison into the category summary.
    /// </summary>
    /// <param name="accumulator">The current category summary.</param>
    /// <param name="item">The year-over-year comparison to add.</param>
    /// <returns>The updated category summary.</returns>
    public override CategorySummary Accumulate(CategorySummary accumulator, YearOverYearComparison item)
    {
        var growingProducts = accumulator.GrowingProducts;
        var decliningProducts = accumulator.DecliningProducts;
        var newProducts = accumulator.NewProducts;
        var totalGrowth = accumulator.AverageRevenueGrowth * accumulator.TotalProducts;
        var count = accumulator.TotalProducts;

        // Determine growth status
        if (item.RevenueGrowthPercent is null)
            newProducts++;
        else if (item.RevenueGrowthPercent > 0)
        {
            growingProducts++;
            totalGrowth += item.RevenueGrowthPercent.Value;
        }
        else if (item.RevenueGrowthPercent < 0)
        {
            decliningProducts++;
            totalGrowth += item.RevenueGrowthPercent.Value;
        }
        else
        {
            // Stable (0% growth) - count as neither growing nor declining
            totalGrowth += 0;
        }

        count++;

        var avgGrowth = count > 0
            ? totalGrowth / count
            : 0m;

        return new CategorySummary(
            item.CurrentYearSales.Category,
            count,
            growingProducts,
            decliningProducts,
            newProducts,
            avgGrowth
        );
    }
}
