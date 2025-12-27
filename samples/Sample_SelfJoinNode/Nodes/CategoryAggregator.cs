using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_SelfJoinNode;

namespace Sample_SelfJoinNode.Nodes;

/// <summary>
///     Aggregation node that summarizes year-over-year comparisons by category.
///     This node aggregates growth statistics for each product category.
/// </summary>
public class CategoryAggregator : AggregateNode<YearOverYearComparison, string, CategorySummary>
{
    private readonly ILogger<CategoryAggregator>? _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CategoryAggregator" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    public CategoryAggregator(ILogger<CategoryAggregator>? logger = null)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    protected override string GetKey(YearOverYearComparison item) => item.CurrentYearSales.Category;

    /// <inheritdoc />
    protected override CategorySummary CreateAccumulator(string key) => new(key, 0, 0, 0, 0, 0m);

    /// <inheritdoc />
    protected override CategorySummary Accumulate(CategorySummary accumulator, YearOverYearComparison item)
    {
        var growingProducts = accumulator.GrowingProducts;
        var decliningProducts = accumulator.DecliningProducts;
        var newProducts = accumulator.NewProducts;
        var totalGrowth = accumulator.AverageRevenueGrowth * accumulator.TotalProducts;
        var count = accumulator.TotalProducts;

        // Determine growth status
        if (item.RevenueGrowthPercent is null)
        {
            newProducts++;
        }
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

        var avgGrowth = count > 0 ? totalGrowth / count : 0m;

        return accumulator with
        {
            TotalProducts = count,
            GrowingProducts = growingProducts,
            DecliningProducts = decliningProducts,
            NewProducts = newProducts,
            AverageRevenueGrowth = avgGrowth
        };
    }

    /// <inheritdoc />
    protected override CategorySummary Complete(string key, CategorySummary accumulator)
    {
        _logger?.LogInformation(
            "CategoryAggregator: Completed aggregation for category {Category} - {TotalProducts} products, {GrowingProducts} growing, {DecliningProducts} declining, {NewProducts} new",
            key, accumulator.TotalProducts, accumulator.GrowingProducts, accumulator.DecliningProducts, accumulator.NewProducts);

        return accumulator;
    }
}
