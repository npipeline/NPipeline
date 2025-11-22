using System;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace Sample_KeyedJoinNode.Nodes;

/// <summary>
///     Aggregate node that calculates sales statistics by customer tier.
///     This node demonstrates how to perform aggregations on joined data streams.
/// </summary>
public class SalesByCustomerTierAggregator : AggregateNode<EnrichedOrder, string, SalesByCustomerTier>
{
    private readonly ILogger<SalesByCustomerTierAggregator>? _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SalesByCustomerTierAggregator" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    public SalesByCustomerTierAggregator(ILogger<SalesByCustomerTierAggregator>? logger = null)
        : base(WindowAssigner.Tumbling(TimeSpan.FromMinutes(5)))
    {
        _logger = logger;
    }

    /// <summary>
    ///     Extracts the customer tier from an enriched order for grouping.
    /// </summary>
    /// <param name="item">The enriched order.</param>
    /// <returns>The customer tier as the grouping key.</returns>
    public override string GetKey(EnrichedOrder item)
    {
        return item.CustomerTier;
    }

    /// <summary>
    ///     Creates the initial accumulator for a customer tier.
    /// </summary>
    /// <returns>The initial accumulator.</returns>
    public override SalesByCustomerTier CreateAccumulator()
    {
        return new SalesByCustomerTier("", 0, 0m, 0m, 0);
    }

    /// <summary>
    ///     Accumulates sales data for a customer tier.
    /// </summary>
    /// <param name="accumulator">The current accumulator.</param>
    /// <param name="item">The enriched order to accumulate.</param>
    /// <returns>The updated accumulator.</returns>
    public override SalesByCustomerTier Accumulate(SalesByCustomerTier accumulator, EnrichedOrder item)
    {
        var customerTier = item.CustomerTier;
        var totalOrders = accumulator.TotalOrders + 1;
        var totalRevenue = accumulator.TotalRevenue + item.Order.TotalPrice;

        var averageOrderValue = totalOrders > 0
            ? totalRevenue / totalOrders
            : 0m;

        // Track unique customers
        var uniqueCustomers = accumulator.UniqueCustomers;

        if (item.Order.CustomerId != 0) // Exclude placeholder orders
        {
            // For simplicity, we'll increment unique customers for each order
            // In a real scenario, you'd maintain a set of customer IDs
            uniqueCustomers = Math.Max(uniqueCustomers, 1);
        }

        return new SalesByCustomerTier(
            customerTier,
            totalOrders,
            totalRevenue,
            averageOrderValue,
            uniqueCustomers
        );
    }
}

/// <summary>
///     Aggregate node that calculates sales statistics by product category.
///     This node demonstrates how to perform aggregations on joined data streams.
/// </summary>
public class SalesByCategoryAggregator : AggregateNode<EnrichedOrder, string, SalesByCategory>
{
    private readonly ConcurrentDictionary<string, (int count, string productName)> _categoryProductCounts = new();
    private readonly ILogger<SalesByCategoryAggregator>? _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SalesByCategoryAggregator" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    public SalesByCategoryAggregator(ILogger<SalesByCategoryAggregator>? logger = null)
        : base(WindowAssigner.Tumbling(TimeSpan.FromMinutes(5)))
    {
        _logger = logger;
        _categoryProductCounts = new ConcurrentDictionary<string, (int count, string productName)>();
    }

    /// <summary>
    ///     Extracts the product category from an enriched order for grouping.
    /// </summary>
    /// <param name="item">The enriched order.</param>
    /// <returns>The product category as the grouping key.</returns>
    public override string GetKey(EnrichedOrder item)
    {
        return item.ProductCategory;
    }

    /// <summary>
    ///     Creates the initial accumulator for a product category.
    /// </summary>
    /// <returns>The initial accumulator.</returns>
    public override SalesByCategory CreateAccumulator()
    {
        return new SalesByCategory("", 0, 0, 0m, "");
    }

    /// <summary>
    ///     Accumulates sales data for a product category.
    /// </summary>
    /// <param name="accumulator">The current accumulator.</param>
    /// <param name="item">The enriched order to accumulate.</param>
    /// <returns>The updated accumulator.</returns>
    public override SalesByCategory Accumulate(SalesByCategory accumulator, EnrichedOrder item)
    {
        var category = item.ProductCategory;
        var totalOrders = accumulator.TotalOrders + 1;
        var totalQuantity = accumulator.TotalQuantity + item.Order.Quantity;
        var totalRevenue = accumulator.TotalRevenue + item.Order.TotalPrice;

        // Track the most popular product in this category
        var current = _categoryProductCounts.GetOrAdd(item.ProductName, _ => (0, item.ProductName));
        _categoryProductCounts[item.ProductName] = (current.count + 1, item.ProductName);

        var topProduct = GetTopProduct(category);

        return new SalesByCategory(
            category,
            totalOrders,
            totalQuantity,
            totalRevenue,
            topProduct
        );
    }

    /// <summary>
    ///     Gets the top selling product for a category.
    /// </summary>
    /// <param name="category">The product category.</param>
    /// <returns>The name of the top selling product.</returns>
    private string GetTopProduct(string category)
    {
        var topProduct = "";
        var maxCount = 0;

        foreach (var kvp in _categoryProductCounts)
        {
            if (kvp.Value.count > maxCount)
            {
                maxCount = kvp.Value.count;
                topProduct = kvp.Value.productName;
            }
        }

        return topProduct;
    }
}
