using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_SelfJoinNode;

namespace Sample_SelfJoinNode.Nodes;

/// <summary>
///     Source node that generates sample sales data for multiple years.
///     This node creates a realistic dataset demonstrating year-over-year sales patterns.
/// </summary>
public class SalesDataSource : SourceNode<SalesData>
{
    private readonly ILogger<SalesDataSource>? _logger;
    private readonly int _startYear;
    private readonly int _endYear;
    private readonly int _productsPerCategory;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SalesDataSource" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="startYear">The starting year for sales data.</param>
    /// <param name="endYear">The ending year for sales data.</param>
    /// <param name="productsPerCategory">Number of products per category.</param>
    public SalesDataSource(
        ILogger<SalesDataSource>? logger = null,
        int startYear = 2022,
        int endYear = 2024,
        int productsPerCategory = 5)
    {
        _logger = logger;
        _startYear = startYear;
        _endYear = endYear;
        _productsPerCategory = productsPerCategory;
    }

    /// <inheritdoc />
    public override IDataPipe<SalesData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger?.LogInformation("SalesDataSource: Generating sales data from {StartYear} to {EndYear}", _startYear, _endYear);

        var random = new Random(42); // Fixed seed for reproducible results
        var categories = new[] { "Electronics", "Clothing", "Books", "Home & Garden", "Sports" };
        var productNames = new Dictionary<string, string[]>
        {
            ["Electronics"] = new[] { "Smartphone", "Laptop", "Tablet", "Headphones", "Smart Watch" },
            ["Clothing"] = new[] { "T-Shirt", "Jeans", "Jacket", "Sneakers", "Hat" },
            ["Books"] = new[] { "Fiction Novel", "Technical Guide", "Biography", "Cookbook", "Children's Book" },
            ["Home & Garden"] = new[] { "Coffee Maker", "Blender", "Vacuum Cleaner", "Lamp", "Plant Pot" },
            ["Sports"] = new[] { "Basketball", "Tennis Racket", "Yoga Mat", "Running Shoes", "Dumbbells" }
        };

        var salesData = new List<SalesData>();
        var productId = 1;

        foreach (var category in categories)
        {
            var categoryProducts = productNames[category];
            
            for (var productIndex = 0; productIndex < _productsPerCategory; productIndex++)
            {
                var productName = categoryProducts[productIndex % categoryProducts.Length];
                
                // Generate base revenue and units for the first year
                var baseRevenue = (decimal)(random.Next(10000, 100000) + random.NextDouble());
                var baseUnits = random.Next(100, 1000);
                
                for (var year = _startYear; year <= _endYear; year++)
                {
                    // Apply growth/decline patterns to simulate realistic sales
                    var growthFactor = 1.0 + (random.NextDouble() - 0.3) * 0.4; // -12% to +28% growth
                    var revenue = baseRevenue * (decimal)growthFactor * (1 + (year - _startYear) * 0.1);
                    var units = (int)(baseUnits * growthFactor * (1 + (year - _startYear) * 0.05));
                    
                    // Introduce some discontinued products (no data in later years)
                    if (year > _startYear + 1 && productId % 7 == 0)
                    {
                        continue; // Skip this year for discontinued products
                    }
                    
                    // Introduce some new products (no data in earlier years)
                    if (year < _endYear - 1 && productId % 11 == 0)
                    {
                        continue; // Skip earlier years for new products
                    }

                    var sales = new SalesData(
                        productId,
                        productName,
                        year,
                        Math.Round(revenue, 2),
                        units,
                        category
                    );

                    salesData.Add(sales);

                    _logger?.LogDebug(
                        "SalesDataSource: Generated sales for Product {ProductId} ({ProductName}), Year {Year}, Revenue {Revenue:C}, Units {Units}",
                        sales.ProductId, sales.ProductName, sales.Year, sales.Revenue, sales.UnitsSold);
                }

                productId++;
            }
        }

        _logger?.LogInformation("SalesDataSource: Generated {Count} sales records", salesData.Count);

        return new InMemoryDataPipe<SalesData>(salesData, "SalesDataSource");
    }
}
