# Sample: Self Join Node

This sample demonstrates how to use NPipeline's `AddSelfJoin` extension method to join a data stream with itself. It showcases year-over-year sales comparison
as a practical, real-world use case for self-join operations.

## Overview

The sample simulates a business intelligence scenario where:

- Sales data is collected across multiple years (2022-2024)
- Products need to be compared year-over-year to identify growth trends
- New products (no previous year data) and discontinued products need special handling
- Category-level summaries provide insights into overall performance

## Key Concepts Demonstrated

### Self-Join Functionality

- **AddSelfJoin Extension Method**: Convenient API for joining a data stream with itself
- **Type-Safe Wrapper Types**: Internal `LeftWrapper<T>` and `RightWrapper<T>` handle the "type erasure" issue in join nodes
- **Same-Type Joins**: Joining data of the same type (SalesData) based on different criteria
- **Flexible Join Strategies**: Inner, LeftOuter, RightOuter, and FullOuter join types

### Data Processing Patterns

- **Temporal Data Analysis**: Comparing data across time periods
- **Data Enrichment**: Creating rich comparison objects from raw sales data
- **Aggregation**: Generating category-level summaries from individual product comparisons
- **Null Handling**: Gracefully handling missing data for new and discontinued products

### Performance Considerations

- **Memory Management**: Self-join nodes maintain in-memory buffers for unmatched items
- **Key Selection**: Using ProductId as the join key for efficient matching
- **Stream Processing**: Real-time processing as data flows through the pipeline

## Architecture

### Data Models

- `SalesData`: Base type containing sales information for a product in a specific year
- `YearOverYearComparison`: Result of joining current year sales with previous year sales
- `CategorySummary`: Aggregated growth statistics by product category

### Pipeline Components

#### Source Nodes

- `SalesDataSource`: Generates realistic sales data for multiple years with growth patterns

#### Processing Nodes

- `CurrentYearFilter`: Filters data for the current (comparison) year
- `PreviousYearFilter`: Filters data for the previous year
- `SelfJoinNode`: Joins the two filtered streams using AddSelfJoin extension method
- `CategoryAggregator`: Aggregates growth statistics by category

#### Output Nodes

- `ConsoleSink<T>`: Generic sink for formatted console output

## Pipeline Flow

```
SalesDataSource ──┬──► CurrentYearFilter ──┐
                  │                       ├──► SelfJoin ──┬──► ConsoleSink (Comparisons)
                  └──► PreviousYearFilter ──┘              │
                                                           └──► CategoryAggregator ──► ConsoleSink (Summaries)
```

### Self-Join Mechanics

The self-join operation works as follows:

1. **Stream Splitting**: The source stream is split into two parallel streams:
    - Current year data (left side of join)
    - Previous year data (right side of join)

2. **Type Wrapping**: Internally, items are wrapped to distinguish left and right inputs:
    - `LeftWrapper<SalesData>` for current year data
    - `RightWrapper<SalesData>` for previous year data

3. **Key Extraction**: The join key (ProductId) is extracted from both inputs

4. **Matching**: Items are matched based on ProductId using the specified join type

5. **Result Creation**: Matched items are combined into `YearOverYearComparison` objects

### Join Strategies

#### Inner Join (Default)

- Only produces output when both current and previous year data exist
- Excludes new products (no previous year data)
- Excludes discontinued products (no current year data)
- Most common for strict year-over-year comparisons

#### Left Outer Join

- All current year products are included
- New products show "New Product" status with null previous year data
- Discontinued products are excluded
- Useful when current year completeness is critical

#### Right Outer Join

- All previous year products are included
- Discontinued products are included with null current year data
- New products are excluded
- Useful for historical analysis and tracking discontinued items

#### Full Outer Join

- All products from both years are included
- New products show "New Product" status
- Discontinued products are included
- Maximum data preservation with appropriate null handling

## Real-World Applications

### Year-Over-Year Sales Analysis

```csharp
// Compare 2024 sales with 2023 sales
var selfJoin = builder.AddSelfJoin<
    SalesData,
    SalesData,
    int,
    YearOverYearComparison
>(
    "self-join",
    JoinType.LeftOuter,
    (left, right) => new YearOverYearComparison(left, right)
);
```

### Price Comparison Across Catalogs

```csharp
// Compare prices between two catalog versions
var priceComparison = builder.AddSelfJoin<
    ProductPrice,
    ProductPrice,
    string,
    PriceChange
>(
    "price-comparison",
    JoinType.Inner,
    (current, previous) => new PriceChange(current, previous)
);
```

### Event Correlation Between Systems

```csharp
// Correlate events from two system logs
var eventCorrelation = builder.AddSelfJoin<
    SystemEvent,
    SystemEvent,
    string,
    CorrelatedEvent
>(
    "event-correlation",
    JoinType.FullOuter,
    (systemA, systemB) => new CorrelatedEvent(systemA, systemB)
);
```

### Order Matching from Different Sources

```csharp
// Match orders from different sales channels
var orderMatching = builder.AddSelfJoin<
    Order,
    Order,
    string,
    MatchedOrder
>(
    "order-matching",
    JoinType.Inner,
    (online, retail) => new MatchedOrder(online, retail)
);
```

## Running the Sample

### Prerequisites

- .NET 8.0 or later
- NPipeline NuGet packages

### Execution

```bash
dotnet run --project Sample_SelfJoinNode
```

### Expected Output

The sample will demonstrate each join type sequentially:

#### 1. Inner Join Output

```
========================================
Demonstrating Inner Join
========================================

Join Type: Inner
Inner Join: Only products with data in both years will be compared.
Comparison Year: 2024

Starting pipeline execution...

Output: [Moderate Growth] Smartphone (Electronics) - Revenue: $85,234.56 (Δ +12.5%), Units: 245 (Δ +8.3%)
Output: [Strong Growth] Laptop (Electronics) - Revenue: $112,345.67 (Δ +25.3%), Units: 189 (Δ +18.2%)
Output: [Decline] Tablet (Electronics) - Revenue: $45,678.90 (Δ -5.2%), Units: 156 (Δ -3.1%)

📋 comparison-sink: Processed 15 items total

Output: Category: Electronics - Products: 3, Growing: 2 (66.7%), Declining: 1, New: 0, Avg Growth: +10.9%
Output: Category: Clothing - Products: 3, Growing: 2 (66.7%), Declining: 1, New: 0, Avg Growth: +8.5%
Output: Category: Books - Products: 3, Growing: 1 (33.3%), Declining: 2, New: 0, Avg Growth: -2.1%

📋 category-sink: Processed 5 items total
```

#### 2. Left Outer Join Output

```
========================================
Demonstrating LeftOuter Join
========================================

Join Type: LeftOuter
Left Outer Join: All products in the current year will be included. New products (no previous year data) will show 'New Product' status.
Comparison Year: 2024

Starting pipeline execution...

Output: [New Product] Smart Watch (Electronics) - Revenue: $34,567.89 (Δ N/A), Units: 123 (Δ N/A)
Output: [Moderate Growth] Smartphone (Electronics) - Revenue: $85,234.56 (Δ +12.5%), Units: 245 (Δ +8.3%)
Output: [Strong Growth] Laptop (Electronics) - Revenue: $112,345.67 (Δ +25.3%), Units: 189 (Δ +18.2%)

📋 comparison-sink: Processed 18 items total

Output: Category: Electronics - Products: 4, Growing: 2 (50.0%), Declining: 1, New: 1, Avg Growth: +10.9%
Output: Category: Clothing - Products: 4, Growing: 2 (50.0%), Declining: 1, New: 1, Avg Growth: +8.5%

📋 category-sink: Processed 5 items total
```

#### 3. Right Outer Join Output

```
========================================
Demonstrating RightOuter Join
========================================

Join Type: RightOuter
Right Outer Join: All products in the previous year will be included. Discontinued products (no current year data) will be included.
Comparison Year: 2024

Starting pipeline execution...

Output: [Significant Decline] Headphones (Electronics) - Revenue: $0.00 (Δ N/A), Units: 0 (Δ N/A)
Output: [Moderate Growth] Smartphone (Electronics) - Revenue: $85,234.56 (Δ +12.5%), Units: 245 (Δ +8.3%)

📋 comparison-sink: Processed 17 items total

Output: Category: Electronics - Products: 4, Growing: 2 (50.0%), Declining: 2, New: 0, Avg Growth: -5.2%
Output: Category: Clothing - Products: 4, Growing: 2 (50.0%), Declining: 2, New: 0, Avg Growth: -3.1%

📋 category-sink: Processed 5 items total
```

#### 4. Full Outer Join Output

```
========================================
Demonstrating FullOuter Join
========================================

Join Type: FullOuter
Full Outer Join: All products from both years will be included, with appropriate handling for new and discontinued products.
Comparison Year: 2024

Starting pipeline execution...

Output: [New Product] Smart Watch (Electronics) - Revenue: $34,567.89 (Δ N/A), Units: 123 (Δ N/A)
Output: [Significant Decline] Headphones (Electronics) - Revenue: $0.00 (Δ N/A), Units: 0 (Δ N/A)
Output: [Moderate Growth] Smartphone (Electronics) - Revenue: $85,234.56 (Δ +12.5%), Units: 245 (Δ +8.3%)

📋 comparison-sink: Processed 20 items total

Output: Category: Electronics - Products: 5, Growing: 2 (40.0%), Declining: 2, New: 1, Avg Growth: +2.8%
Output: Category: Clothing - Products: 5, Growing: 2 (40.0%), Declining: 2, New: 1, Avg Growth: +2.5%

📋 category-sink: Processed 5 items total
```

## Extending the Sample

### Custom Self-Join Nodes

```csharp
// Custom self-join for comparing inventory levels
var inventoryComparison = builder.AddSelfJoin<
    InventorySnapshot,
    InventorySnapshot,
    int,
    InventoryChange
>(
    "inventory-comparison",
    JoinType.LeftOuter,
    (current, previous) => new InventoryChange(current, previous)
);
```

### Composite Keys

```csharp
// Self-join with composite key (ProductId + Location)
var locationComparison = builder.AddSelfJoin<
    LocationSales,
    LocationSales,
    (int, string),
    LocationComparison
>(
    "location-comparison",
    JoinType.Inner,
    (current, previous) => new LocationComparison(current, previous)
);
```

### Custom Aggregations

```csharp
public class CustomGrowthAggregator : AggregateNode<YearOverYearComparison, string, CustomGrowthMetric>
{
    protected override string GetKey(YearOverYearComparison item) => item.CurrentYearSales.Category;

    protected override CustomGrowthMetric CreateAccumulator(string key) =>
        new(key, 0, 0m, 0m, 0);

    protected override CustomGrowthMetric Accumulate(CustomGrowthMetric accumulator, YearOverYearComparison item)
    {
        var totalRevenue = accumulator.TotalRevenue + item.CurrentYearSales.Revenue;
        var totalGrowth = accumulator.TotalGrowth + (item.RevenueGrowthPercent ?? 0m);
        var count = accumulator.Count + 1;

        return accumulator with
        {
            TotalRevenue = totalRevenue,
            TotalGrowth = totalGrowth,
            AverageGrowth = totalGrowth / count,
            Count = count
        };
    }
}
```

## Best Practices

1. **Choose the Right Join Type**: Select based on business requirements and data quality
    - Use Inner Join for strict comparisons
    - Use LeftOuter Join when current year completeness is critical
    - Use RightOuter Join for historical analysis
    - Use FullOuter Join for comprehensive data preservation

2. **Handle Nulls Gracefully**: Implement proper null handling for unmatched items
    - Use nullable types for optional previous year data
    - Provide meaningful status indicators (e.g., "New Product")
    - Consider default values for calculations

3. **Monitor Memory Usage**: Be aware of memory consumption with large datasets
    - Self-join nodes buffer unmatched items in memory
    - Consider data volume and key cardinality
    - Use appropriate join types to minimize buffering

4. **Optimize Key Selection**: Use efficient join keys for better performance
    - Prefer low-cardinality keys
    - Ensure keys are indexed if using external data sources
    - Consider composite keys for complex matching

5. **Test with Realistic Data**: Use representative data volumes and distributions
    - Include edge cases (new products, discontinued products)
    - Test with various growth patterns
    - Verify aggregation accuracy

## Troubleshooting

### Common Issues

- **Memory Pressure**: Large datasets or high-cardinality keys
    - Solution: Use appropriate join types, consider data partitioning

- **Performance Issues**: Poor key distribution or stream ordering
    - Solution: Optimize key selection, consider pre-sorting

- **Data Quality**: Unexpected null values or missing matches
    - Solution: Implement proper null handling, validate input data

- **Type Erasure**: Confusion about left vs right inputs
    - Solution: Understand the internal wrapper types, use clear variable names

### Debugging Tips

1. **Enable Debug Logging**: Set log level to Debug for detailed join operations
2. **Inspect Join Results**: Use ConsoleSink to view individual comparisons
3. **Verify Key Extraction**: Ensure keys are correctly extracted from both inputs
4. **Check Aggregation**: Verify category summaries match individual comparisons

## Related Samples

- [Sample 13: Keyed Join Node](../Sample_KeyedJoinNode/) - Multi-stream joins with different types
- [Sample 8: Aggregate Node](../Sample_AggregateNode/) - Aggregation patterns
- [Sample 5: Branch Node](../Sample_BranchNode/) - Stream splitting patterns
- [Sample 12: Batching Node](../Sample_BatchingNode/) - Stream processing patterns

## Summary

This sample demonstrates the power and flexibility of NPipeline's self-join functionality through a practical year-over-year sales analysis scenario. The
`AddSelfJoin` extension method provides a clean, type-safe API for joining a data stream with itself, solving the "type erasure" issue that would otherwise
complicate such operations.

Key takeaways:

- Self-joins are essential for temporal data analysis and comparisons
- The AddSelfJoin extension method provides a convenient, type-safe API
- Different join types serve different business requirements
- Proper null handling is critical for robust self-join operations
- Aggregations can build powerful insights from self-join results

This implementation showcases production-ready patterns for business intelligence, data analysis, and real-time reporting in NPipeline applications.
