# Sample 13: Keyed Join Node

This sample demonstrates how to use NPipeline's `KeyedJoinNode` to join data streams based on common keys. It showcases different join strategies, data
enrichment patterns, and real-time aggregation capabilities.

## Overview

The sample simulates a real-world e-commerce scenario where:

- Orders are processed with customer foreign keys
- Customer master data is maintained separately
- Product information enriches the joined data
- Business intelligence is generated through aggregations

## Key Concepts Demonstrated

### KeyedJoinNode Functionality

- **Inner Join**: Only processes orders with matching customers
- **Left Outer Join**: Processes all orders, with placeholder data for unmatched customers
- **Right Outer Join**: Processes all customers, with placeholder data for unmatched orders
- **Full Outer Join**: Processes all orders and customers, handling unmatched items appropriately

### Data Processing Patterns

- **Multi-stream correlation**: Joining independent data streams based on common keys
- **Data enrichment**: Adding product information to joined order-customer data
- **Real-time aggregation**: Generating business insights as data flows through the pipeline
- **Unmatched item handling**: Strategies for dealing with data quality issues

### Performance Considerations

- **Memory management**: KeyedJoinNode maintains in-memory buffers for unmatched items
- **Key distribution**: Join performance depends on key cardinality and distribution
- **Stream ordering**: Impact of input stream ordering on join efficiency

## Architecture

### Data Models

- `Order`: Customer orders with product, quantity, and pricing information
- `Customer`: Customer master data with tier and contact information
- `Product`: Product catalog with category and pricing
- `OrderCustomerJoin`: Result of joining orders with customers
- `EnrichedOrder`: Orders enriched with product information
- `SalesByCustomerTier`: Aggregated sales data by customer tier
- `SalesByCategory`: Aggregated sales data by product category

### Pipeline Components

#### Source Nodes

- `OrderSource`: Generates sample order data with realistic distribution
- `CustomerSource`: Provides customer master data
- `ProductSource`: Supplies product catalog information

#### Processing Nodes

- `OrderCustomerJoinNode`: KeyedJoinNode implementation joining orders and customers
- `ProductLookupNode`: Enriches joined data with product information
- `SalesByCustomerTierAggregator`: Aggregates sales by customer tier
- `SalesByCategoryAggregator`: Aggregates sales by product category

#### Output Nodes

- `ConsoleSink<T>`: Generic sink for formatted console output

## Pipeline Flow

```
OrderSource ──┐
               ├──► OrderCustomerJoin ──┐
CustomerSource ──┘                      │
                                       ├──► ProductLookup ──┬──► SalesByCustomerTier ──► ConsoleSink
ProductSource ─────────────────────────────┘                    │
                                                          ├──► SalesByCategory ──► ConsoleSink
                                                          │
                                                          └──► ConsoleSink (Enriched Orders)
```

### Join Strategies

#### Inner Join (Default)

- Only produces output when both order and customer exist
- Most common join type for transactional processing
- Eliminates data quality issues but may miss valid business data

#### Left Outer Join

- All orders are processed, regardless of customer existence
- Placeholder customer data for unmatched orders
- Useful when order completeness is critical

#### Right Outer Join

- All customers are processed, regardless of order existence
- Placeholder order data for customers without orders
- Useful for customer analytics and reporting

#### Full Outer Join

- All orders and customers are processed
- Comprehensive view of all data
- Maximum data preservation with appropriate null handling

## Real-World Applications

### E-commerce Order Processing

```csharp
// Join orders with customer data for personalized marketing
var orderCustomerJoin = new OrderCustomerJoinNode(logger, JoinType.LeftOuter);
```

### Financial Transaction Matching

```csharp
// Match transactions with account details
var transactionAccountJoin = new TransactionAccountJoinNode(logger, JoinType.Inner);
```

### IoT Sensor Data Fusion

```csharp
// Combine sensor readings with calibration data
var sensorCalibrationJoin = new SensorCalibrationJoinNode(logger, JoinType.LeftOuter);
```

### Log Analysis with User Profiles

```csharp
// Enrich log events with user profile information
var logProfileJoin = new LogProfileJoinNode(logger, JoinType.LeftOuter);
```

## Performance Optimization

### Memory Management

- KeyedJoinNode buffers unmatched items in memory
- Consider data volume and key cardinality when choosing join types
- Monitor memory usage with large datasets

### Key Selection

- Use low-cardinality keys for better performance
- Composite keys should be carefully designed
- Consider key distribution patterns

### Stream Ordering

- Ordered streams can improve join performance
- Consider pre-sorting when possible
- Be aware of the trade-offs in real-time scenarios

## Data Quality Considerations

### Handling Unmatched Items

- Inner joins eliminate unmatched items (may lose valid data)
- Outer joins preserve all items (require null handling)
- Choose based on business requirements

### Error Handling

- The sample demonstrates graceful handling of missing data
- Placeholder values maintain data structure integrity
- Logging helps identify data quality issues

## Running the Sample

### Prerequisites

- .NET 8.0 or later
- NPipeline NuGet packages

### Execution

```bash
dotnet run --project Sample_13_KeyedJoinNode
```

### Expected Output

The sample will demonstrate each join type sequentially:

1. Inner Join - shows only matched orders and customers
2. Left Outer Join - includes unmatched orders with placeholder customers
3. Right Outer Join - includes unmatched customers with placeholder orders
4. Full Outer Join - includes all data with appropriate handling

For each join type, you'll see:

- Real-time join operations as they occur
- Product enrichment results
- Aggregated sales data by customer tier and category
- Summary statistics

## Extending the Sample

### Custom Join Nodes

```csharp
[KeySelector(typeof(Order), "OrderId")]
[KeySelector(typeof(OrderDetail), "OrderId")]
public class OrderDetailJoinNode : KeyedJoinNode<int, Order, OrderDetail, OrderWithDetails>
{
    public override OrderWithDetails CreateOutput(Order order, OrderDetail detail)
    {
        return new OrderWithDetails(order, detail);
    }
}
```

### Composite Keys

```csharp
[KeySelector(typeof(Transaction), "AccountId", "TransactionDate")]
[KeySelector(typeof(DailyBalance), "AccountId", "BalanceDate")]
public class TransactionBalanceJoinNode : KeyedJoinNode<(int, DateTime), Transaction, DailyBalance, ReconciledTransaction>
{
    // Implementation
}
```

### Custom Aggregations

```csharp
public class CustomSalesAggregator : AggregateNode<EnrichedOrder, string, CustomSalesMetric>
{
    protected override string GetKey(EnrichedOrder item) => item.ProductCategory;

    protected override CustomSalesMetric CreateAccumulator(string key) => new(key, 0, 0m);

    protected override CustomSalesMetric Accumulate(CustomSalesMetric accumulator, EnrichedOrder item)
    {
        return accumulator with
        {
            TotalOrders = accumulator.TotalOrders + 1,
            TotalRevenue = accumulator.TotalRevenue + item.Order.TotalPrice
        };
    }
}
```

## Best Practices

1. **Choose the Right Join Type**: Select based on business requirements and data quality
2. **Monitor Memory Usage**: Be aware of memory consumption with large datasets
3. **Handle Nulls Gracefully**: Implement proper null handling for unmatched items
4. **Log Join Operations**: Monitor join performance and data quality issues
5. **Test with Realistic Data**: Use representative data volumes and key distributions
6. **Consider Performance**: Optimize key selection and stream ordering for your use case

## Troubleshooting

### Common Issues

- **Memory Pressure**: Large datasets or high-cardinality keys
- **Performance Issues**: Poor key distribution or stream ordering
- **Data Quality**: Unexpected null values or missing matches

### Solutions

- Use appropriate join types for your data quality
- Monitor and tune memory usage
- Consider data preprocessing for better key distribution
- Implement proper error handling and logging

## Related Samples

- [Sample 10: Complex Data Transformations](../Sample_10_ComplexDataTransformations/) - Advanced join patterns
- [Sample 12: Batching Node](../Sample_12_BatchingNode/) - Stream processing patterns
- [Sample 11: CSV Connector](../Sample_11_CsvConnector/) - External data integration
