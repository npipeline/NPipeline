# Sample 10: Complex Data Transformations

## Overview

This sample demonstrates advanced NPipeline capabilities for sophisticated data processing scenarios including multi-stream joins, lookup operations, complex
aggregations, and data lineage tracking.

## Core Concepts

1. **Multi-Stream Joins**
    - Joining orders with customer data using KeyedJoinNode
    - Inner and outer join patterns with KeySelector attributes

2. **External Data Lookups**
    - Product enrichment using LookupNode
    - Asynchronous data fetching and caching patterns

3. **Complex Aggregations**
    - Time-windowed sales aggregation by category
    - Customer behavior analysis with multiple metrics
    - Tumbling windows for real-time analytics

4. **Data Lineage Tracking**
    - Complete audit trail of data transformations
    - Transformation metadata and timestamps
    - Debugging and governance capabilities

## Quick Setup and Run

### Prerequisites

- .NET 8.0, .NET 9.0 or .NET 10.0 SDK
- JetBrains Rider, Visual Studio 2022, VS Code, or .NET CLI

### Running the Sample

```bash
cd samples/Sample_10_ComplexDataTransformations
dotnet restore
dotnet run
```

## Pipeline Flow

```
OrderSource ──┐
              ├── OrderCustomerJoin ──┬── ProductLookup ──┬── SalesByCategoryAggregation ──┐
CustomerSource ──┘                    │                    │                                  │
                                       │                    ├── CustomerBehaviorAggregation ────┤
ProductSource ──────────────────────────┘                    │                                  │
                                                        ├── LineageTrackingNode ────────┤
                                                        │                                  │
                                                        └── ConsoleSink <───────────────────┘

LineageSink <───────────────────────────────────────────────────┘
```

## Key Components

### Data Models

- **Order**: E-commerce order records
- **Customer**: Customer information with demographics
- **Product**: Product catalog with pricing
- **EnrichedOrder**: Orders with customer and product data
- **SalesByCategory**: Aggregated sales metrics
- **CustomerPurchaseBehavior**: Customer analytics data

### Custom Nodes

- **OrderSource/CustomerSource/ProductSource**: Data generation
- **OrderCustomerJoinNode**: Multi-stream join implementation
- **ProductLookupNode**: External data enrichment
- **SalesByCategoryAggregator**: Category-based sales analytics
- **CustomerBehaviorAggregator**: Customer pattern analysis
- **LineageTrackingNode**: Data provenance tracking
- **ConsoleSink/LineageSink**: Result output

## Expected Output

The sample generates realistic e-commerce data and processes it through the pipeline, producing:

1. **Sales Analytics**: Revenue and order counts by product category
2. **Customer Insights**: Purchase patterns and behavior metrics
3. **Lineage Information**: Complete transformation audit trail
4. **Processing Statistics**: Performance and throughput metrics

## Advanced Features Demonstrated

- **Stream Processing**: Real-time data flow with multiple sources
- **Stateful Operations**: Windowed aggregations with time semantics
- **Data Enrichment**: External lookups with caching
- **Auditability**: Complete lineage tracking for compliance
- **Performance**: Efficient processing with minimal allocations

## Production Considerations

This sample showcases patterns suitable for:

- Real-time analytics platforms
- Data warehouse ETL pipelines
- Customer analytics systems
- Financial data processing
- IoT data aggregation

The implementation demonstrates production-ready patterns for scalability, reliability, and observability.
