# Sample_CustomMergeNode

This sample demonstrates advanced stream merging capabilities in NPipeline for financial trading systems. It showcases how to merge market data from multiple
exchanges using CustomMergeNode with priority-based conflict resolution and temporal alignment strategies.

## Overview

In modern financial trading systems, receiving market data from multiple exchanges is common. Each exchange may provide different prices, volumes, and update
frequencies for the same financial instruments. This sample demonstrates how to:

- Merge market data streams from multiple exchanges (NYSE, NASDAQ, International)
- Apply priority-based conflict resolution when data conflicts occur
- Perform temporal alignment to handle timing differences between exchanges
- Validate data quality and calculate quality scores
- Output merged market data with quality annotations

## Key Concepts Demonstrated

- **CustomMergeNode**: Advanced merging with custom conflict resolution logic
- **Priority-based Merging**: Resolving conflicts based on exchange priorities
- **Temporal Alignment**: Handling timing differences with configurable tolerance
- **Data Quality Validation**: Scoring and validating incoming market data
- **Financial Data Modeling**: Realistic market data structures and business logic
- **Multi-stream Processing**: Coordinating multiple data sources simultaneously

## Running the Sample

```bash
dotnet run --project Sample_CustomMergeNode.csproj
```

## Pipeline Architecture

The pipeline implements a financial trading data processing flow:

```
NYSE Market Data Source ──┐
                          ├── PriorityBasedMergeNode ── DataQualityValidator ── MarketDataSink
NASDAQ Market Data Source ─┤
                          │
International Market Data Source ──┘
```

### Components

1. **Market Data Sources**:
    - `NyseMarketDataSource`: Simulates NYSE market data feed
    - `NasdaqMarketDataSource`: Simulates NASDAQ market data feed
    - `InternationalMarketDataSource`: Simulates international exchange data feed

2. **Merge Processing**:
    - `PriorityBasedMergeNode`: Custom merge node implementing advanced merging logic
    - `PriorityMergeStrategy`: Implements priority-based conflict resolution
    - `TemporalAlignmentStrategy`: Handles temporal alignment with delay tolerance

3. **Quality Assurance**:
    - `DataQualityValidator`: Validates and scores data quality
    - `DataQualityScore`: Calculates and stores quality metrics

4. **Output**:
    - `MarketDataSink`: Outputs merged market data with quality annotations

## Data Models

### MarketDataTick

Represents individual market data ticks with:

- Exchange identifier
- Financial symbol (ticker)
- Current price
- Trading volume
- UTC timestamp
- Data quality indicators

### ExchangePriority

Enum defining exchange priorities:

- NYSE = 1 (highest priority)
- NASDAQ = 2 (medium priority)
- International = 3 (lowest priority)

### DataQualityScore

Class for calculating and storing:

- Completeness score
- Timeliness score
- Accuracy score
- Overall quality rating

### MergedMarketData

Result of the merge process containing:

- Merged price and volume
- Quality annotations
- Source exchange information
- Merge metadata

## Merge Strategies

### Priority-based Conflict Resolution

When multiple exchanges provide conflicting data for the same symbol:

1. NYSE data takes precedence over NASDAQ and International
2. NASDAQ data takes precedence over International
3. Temporal proximity is considered within priority tiers
4. Quality scores influence final selection

### Temporal Alignment

Handles timing differences between exchanges:

- Configurable delay tolerance window
- Interpolation for missing data points
- Time-based synchronization
- Out-of-order data handling

## Implementation Details

### CustomMergeNode Features

- Multi-stream input handling
- Configurable merge strategies
- Conflict resolution logic
- Quality-aware merging
- Temporal synchronization

### Error Handling

- Graceful degradation when exchanges are unavailable
- Data validation and quality checks
- Timeout handling for delayed data
- Circuit breaker patterns for reliability

### Performance Considerations

- Efficient stream processing using Channel<T> for high-throughput scenarios
- Minimal memory footprint with bounded channels and backpressure handling
- Parallel processing capabilities with concurrent data streams
- Configurable batch sizes and buffer management
- Lock-free concurrent operations for maximum performance
- Intelligent drop strategies (DropOldest, DropNewest) for backpressure scenarios

### Channel<T> Implementation

- High-performance concurrent data structure for producer-consumer scenarios
- Bounded channels prevent memory leaks in high-throughput systems
- Configurable full mode strategies for backpressure handling
- Lock-free operations for optimal CPU utilization
- Async/await patterns for non-blocking I/O operations
- Proper disposal and resource cleanup

### High-Frequency Trading Optimizations

- Sub-millisecond latency processing for real-time trading scenarios
- Priority-based routing for time-critical market data
- Quality scoring for automated data validation
- Temporal alignment with configurable delay tolerance
- Circuit breaker patterns for system reliability
- Comprehensive metrics and monitoring capabilities
- Error isolation and graceful degradation

## Key Takeaways

1. **Custom Merge Logic**: CustomMergeNode enables sophisticated merging beyond simple concatenation
2. **Business Rule Integration**: Priority-based merging reflects real-world trading preferences
3. **Quality Awareness**: Data quality scoring ensures reliable output
4. **Temporal Coordination**: Time alignment handles real-world timing discrepancies
5. **Extensibility**: Strategy pattern allows for different merge approaches

## Extending the Sample

This sample can be extended to demonstrate:

- Additional exchanges and data sources
- More sophisticated merge algorithms
- Real-time market data feeds
- Persistence and historical analysis
- Alerting for unusual market conditions
- Performance monitoring and metrics

## Dependencies

- NPipeline core library
- NPipeline.Extensions.DependencyInjection
- Microsoft.Extensions.Hosting

## .NET Versions

This sample supports:

- .NET 8.0
- .NET 9.0
- .NET 10.0
