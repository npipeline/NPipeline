# Sample_UnbatchingNode

This sample demonstrates the unbatching functionality in NPipeline for converting batched analytics results back to individual item streams. It shows how to efficiently process data in batches for analytics while maintaining the ability to generate individual events for real-time processing and alerting.

## Overview

Unbatching is a crucial pattern for scenarios where you need both efficient batch processing and individual event processing. This sample demonstrates how to:

- Process individual market data events in batches for efficient analytics
- Convert batch analytics results back to individual alert events
- Enable real-time alerting based on batch insights
- Maintain traceability between batch results and original events

## Key Concepts Demonstrated

### Unbatching Pattern

The unbatching pattern involves:
- **Batching for Efficiency**: Collect individual items into batches for cost-effective processing
- **Batch Analytics**: Perform comprehensive analytics on batches to derive insights
- **Unbatching for Real-time**: Convert batch results back to individual events for downstream processing
- **Individual Event Processing**: Process individual events for real-time operations like alerting

### Financial Trading Scenario

This sample implements a realistic financial trading system:
- **Market Data Ingestion**: Individual market data events from multiple exchanges
- **Batch Analytics**: Efficient processing of market data in batches for analytics
- **Real-time Alerting**: Individual alert events generated from batch insights
- **Threshold-based Monitoring**: Alerts based on price anomalies, volatility, and anomaly scores

## Sample Scenario

This sample simulates a financial trading system that processes market data:

1. **MarketDataSource**: Generates individual market data events from multiple exchanges
2. **BatchingNode**: Collects events into batches based on size and time thresholds
3. **BatchAnalyticsTransform**: Processes batches with comprehensive analytics calculations
4. **IndividualEventProcessor**: Converts batch results back to individual alert events (UNBATCHING)
5. **RealTimeAlertingSink**: Processes individual alert events for real-time monitoring

## Running the Sample

### Basic Execution

```bash
dotnet run --project samples/Sample_UnbatchingNode
```

This will run the unbatching pipeline with default configuration:
- Batch size: 15 events
- Batch timeout: 2 seconds
- Market data events: 100 events
- Price anomaly threshold: 2%
- Volatility threshold: 5%
- Anomaly score threshold: 0.7

### Running Tests

```bash
dotnet test samples/Sample_UnbatchingNode
```

The comprehensive test suite covers:
- Different unbatching scenarios
- Various batch sizes and thresholds
- Error handling scenarios
- Performance characteristics
- Alert generation patterns

## Code Structure

```
Sample_UnbatchingNode/
├── Models.cs                                    # Data models for market data, analytics, and alerts
├── Program.cs                                   # Entry point with pipeline execution
├── UnbatchingStreamConversionPipeline.cs        # Main pipeline definition with unbatching flow
├── README.md                                    # This documentation
└── Nodes/
    ├── MarketDataSource.cs                      # Generates individual market data events
    ├── BatchAnalyticsTransform.cs              # Processes batches with analytics calculations
    ├── IndividualEventProcessor.cs              # Converts batch results to individual alerts (UNBATCHING)
    └── RealTimeAlertingSink.cs                 # Processes individual alert events
```

## Key Components

### Data Models

- **MarketDataEvent**: Individual market data from trading exchanges
- **BatchAnalyticsResult**: Analytics results from batch processing
- **AlertEvent**: Individual alert events generated from batch insights

### Pipeline Nodes

#### MarketDataSource

```csharp
public class MarketDataSource : SourceNode<MarketDataEvent>
```

- Generates realistic market data from multiple exchanges
- Configurable event count and intervals
- Simulates price movements and trading volumes

#### BatchingNode

```csharp
public class BatchingNode<T> : TransformNode<T, IReadOnlyCollection<T>>
```

- Core batching functionality with size and time thresholds
- Automatically handled by `BatchingExecutionStrategy`
- Transforms individual items into collections

#### BatchAnalyticsTransform

```csharp
public class BatchAnalyticsTransform : TransformNode<IReadOnlyCollection<MarketDataEvent>, BatchAnalyticsResult>
```

- Demonstrates efficient batch processing for financial analytics
- Calculates price volatility, VWAP, trends, and anomaly scores
- Shows performance benefits of batched computations

#### IndividualEventProcessor

```csharp
public class IndividualEventProcessor : TransformNode<BatchAnalyticsResult, AlertEvent>
```

- **KEY UNBATCHING COMPONENT**: Converts batch results back to individual events
- Generates alerts based on batch analytics insights
- Maintains traceability between batch results and original events

#### RealTimeAlertingSink

```csharp
public class RealTimeAlertingSink : SinkNode<AlertEvent>
```

- Processes individual alert events for real-time monitoring
- Implements rate limiting to prevent alert fatigue
- Provides comprehensive alert statistics and analysis

## Configuration Options

The `UnbatchingStreamConversionPipeline` accepts several configuration parameters:

```csharp
var pipeline = new UnbatchingStreamConversionPipeline(
    batchSize: 15,                           // Maximum events per batch
    batchTimeout: TimeSpan.FromSeconds(2),   // Maximum time before emitting batch
    marketDataEventCount: 100,              // Number of market data events to generate
    marketDataInterval: TimeSpan.FromMilliseconds(50),  // Interval between events
    priceAnomalyThreshold: 2.0m,            // Price anomaly detection threshold
    volatilityThreshold: 5.0m,              // Volatility alert threshold
    anomalyScoreThreshold: 0.7              // Anomaly score alert threshold
);
```

## Performance Analysis

The sample provides detailed performance analysis showing:

- **Batch Processing Time**: Time spent on analytics calculations
- **Unbatching Time**: Time spent converting batch results to individual events
- **Alert Processing Time**: Time spent processing individual alerts
- **Efficiency Gains**: Benefits of batch processing vs individual processing

Example output:

```
=== UNBATCHING PERFORMANCE ANALYSIS ===
Total batch processing time: 245ms
Total unbatching time: 89ms
Total alert processing time: 156ms
Batches processed: 7
Individual alerts generated: 23
Average time per batch: 35.0ms
Average time per alert: 6.8ms
Efficiency gain from batching: 67% compared to individual processing
```

## Unbatching Strategies in Practice

### When to Use Unbatching

- **Analytics + Real-time Processing**: When you need both efficient analytics and real-time event processing
- **Cost Optimization**: When batch processing is significantly cheaper than individual processing
- **Insight Distribution**: When batch insights need to be applied to individual events
- **Hybrid Systems**: When different parts of your system require different data formats

### Unbatching Patterns

1. **Batch-to-Individual**: Convert batch results to individual events for downstream processing
2. **Insight Application**: Apply batch-level insights to individual items
3. **Alert Generation**: Generate individual alerts based on batch analytics
4. **Traceability**: Maintain links between batch results and original events

## Testing Unbatched Pipelines

The sample demonstrates comprehensive testing patterns for unbatched pipelines:

```csharp
var result = await new PipelineTestHarness<UnbatchingStreamConversionPipeline>()
    .WithParameter("BatchSize", 10)
    .WithParameter("BatchTimeout", TimeSpan.FromSeconds(1))
    .WithParameter("MarketDataEventCount", 50)
    .WithParameter("PriceAnomalyThreshold", 1.5m)
    .CaptureErrors()
    .RunAsync();

result.Success.Should().BeTrue();
result.Duration.Should().BeLessThan(TimeSpan.FromSeconds(15));
```

### Test Scenarios Covered

- Different batch sizes and unbatching configurations
- Various alert thresholds and sensitivity levels
- Empty input handling
- Single event processing
- Cancellation scenarios
- Error handling and recovery

## Best Practices

1. **Choose Appropriate Batch Sizes**: Balance processing efficiency with real-time requirements
2. **Set Reasonable Timeouts**: Ensure timely processing even with low data flow
3. **Maintain Traceability**: Keep links between batch results and original events
4. **Implement Rate Limiting**: Prevent alert fatigue in real-time processing
5. **Monitor Performance**: Track both batch and individual processing metrics
6. **Test Thoroughly**: Verify behavior with various data flow patterns

## Common Use Cases

- **Financial Trading**: Market data analytics with real-time alerting
- **IoT Monitoring**: Sensor data analytics with individual device alerts
- **Log Processing**: Batch log analysis with individual event alerts
- **Fraud Detection**: Batch pattern analysis with individual transaction alerts
- **Quality Control**: Batch quality metrics with individual item alerts

## Error Handling

The sample demonstrates error handling in unbatched operations:

- Graceful handling of empty batches
- Simulation of processing failures
- Error recovery and reporting
- Partial batch processing
- Alert generation for error conditions

## Extending the Sample

You can extend this sample by:

1. **Adding Custom Unbatching Strategies**: Implement custom logic for converting batch results
2. **Different Data Sources**: Add support for various input formats and systems
3. **Advanced Analytics**: Implement more sophisticated batch analytics algorithms
4. **Real-time Integrations**: Connect to external alerting and monitoring systems
5. **Performance Monitoring**: Add detailed metrics and observability
6. **Machine Learning**: Integrate ML models for anomaly detection and prediction

## Conclusion

This sample provides a comprehensive demonstration of unbatching functionality in NPipeline. It shows how to combine the efficiency of batch processing with the responsiveness of individual event processing.

The key takeaway is that unbatching is a powerful pattern that enables you to:
- Process data efficiently in batches for analytics
- Convert batch results back to individual events when needed
- Maintain real-time capabilities while leveraging batch processing benefits
- Build hybrid systems that optimize for both efficiency and responsiveness

This pattern is particularly valuable in scenarios where you need the cost-effectiveness of batch processing combined with the immediacy of real-time event processing.