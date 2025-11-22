# Time-Windowed Join Node Sample

This sample demonstrates NPipeline's `TimeWindowedJoinNode` functionality for joining data streams based on time windows, a key technique for real-time and
event-driven data processing.

## Overview

The sample showcases how to correlate IoT sensor readings with maintenance events that occur within specific time windows, demonstrating advanced temporal data
processing patterns commonly used in IoT monitoring, financial systems, and real-time analytics.

## Key Concepts Demonstrated

### Time-Windowed Join Node

- **TimeWindowedJoinNode**: Joins streams based on both keys and temporal proximity
- **Window Strategies**: Tumbling and sliding windows with configurable sizes
- **Timestamp Extraction**: Custom timestamp extractors for event-time processing
- **Watermark Management**: Automatic handling of late-arriving data and out-of-order events
- **Memory Efficiency**: State management with automatic cleanup of expired windows

### Window Types

- **Tumbling Windows**: Fixed-size, non-overlapping time intervals
    - Example: 2-minute windows starting at 00:00, 00:02, 00:04, etc.
    - Use case: Analyzing maintenance impact on discrete time periods

- **Sliding Windows**: Fixed-size windows that slide by a specified interval
    - Example: 2-minute windows sliding every 30 seconds
    - Use case: Continuous monitoring with overlapping analysis periods

### Advanced Features

- **Out-of-Order Handling**: Configurable tolerance for late-arriving events
- **Watermark Generation**: Automatic progress tracking for event time
- **Join Types**: Support for Inner, LeftOuter, RightOuter, and FullOuter joins
- **Performance Metrics**: Built-in monitoring of waiting lists and memory usage

## Pipeline Architecture

```
┌─────────────────┐    ┌──────────────────┐
│ SensorReading   │    │ MaintenanceEvent │
│ Source          │    │ Source           │
└─────────┬───────┘    └─────────┬────────┘
          │                      │
          │                      │
          ▼                      ▼
┌─────────────────────────────────────────────────────────┐
│         SensorMaintenanceJoinNode           │
│    (TimeWindowedJoinNode)                │
│  - Correlates by DeviceId               │
│  - Matches within time windows           │
└─────────┬─────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────┐
│     SensorDataEnrichmentTransform            │
│    - Calculates temperature changes           │
│    - Assesses maintenance impact           │
└─────────┬─────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────┐
│  MaintenanceEffectivenessAggregator             │
│    - Groups by device ID                   │
│    - Generates effectiveness reports        │
└─────┬───────────┬───────────┬───────────┘
      │           │           │
      ▼           ▼           ▼
┌─────────┐ ┌─────────┐ ┌─────────────────┐
│ Join    │ │Enriched │ │ Effectiveness   │
│ Sink     │ │Sink      │ │ Sink           │
└─────────┘ └─────────┘ └─────────────────┘
```

## Data Models

### Core Entities

- **SensorReading**: IoT sensor data with temperature, humidity, and timestamps
- **MaintenanceEvent**: Maintenance activities performed on devices
- **SensorMaintenanceJoin**: Result of correlating sensors with maintenance within time windows
- **EnrichedSensorData**: Enhanced data with temperature change analysis and impact assessment
- **MaintenanceEffectivenessReport**: Aggregated performance metrics by device

### Key Features

- **Timestamp Support**: All models include proper timestamp fields for event-time processing
- **Device Correlation**: Join operations based on DeviceId foreign key relationships
- **Impact Analysis**: Automatic calculation of maintenance effectiveness metrics
- **Window Metadata**: Detailed tracking of time window boundaries and temporal relationships

## Running the Sample

### Prerequisites

- .NET 8.0 or later
- NPipeline framework

### Execution

The sample demonstrates three different window configurations:

1. **Tumbling Windows (2 minutes)**
   ```bash
   dotnet run --project Sample_14_TimeWindowedJoinNode.csproj
   ```

2. **Sliding Windows (2 minutes, sliding every 30 seconds)**
    - Demonstrates overlapping window analysis

3. **Sliding Windows (1 minute, sliding every 15 seconds)**
    - Shows higher-resolution temporal analysis

Each configuration runs sequentially, allowing you to compare the different behaviors and outputs.

### Expected Output

The sample outputs three types of results:

1. **Join Results**: Raw sensor-maintenance correlations within time windows
2. **Enriched Data**: Temperature analysis and maintenance impact assessments
3. **Effectiveness Reports**: Aggregated performance metrics by device

## Performance Considerations

### Memory Usage

- **Window Size**: Larger windows require more memory for state management
- **Event Rate**: Higher event rates increase memory pressure
- **Out-of-Order Tolerance**: Larger tolerances keep more items in memory

### Latency vs. Accuracy

- **Smaller Windows**: Faster results but may miss correlations
- **Larger Windows**: Better correlation detection but increased latency
- **Watermark Interval**: Affects cleanup frequency and memory usage

### Optimization Tips

```csharp
// Configure appropriate window sizes for your use case
windowAssigner: WindowAssigner.Tumbling(TimeSpan.FromMinutes(2))

// Set reasonable out-of-orderness tolerance
maxOutOfOrderness: TimeSpan.FromSeconds(30)

// Adjust watermark interval for cleanup frequency
watermarkInterval: TimeSpan.FromSeconds(10)
```

## Real-World Applications

### IoT Monitoring

- Correlate sensor readings with maintenance events
- Detect equipment performance changes after servicing
- Monitor environmental conditions with calibration events

### Financial Systems

- Match transactions with market data events
- Correlate trades with price movements within time windows
- Detect market manipulation patterns

### Log Analysis

- Join error logs with system events
- Correlate user activities with backend operations
- Analyze performance metrics with deployment events

### E-commerce

- Match user clicks with ad impressions
- Correlate purchases with promotional events
- Analyze shopping behavior with inventory changes

## Advanced Configuration

### Custom Timestamp Extraction

```csharp
// Extract timestamps from complex data structures
timestampExtractor1: new TimestampExtractor<SensorReading>(reading =>
{
    // Custom timestamp logic
    return reading.EventTime ?? reading.ProcessedTime;
})
```

### Join Type Selection

```csharp
// Configure different join behaviors
JoinType.Inner     // Only matching pairs
JoinType.LeftOuter  // All sensors with placeholder maintenance
JoinType.RightOuter // All maintenance with placeholder sensors
JoinType.FullOuter  // All items with appropriate placeholders
```

### Window Strategy Selection

```csharp
// Tumbling windows for discrete time periods
WindowAssigner.Tumbling(TimeSpan.FromMinutes(5))

// Sliding windows for continuous analysis
WindowAssigner.Sliding(TimeSpan.FromMinutes(5), TimeSpan.FromSeconds(30))
```

## Troubleshooting

### Common Issues

1. **No Join Results**
    - Check timestamp extraction logic
    - Verify window size is appropriate for data timing
    - Ensure key selectors match correctly

2. **High Memory Usage**
    - Reduce window size
    - Decrease out-of-orderness tolerance
    - Increase watermark interval for more frequent cleanup

3. **Late Data Loss**
    - Increase maxOutOfOrderness tolerance
    - Check for clock synchronization issues
    - Verify timestamp accuracy in source data

### Debugging Tips

- Enable debug logging to see window assignments
- Monitor join node state metrics
- Check watermark progression in logs
- Verify timestamp extraction with test data

## Extending the Sample

### Adding New Window Types

Implement custom `WindowAssigner` for specialized windowing strategies:

```csharp
public class CustomWindowAssigner : WindowAssigner
{
    public override IEnumerable<IWindow> AssignWindows<T>(T item, DateTimeOffset timestamp, TimestampExtractor<T>? extractor = null)
    {
        // Custom window assignment logic
        yield return new CustomWindow(start, end);
    }
}
```

### Adding New Analysis

Extend the enrichment transform to include domain-specific metrics:

```csharp
public class CustomAnalysisTransform : ITransformNode<SensorMaintenanceJoin, CustomResult>
{
    // Custom analysis logic
}
```

## Best Practices

1. **Window Size Selection**: Choose window sizes based on business requirements and data characteristics
2. **Timestamp Accuracy**: Ensure reliable timestamp extraction from source data
3. **Memory Monitoring**: Monitor join node metrics for optimal performance
4. **Error Handling**: Implement proper error handling for out-of-order and late data
5. **Testing**: Test with realistic data patterns including edge cases

## Conclusion

This sample demonstrates production-ready patterns for time-windowed joins in NPipeline, providing a foundation for building sophisticated real-time data
processing applications. The combination of flexible windowing strategies, robust timestamp handling, and comprehensive analysis capabilities makes it suitable
for a wide range of enterprise use cases.

For more information on NPipeline's advanced features, see the [NPipeline Documentation](../../docs/README.md).
