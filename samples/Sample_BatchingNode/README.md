# Sample_BatchingNode

This sample demonstrates the `BatchingNode<T>` functionality in NPipeline for efficient batch processing of individual items. It shows how to collect items into
batches based on size, time, or hybrid strategies to improve processing efficiency.

## Overview

Batching is a crucial optimization pattern for scenarios where processing items individually is inefficient. This sample demonstrates how to use
`BatchingNode<T>` to:

- Collect individual sensor readings into batches
- Process batches efficiently with aggregations and calculations
- Perform bulk database operations
- Demonstrate performance improvements over individual processing

## Key Concepts Demonstrated

### BatchingNode<T>

The `BatchingNode<T>` is a specialized transform node that:

- Collects individual items into `IReadOnlyCollection<T>` batches
- Emits batches when either size OR time thresholds are reached
- Works with `BatchingExecutionStrategy` for optimal performance
- Changes the data flow from individual items to collections

### Batching Strategies

1. **Size-Based Batching**: Batches are created when they reach a specified item count
2. **Time-Based Batching**: Batches are created after a time window expires
3. **Hybrid Batching**: Batches are created when either size OR time threshold is reached first

### Performance Benefits

- **Computational Operations**: Aggregations and calculations are more efficient on batches
- **Database Operations**: Bulk inserts are much faster than individual inserts
- **Network Operations**: Batch API calls reduce overhead compared to individual calls

## Sample Scenario

This sample simulates an IoT sensor data processing pipeline:

1. **SensorSource**: Generates individual sensor readings from multiple devices
2. **BatchingNode**: Collects readings into batches based on configuration
3. **BatchProcessingTransform**: Processes batches with aggregations and calculations
4. **DatabaseSink**: Performs bulk database operations with batched results

## Running the Sample

### Basic Execution

```bash
dotnet run --project samples/Sample_BatchingNode
```

This will run four different batching scenarios to demonstrate various configurations:

1. **Size-Based Batching**: Batches of 10 items with long timeout
2. **Time-Based Batching**: 2-second timeout with large batch size
3. **Hybrid Batching**: 8 items OR 1.5 seconds, whichever comes first
4. **High-Frequency Data**: Small batches with rapid data generation

### Running Tests

```bash
dotnet test samples/Sample_BatchingNode
```

The comprehensive test suite covers:

- Different batching strategies
- Various batch sizes and timeouts
- Error handling scenarios
- Performance characteristics
- Concurrent execution

## Code Structure

```
Sample_BatchingNode/
├── Models.cs                           # Data models for sensor readings and results
├── Program.cs                          # Entry point with multiple scenario demonstrations
├── BatchingPipeline.cs                 # Main pipeline definition with configurable parameters
├── BatchingPipelineTests.cs            # Comprehensive test suite
├── Nodes/
│   ├── SensorSource.cs                 # Generates individual sensor readings
│   ├── BatchProcessingTransform.cs     # Processes batches efficiently
│   └── DatabaseSink.cs                 # Performs bulk database operations
└── README.md                           # This documentation
```

## Key Components

### Data Models

- **SensorReading**: Individual sensor data from IoT devices
- **BatchProcessingResult**: Aggregated results from batch processing
- **DatabaseInsertResult**: Results from bulk database operations

### Pipeline Nodes

#### SensorSource

```csharp
public class SensorSource : SourceNode<SensorReading>
```

- Generates realistic sensor data from multiple devices
- Configurable reading count and intervals
- Simulates temperature, humidity, pressure, and battery readings

#### BatchingNode

```csharp
public class BatchingNode<T> : TransformNode<T, IReadOnlyCollection<T>>
```

- Core batching functionality with size and time thresholds
- Automatically handled by `BatchingExecutionStrategy`
- Transforms individual items into collections

#### BatchProcessingTransform

```csharp
public class BatchProcessingTransform : TransformNode<IReadOnlyCollection<SensorReading>, BatchProcessingResult>
```

- Demonstrates efficient batch processing
- Calculates aggregations (averages, ranges, minimums)
- Shows performance benefits of batched computations

#### DatabaseSink

```csharp
public class DatabaseSink : SinkNode<BatchProcessingResult>
```

- Simulates bulk database operations
- Demonstrates performance improvements over individual inserts
- Provides detailed timing and success metrics

## Configuration Options

The `BatchingPipeline` accepts several configuration parameters:

```csharp
var pipeline = new BatchingPipeline(
    batchSize: 10,                    // Maximum items per batch
    batchTimeout: TimeSpan.FromSeconds(2),  // Maximum time before emitting batch
    sensorReadingCount: 50,           // Number of sensor readings to generate
    sensorInterval: TimeSpan.FromMilliseconds(100)  // Interval between readings
);
```

## Performance Analysis

The sample provides detailed performance analysis showing:

- **Processing Time**: Time spent on batch computations
- **Database Time**: Time spent on bulk operations
- **Efficiency Gains**: Comparison with individual processing
- **Success Rates**: Batch processing success metrics

Example output:

```
=== PERFORMANCE ANALYSIS ===
Total batch processing time: 245ms
Total database operation time: 180ms
Average processing time per batch: 49.0ms
Average database time per batch: 36.0ms
Average time per reading (including processing): 8.50ms
Estimated time for individual inserts: 500ms (5ms per reading)
Time saved by batching: 215ms (43.0% improvement)
```

## Batching Strategies in Practice

### When to Use Size-Based Batching

- When you have control over data flow rate
- When processing time is predictable
- When memory usage is a concern (smaller, predictable batches)

### When to Use Time-Based Batching

- When data flow is unpredictable or bursty
- When you need guaranteed processing latency
- When real-time processing is important

### When to Use Hybrid Batching

- When you want both efficiency and latency guarantees
- When data flow patterns vary over time
- When you need to balance memory usage and responsiveness

## Testing Batched Pipelines

The sample demonstrates comprehensive testing patterns for batched pipelines:

```csharp
var result = await new PipelineTestHarness<BatchingPipeline>()
    .WithParameter("BatchSize", 5)
    .WithParameter("BatchTimeout", TimeSpan.FromSeconds(1))
    .WithParameter("SensorReadingCount", 25)
    .CaptureErrors()
    .RunAsync();

result.Success.Should().BeTrue();
result.Duration.Should().BeLessThan(TimeSpan.FromSeconds(10));
```

### Test Scenarios Covered

- Different batch sizes and timeouts
- Empty input handling
- Single item processing
- Cancellation scenarios
- Concurrent execution
- Error handling

## Best Practices

1. **Choose Appropriate Batch Sizes**: Balance memory usage with processing efficiency
2. **Set Reasonable Timeouts**: Ensure timely processing even with low data flow
3. **Handle Partial Batches**: Always process the final batch even if not full
4. **Monitor Performance**: Track batch processing times and success rates
5. **Test Thoroughly**: Verify behavior with various data flow patterns

## Common Use Cases

- **IoT Data Processing**: Sensor readings from multiple devices
- **Log Processing**: Batch processing of log entries
- **Financial Transactions**: Grouping transactions for bulk processing
- **Data Analytics**: Batch computations for statistical analysis
- **ETL Operations**: Bulk data loading and transformation

## Error Handling

The sample demonstrates error handling in batched operations:

- Graceful handling of empty batches
- Simulation of database failures
- Error recovery and reporting
- Partial batch processing

## Extending the Sample

You can extend this sample by:

1. **Adding Custom Batching Strategies**: Implement custom logic for batch creation
2. **Different Data Sources**: Add support for various input formats
3. **Advanced Processing**: Implement more complex batch computations
4. **Real Database Integration**: Replace simulated operations with real database calls
5. **Monitoring**: Add metrics and observability for production use

## Conclusion

This sample provides a comprehensive demonstration of `BatchingNode<T>` functionality in NPipeline. It shows how batching can significantly improve performance
for various operations while maintaining flexibility and reliability.

The key takeaway is that batching is a powerful optimization pattern that can provide substantial performance improvements when used appropriately for your
specific use case.
