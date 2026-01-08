# Observability Extension Sample

## Overview

This sample demonstrates the comprehensive observability features of the **NPipeline.Extensions.Observability** extension. It showcases how to collect, track,
and analyze detailed metrics about pipeline execution across multiple stages.

## What This Sample Demonstrates

### Core Concepts

1. **Metrics Collection**: Automatic collection of execution metrics at each node
2. **Performance Tracking**: Duration, throughput, and item processing times
3. **Data Flow Analysis**: Understanding item counts at each stage (processed vs. emitted)
4. **Thread Information**: Tracking which threads execute each node
5. **Resource Usage**: Monitoring memory consumption during execution
6. **Error Handling**: Recording failures and exceptions

### Pipeline Structure

The sample implements a numeric data processing pipeline with four stages:

```
NumberGenerator → NumberFilter → NumberMultiplier → ResultAggregator
     (Source)       (Transform)     (Transform)         (Sink)
```

**Data Flow Example:**

- **Input**: Numbers 1-100
- **After Filter**: Odd numbers only (1, 3, 5, ..., 99) = 50 items
- **After Multiplier**: Each odd number × 3 (3, 9, 15, ..., 297) = 50 items
- **Final Output**: Aggregated results with statistics

## Key Features Demonstrated

### 1. Node-Level Metrics

Each node automatically records:

- **Duration**: Total execution time in milliseconds
- **Items Processed**: Total items received
- **Items Emitted**: Total items sent to the next node
- **Success Status**: Whether the node completed successfully
- **Thread ID**: Which thread executed the node

### 2. Performance Metrics

Nodes can record:

- **Throughput**: Items per second
- **Average Processing Time**: Milliseconds per item
- **Retry Attempts**: Number of retries (if using retry policies)

### 3. Resource Metrics

Optional tracking of:

- **Peak Memory Usage**: Maximum memory consumed during execution
- **Processor Time**: CPU time used by the node
- **Initial Memory**: Memory state at node start

### 4. Difference Between Processed and Emitted

The `NumberFilter` node demonstrates filtering:

- **Processed**: 100 items (all input numbers)
- **Emitted**: 50 items (odd numbers only)

This difference is crucial for understanding data flow and filter effectiveness.

## Running the Sample

### Build and Run

```bash
# From the project root
dotnet build samples/Sample_ObservabilityExtension/Sample_ObservabilityExtension.csproj

# Run the sample
dotnet run --project samples/Sample_ObservabilityExtension/Sample_ObservabilityExtension.csproj
```

### Expected Output

```
=== NPipeline Sample: Observability Extension ===

Registered NPipeline services with observability extension.

Pipeline Description:
[Pipeline details...]

Starting pipeline execution with metrics collection...

[NumberGenerator] Initializing number generation (1-100)
[NumberFilter] Passed: 1
[NumberFilter] Passed: 3
[NumberFilter] Passed: 5
...
[NumberMultiplier] 1 × 3 = 3
[NumberMultiplier] 3 × 3 = 9
...
[ResultAggregator] Item 1: 3
[ResultAggregator] Item 2: 9
...
[ResultAggregator] === AGGREGATION RESULTS ===
  Total items received: 50
  Sum: 7575
  Average: 151.50
  Min: 3
  Max: 297

=== EXECUTION SUMMARY ===
Total Execution Time: [XXX]ms

=== NODE METRICS ===

Node: number-generator
  Duration: [XXX]ms
  Items Processed: 100
  Items Emitted: 100
  Success: True
  Throughput: 100000.00 items/sec
  Avg Processing Time: 0.01ms per item

Node: number-filter
  Duration: [XXX]ms
  Items Processed: 100
  Items Emitted: 50
  Success: True
  Throughput: 20000.00 items/sec
  Avg Processing Time: 0.05ms per item

Node: number-multiplier
  Duration: [XXX]ms
  Items Processed: 50
  Items Emitted: 50
  Success: True
  Throughput: 10000.00 items/sec
  Avg Processing Time: 0.10ms per item

Node: result-aggregator
  Duration: [XXX]ms
  Items Processed: 50
  Items Emitted: 0
  Success: True
  Throughput: 20000.00 items/sec
  Avg Processing Time: 0.05ms per item

Pipeline execution completed successfully!
```

## Code Structure

### Nodes

- **NumberGenerator.cs**: Source node generating 1-100
- **NumberFilter.cs**: Transform that filters for odd numbers
- **NumberMultiplier.cs**: Transform that multiplies by 3
- **ResultAggregator.cs**: Sink that aggregates and displays results

### Pipeline Definition

- **ObservabilityDemoPipeline.cs**: Defines the pipeline structure and connections

### Program Entry Point

- **Program.cs**: Sets up DI, runs the pipeline, and displays metrics

## Understanding the Metrics

### Duration (DurationMs)

The total wall-clock time the node spent executing, from start to end. This includes:

- Processing time
- I/O wait time
- Time waiting for downstream nodes
- Context switching overhead

### Items Processed vs. Items Emitted

- **Items Processed**: Total input items the node received
- **Items Emitted**: Total output items the node sent

These differ in:

- **Filtering nodes**: Where some items are discarded
- **Aggregating nodes**: Where multiple inputs become one output
- **Sink nodes**: Where items are consumed but not emitted downstream

### Throughput (ThroughputItemsPerSec)

Calculated as: `Items Processed / (Duration in seconds)`

Lower values may indicate:

- I/O-bound operations
- Complex processing logic
- Resource contention
- Network delays

### Average Item Processing Time (AverageItemProcessingMs)

Calculated as: `Duration in milliseconds / Items Processed`

Useful for identifying performance bottlenecks and comparing node efficiency.

## Integration Points

This sample demonstrates integration with:

1. **NPipeline Core**: Using `SourceNode`, `TransformNode`, `SinkNode`
2. **DependencyInjection**: Using `IServiceCollection` and DI patterns
3. **Observability**: Using `IObservabilityCollector` and metrics recording

## Extension Points

To customize observability for your pipelines:

### Custom Metrics Sinks

Implement `IMetricsSink` to send node metrics to your monitoring system:

```csharp
public class CustomMetricsSink : IMetricsSink
{
    public Task RecordAsync(INodeMetrics nodeMetrics)
    {
        // Send to Prometheus, Application Insights, DataDog, etc.
        return Task.CompletedTask;
    }
}

// Register
services.AddNPipelineObservability<CustomMetricsSink, LoggingPipelineMetricsSink>();
```

### Custom Pipeline Metrics Sinks

Implement `IPipelineMetricsSink` for pipeline-level metrics:

```csharp
public class CustomPipelineMetricsSink : IPipelineMetricsSink
{
    public Task RecordAsync(IPipelineMetrics pipelineMetrics)
    {
        // Process pipeline metrics
        return Task.CompletedTask;
    }
}
```

## Advanced Topics

### Thread Safety

The `ObservabilityCollector` uses `ConcurrentDictionary<string, NodeMetricsBuilder>` to safely handle concurrent node executions. This makes it compatible with:

- Parallel execution strategies
- Multi-threaded node implementations
- Concurrent data processing

See [Sample_ParallelExecution_Simplified](../Sample_ParallelExecution_Simplified/) for combining observability with parallel processing.

### Composition with Observability

When using composite pipelines ([Sample_Composition](../Sample_Composition/)), observability metrics are automatically collected for:

- Sub-pipeline nodes
- Nested node hierarchies
- Composite execution contexts

Metrics flow through the entire pipeline hierarchy, providing end-to-end visibility.

## Best Practices

1. **Record Metrics in OnNodeComplete**: Use `OnNodeComplete()` to ensure all metrics are recorded when processing finishes
2. **Track Both Directions**: Record items processed and emitted to understand filtering/expansion
3. **Use Consistent Node IDs**: Use the node's `Id` property when recording metrics
4. **Handle Null Collectors**: Always check if the collector is available before recording
5. **Monitor Resource Usage**: Include peak memory and processor time when available
6. **Analyze Throughput Trends**: Compare throughput across nodes to identify bottlenecks

## Related Samples

- [Sample_BasicPipeline](../Sample_BasicPipeline/) - Foundation for understanding pipelines
- [Sample_ParallelExecution_Simplified](../Sample_ParallelExecution_Simplified/) - Combining with parallel processing
- [Sample_Composition](../Sample_Composition/) - Hierarchical pipelines with observability
- [Sample_PerformanceOptimization](../Sample_PerformanceOptimization/) - Using metrics for tuning

## Further Reading

- [Observability Documentation](../../docs/extensions/observability.md)
- [Performance Metrics Guide](../../docs/advanced-topics/performance-metrics.md)
- [Metrics Collection API Reference](../../docs/reference/observability-api.md)
