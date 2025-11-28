# NPipeline Extensions Parallelism

NPipeline Extensions Parallelism is a high-performance package that provides configurable parallel execution strategies for NPipeline pipelines. This package enables developers to implement efficient parallel processing with configurable backpressure handling, queue policies, and performance monitoring capabilities.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Extensions.Parallelism
```

## Execution Strategies

The package provides multiple execution strategies to handle different parallel processing scenarios:

### BlockingParallelStrategy

The default strategy that preserves ordering and applies end-to-end backpressure using TPL Dataflow. This strategy is ideal for scenarios requiring flow control and ordered output.

- **Preserves input ordering** in output
- **Applies backpressure** to prevent memory buildup
- **Configurable queue bounds** for input and output
- **Best for**: Ordered processing, batch jobs, scenarios where data loss is unacceptable

### DropOldestParallelStrategy

A strategy that drops the oldest items when the input queue is full, prioritizing newer data. This strategy is suitable for real-time scenarios where freshness is critical.

- **Drops oldest items** when queue is full
- **Prioritizes newer data** for processing
- **Inherently unordered** output
- **Best for**: Real-time alerts, live dashboards, scenarios where recent data is most valuable

### DropNewestParallelStrategy

A strategy that drops the newest items when the input queue is full, preserving older data. This strategy is suitable for analytics scenarios where historical data is important.

- **Drops newest items** when queue is full
- **Preserves older data** in the queue
- **Inherently unordered** output
- **Best for**: Analytics, batch processing, scenarios where data completeness is important

### ParallelExecutionStrategy

A facade strategy that automatically selects the appropriate concrete implementation based on the configured queue policy. This provides a unified interface while allowing runtime strategy selection.

## Queue Policies

The package supports different queue policies to handle backpressure when the input queue reaches its capacity:

### BoundedQueuePolicy.Block

The default policy that blocks the producer when the queue is full, providing end-to-end flow control.

```csharp
var options = new ParallelOptions(
    MaxQueueLength: 1000,        // Maximum queue length
    QueuePolicy: BoundedQueuePolicy.Block,
    OutputBufferCapacity: 500    // Optional output buffer capacity
);
```

**Performance implications:**
- Provides natural backpressure to upstream components
- Prevents memory buildup under sustained load
- May increase latency when queues are full
- Ensures no data loss

### BoundedQueuePolicy.DropOldest

Drops the oldest items when the queue is full, making room for newer items.

```csharp
var options = new ParallelOptions(
    MaxQueueLength: 1000,
    QueuePolicy: BoundedQueuePolicy.DropOldest
);
```

**Performance implications:**
- Maintains constant memory usage
- Prioritizes data freshness
- May cause data loss under sustained load
- Reduces latency for new items

### BoundedQueuePolicy.DropNewest

Drops the newest items when the queue is full, preserving older items.

```csharp
var options = new ParallelOptions(
    MaxQueueLength: 1000,
    QueuePolicy: BoundedQueuePolicy.DropNewest
);
```

**Performance implications:**
- Maintains constant memory usage
- Preserves historical data
- May cause data loss for recent items
- Reduces processing variability

## Usage Examples

### Basic Parallel Execution

```csharp
using NPipeline.Extensions.Parallelism;

// Create a pipeline with parallel execution
var pipeline = PipelineBuilder.Create()
    .AddSource<MySource, InputData>()
    .AddTransform<MyTransform, InputData, OutputData>()
    .WithBlockingParallelism(builder, maxDegreeOfParallelism: 4)
    .AddSink<MySink, OutputData>()
    .Build();

// Execute the pipeline
await pipeline.ExecuteAsync(context);
```

### Different Queue Policies

```csharp
// Drop oldest policy for real-time processing
var realtimeTransform = builder
    .AddTransform<RealtimeTransform, SensorData, AlertData>()
    .WithDropOldestParallelism(builder, 
        maxDegreeOfParallelism: 8,
        maxQueueLength: 100);

// Drop newest policy for analytics
var analyticsTransform = builder
    .AddTransform<AnalyticsTransform, EventData, MetricsData>()
    .WithDropNewestParallelism(builder,
        maxDegreeOfParallelism: 4,
        maxQueueLength: 1000);

// Custom parallel options
var customTransform = builder
    .AddTransform<CustomTransform, InputData, OutputData>()
    .WithParallelism(builder, 
        new ParallelOptions(
            MaxDegreeOfParallelism: 6,
            MaxQueueLength: 500,
            QueuePolicy: BoundedQueuePolicy.DropOldest,
            OutputBufferCapacity: 200,
            PreserveOrdering: false),
        new DropOldestParallelStrategy());
```

### Performance Monitoring

```csharp
// Access parallel execution metrics after pipeline execution
if (context.TryGetParallelMetrics("transformNodeId", out var metrics))
{
    Console.WriteLine($"Processed: {metrics.Processed}");
    Console.WriteLine($"Dropped (oldest): {metrics.DroppedOldest}");
    Console.WriteLine($"Dropped (newest): {metrics.DroppedNewest}");
    Console.WriteLine($"Retry events: {metrics.RetryEvents}");
    Console.WriteLine($"Items with retry: {metrics.ItemsWithRetry}");
    Console.WriteLine($"Max retry attempts: {metrics.MaxItemRetryAttempts}");
}
```

### Custom Parallel Options

```csharp
// Create a strategy with custom options
var strategy = ParallelExecutionStrategy.Create(new ParallelOptions(
    MaxDegreeOfParallelism: Environment.ProcessorCount * 2,
    MaxQueueLength: 10000,
    QueuePolicy: BoundedQueuePolicy.Block,
    OutputBufferCapacity: 5000,
    PreserveOrdering: true
));

// Apply to a specific node
builder.WithExecutionStrategy(transformNodeHandle, strategy);
```

## Performance Considerations

### Strategy Selection Guidelines

| Scenario | Recommended Strategy | Reason |
|----------|---------------------|--------|
| Batch processing with ordering requirements | BlockingParallelStrategy | Preserves order and prevents data loss |
| Real-time alerts with latency sensitivity | DropOldestParallelStrategy | Prioritizes newest data |
| Analytics with data completeness requirements | DropNewestParallelStrategy | Preserves historical data |
| High-throughput with controlled memory usage | Drop policies | Bounded memory usage |
| Variable load with backpressure needs | BlockingParallelStrategy | Natural flow control |

### Memory Usage Patterns

- **BlockingParallelStrategy**: Memory usage scales with queue size and processing speed differences
- **DropOldestParallelStrategy**: Constant memory usage bounded by MaxQueueLength
- **DropNewestParallelStrategy**: Constant memory usage bounded by MaxQueueLength

### CPU Utilization Optimization

```csharp
// For CPU-bound workloads
var cpuBoundOptions = new ParallelOptions(
    MaxDegreeOfParallelism: Environment.ProcessorCount,
    QueuePolicy: BoundedQueuePolicy.Block
);

// For I/O-bound workloads
var ioBoundOptions = new ParallelOptions(
    MaxDegreeOfParallelism: Environment.ProcessorCount * 4,
    QueuePolicy: BoundedQueuePolicy.DropOldest,
    MaxQueueLength: 1000
);

// For mixed workloads
var mixedOptions = new ParallelOptions(
    MaxDegreeOfParallelism: Environment.ProcessorCount * 2,
    QueuePolicy: BoundedQueuePolicy.Block,
    MaxQueueLength: 500,
    OutputBufferCapacity: 250
);
```

## Requirements

- **.NET 8.0** or later
- **NPipeline** core package
- **System.Threading.Tasks.Dataflow** (automatically included for .NET 8.0)

## License

MIT License - see LICENSE file for details.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Extensions](https://www.nuget.org/packages/NPipeline.Extensions)** - Additional pipeline components
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration
- **[NPipeline.Analyzers](https://www.nuget.org/packages/NPipeline.Analyzers)** - Roslyn analyzers for pipeline development

## Support

- **Documentation**: [https://npipeline.readthedocs.io](https://npipeline.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/npipeline/NPipeline/issues)
- **Discussions**: [GitHub Discussions](https://github.com/npipeline/NPipeline/discussions)
- **Discord**: [NPipeline Community](https://discord.gg/npipeline)