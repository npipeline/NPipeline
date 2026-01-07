# NPipeline.Extensions.Observability

Comprehensive observability and metrics collection for NPipeline pipelines.

## Features

- **Automatic metrics collection** - Node timing and lifecycle metrics are captured automatically
- **Thread-safe metrics collection** for concurrent pipeline execution
- **Node-level metrics** tracking execution time, throughput, memory usage, and more
- **Pipeline-level metrics** aggregating performance across all nodes
- **Flexible sink architecture** for logging, monitoring, or custom integrations
- **Seamless DI integration** with Microsoft.Extensions.DependencyInjection
- **Per-node observability configuration** using fluent `WithObservability()` extension methods

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability
```

## Quick Start

The simplest way to enable automatic observability is to use the `IObservablePipelineContextFactory`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;

// Register observability services - this wires everything up automatically
services.AddNPipelineObservability();

// In your pipeline execution code:
await using var scope = host.Services.CreateAsyncScope();
var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();

// Create a context with observability pre-configured
var contextFactory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();
await using var context = contextFactory.Create();

// Run your pipeline - metrics are collected automatically!
await runner.RunAsync<MyPipeline>(context);

// Retrieve collected metrics
var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();
var nodeMetrics = collector.GetNodeMetrics();
```

## Core Components

### IObservablePipelineContextFactory

Factory for creating pipeline contexts with observability pre-configured. The created context
automatically has its `ExecutionObserver` set to the `MetricsCollectingExecutionObserver`,
enabling automatic metrics collection during pipeline execution.

### IObservabilityCollector

Thread-safe collector for metrics during pipeline execution. Records:

- Node start/end times
- Item processing counts
- Retry attempts
- Performance metrics (throughput, memory, CPU)

### IMetricsSink

Interface for consuming node-level metrics. Implement to integrate with:

- Logging frameworks (built-in `LoggingMetricsSink`)
- Monitoring systems (Prometheus, Application Insights, etc.)
- Custom metric storage

### IPipelineMetricsSink

Interface for consuming pipeline-level metrics aggregated across all nodes.

### MetricsCollectingExecutionObserver

`IExecutionObserver` implementation that automatically collects metrics from pipeline events.
This is automatically wired up when using `AddNPipelineObservability()`.

## Per-Node Observability Configuration

You can configure observability options for individual nodes using the `WithObservability()` extension method:

```csharp
public class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, int>();
        
        // Configure with default options (timing, item counts, thread info, performance metrics)
        var transform = builder.AddTransform<MyTransform, int, string>()
            .WithObservability(builder);
        
        // Configure with full options (includes memory tracking)
        var sink = builder.AddSink<MySink, string>()
            .WithObservability(builder, ObservabilityOptions.Full);
        
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### ObservabilityOptions Presets

| Preset | Timing | Item Counts | Memory | Thread Info | Performance |
|--------|--------|-------------|--------|-------------|-------------|
| `Default` | ✓ | ✓ | ✗ | ✓ | ✓ |
| `Full` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `Minimal` | ✓ | ✗ | ✗ | ✗ | ✗ |
| `Disabled` | ✗ | ✗ | ✗ | ✗ | ✗ |

## Configuration Options

### Default Configuration (Logging Sinks)

```csharp
services.AddNPipelineObservability();
```

### Custom Sink Types

```csharp
services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>();
```

### Factory Delegates

```csharp
services.AddNPipelineObservability(
    sp => new CustomMetricsSink(sp.GetRequiredService<ILogger>()),
    sp => new CustomPipelineMetricsSink(sp.GetRequiredService<ILogger>()));
```

### Custom Collector

```csharp
services.AddNPipelineObservability<CustomCollector, CustomMetricsSink, CustomPipelineMetricsSink>();
```

## Metrics Collected

### Node Metrics

- **NodeId**: Unique identifier
- **StartTime/EndTime**: Execution timestamps
- **DurationMs**: Execution duration in milliseconds
- **Success**: Whether execution succeeded
- **ItemsProcessed/ItemsEmitted**: Item counts
- **RetryCount**: Number of retry attempts
- **ThroughputItemsPerSec**: Processing throughput
- **PeakMemoryUsageMb**: Peak memory usage
- **ProcessorTimeMs**: CPU time consumed
- **ThreadId**: Executing thread ID
- **Exception**: Any error that occurred

### Pipeline Metrics

- **PipelineName**: Pipeline identifier
- **RunId**: Unique run identifier (GUID)
- **StartTime/EndTime**: Pipeline execution timestamps
- **DurationMs**: Total execution duration
- **Success**: Overall pipeline success
- **TotalItemsProcessed**: Sum of all items processed
- **NodeMetrics**: Collection of node-level metrics
- **Exception**: Any pipeline-level error

## Best Practices

1. **Use scoped collector**: Registered as scoped to ensure isolation per pipeline run
2. **Implement async sinks**: Use `Task RecordAsync` for I/O-bound metric operations
3. **Handle failures gracefully**: Sinks should not throw exceptions that could disrupt pipelines
4. **Consider performance**: Metrics collection adds overhead; profile in production scenarios
5. **Aggregate at pipeline level**: Use `CreatePipelineMetrics` to get comprehensive run summaries

## Documentation

- [Configuration Guide](../../docs/extensions/observability-configuration.md)
- [Usage Examples](../../docs/extensions/observability-examples.md)
- [Metrics Reference](../../docs/extensions/observability-metrics.md)
- [Main Observability Documentation](../../docs/extensions/observability.md)

## Thread Safety

All collector implementations are thread-safe and designed for concurrent use. The `ObservabilityCollector` uses `ConcurrentDictionary` for node metrics and `Interlocked` operations for counter updates.

## Performance Considerations

- Metrics collection overhead is typically < 1% for most pipelines
- Memory overhead is proportional to the number of nodes (~ 1KB per node)
- Sink implementations should be async for I/O operations
- Consider sampling or filtering in high-throughput scenarios

## License

MIT License - See LICENSE file in repository root
