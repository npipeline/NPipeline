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

The simplest way to enable automatic observability is to use `IObservablePipelineContextFactory`:

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

// Run your pipeline - metrics are collected automatically and emitted to sinks!
await runner.RunAsync<MyPipeline>(context);

// Note: Metrics are automatically emitted to registered sinks when the pipeline completes.
// You can also retrieve collected metrics directly:
var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();
var nodeMetrics = collector.GetNodeMetrics();
```

### Automatic Metrics Emission

When you run a pipeline with observability enabled, metrics are automatically emitted to all registered sinks:

- **On successful completion**: All node and pipeline metrics are emitted to sinks
- **On failure**: Metrics are emitted including the exception that caused the failure
- **Sinks are invoked asynchronously**: All registered `IMetricsSink` and `IPipelineMetricsSink` implementations receive metrics

The built-in `LoggingMetricsSink` and `LoggingPipelineMetricsSink` will log metrics to your configured logger automatically.

## Core Components

### IObservablePipelineContextFactory

Factory for creating pipeline contexts with observability pre-configured. The created context
automatically has its `ExecutionObserver` set to `MetricsCollectingExecutionObserver`,
enabling automatic metrics collection during pipeline execution.

### IObservabilityCollector

Thread-safe collector for metrics during pipeline execution. Records:

- Node start/end times
- Item processing counts
- Retry attempts
- Performance metrics (throughput, memory, CPU)

**Important**: The collector automatically emits metrics to all registered sinks when the pipeline completes or fails.

### IMetricsSink

Interface for consuming node-level metrics. Implement to integrate with:

- Logging frameworks (built-in `LoggingMetricsSink`)
- Monitoring systems (Prometheus, Application Insights, etc.)
- Custom metric storage

All registered sinks are automatically invoked when metrics are emitted.

### IPipelineMetricsSink

Interface for consuming pipeline-level metrics aggregated across all nodes.

### MetricsCollectingExecutionObserver

`IExecutionObserver` implementation that automatically collects metrics from pipeline events.
This is automatically wired up when using `AddNPipelineObservability()`.

### IAutoObservabilityScope

Provides automatic metrics recording for nodes that have observability enabled. Automatically tracks:

- Items processed and emitted
- Failures and errors
- Performance metrics

This scope is automatically created when you use `.WithObservability()` on a node.

## Per-Node Observability Configuration

You can configure observability options for individual nodes using `WithObservability()` extension method:

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

**How it works**: When you use `.WithObservability()`, the system:

1. Stores the observability options in the node's execution annotations
2. Creates an `IAutoObservabilityScope` when the node starts executing
3. Automatically records item counts, failures, and performance metrics
4. Disposes the scope when the node completes, ensuring all metrics are captured

### ObservabilityOptions Presets

| Preset     | Timing | Item Counts | Memory | Thread Info | Performance |
|------------|--------|-------------|--------|-------------|-------------|
| `Default`  | ✓      | ✓           | ✗      | ✓           | ✓           |
| `Full`     | ✓      | ✓           | ✓      | ✓           | ✓           |
| `Minimal`  | ✓      | ✗           | ✗      | ✗           | ✗           |
| `Disabled` | ✗      | ✗           | ✗      | ✗           | ✗           |

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
- **ItemsProcessed/ItemsEmitted**: Item counts (automatically tracked when observability is enabled)
- **RetryCount**: Number of retry attempts (thread-safe, uses atomic operations)
- **ThroughputItemsPerSec**: Processing throughput
- **AverageItemProcessingMs**: Average time per item in milliseconds
- **PeakMemoryUsageMb**: Peak memory usage during node execution (per-node delta, not global process memory)
- **ProcessorTimeMs**: CPU time consumed (not available per-node; only available at process level)
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

### Memory Metrics Details

Memory metrics are measured as **per-node deltas** using `GC.GetTotalMemory(false)`, not global process memory:

- Each node gets its own memory usage measurement
- Memory is measured at node start and end using `GC.GetTotalMemory(false)`
- The difference (final memory - initial memory) represents the memory allocated during that specific node's execution
- This provides accurate, isolated memory tracking per node
- **Note**: This measures managed memory allocations, not total process memory or peak working set

**Important**: Memory metrics are only collected when:
1. The extension is configured with `EnableMemoryMetrics = true` (via `ObservabilityExtensionOptions`)
2. The node has `ObservabilityOptions.RecordMemoryUsage = true` set

## Best Practices

1. **Use scoped collector**: Registered as scoped to ensure isolation per pipeline run
2. **Implement async sinks**: Use `Task RecordAsync` for I/O-bound metric operations
3. **Handle failures gracefully**: Sinks should not throw exceptions that could disrupt pipelines
4. **Consider performance**: Metrics collection adds overhead; profile in production scenarios
5. **Aggregate at pipeline level**: Use `CreatePipelineMetrics` to get comprehensive run summaries
6. **Enable memory tracking selectively**: Memory metrics add overhead; use `ObservabilityOptions.Full` only when needed
7. **Trust automatic emission**: Metrics are automatically emitted to sinks on pipeline completion; no manual emission needed

## Documentation

- [Configuration Guide](../../docs/extensions/observability-configuration.md)
- [Usage Examples](../../docs/extensions/observability-examples.md)
- [Metrics Reference](../../docs/extensions/observability-metrics.md)
- [Main Observability Documentation](../../docs/extensions/observability.md)

## Thread Safety

All collector implementations are thread-safe and designed for concurrent use. The `ObservabilityCollector` uses:

- `ConcurrentDictionary` for node metrics storage
- `Interlocked` operations for counter updates
- `Interlocked.CompareExchange` loop for thread-safe retry counting (prevents lost updates under high contention)

This ensures accurate metrics collection even when multiple nodes execute concurrently or when multiple threads update the same node's metrics.

## Performance Considerations

- Metrics collection overhead is typically < 1% for most pipelines
- Memory overhead is proportional to the number of nodes (~ 1KB per node)
- Sink implementations should be async for I/O operations
- Memory metrics use `GC.GetTotalMemory(false)` which adds minimal overhead per node when enabled
- Memory metrics are only collected when both extension-level (`EnableMemoryMetrics`) and node-level (`RecordMemoryUsage`) options are enabled
- Consider sampling or filtering in high-throughput scenarios
- Disable memory tracking (`ObservabilityOptions.Default` instead of `Full`) if not needed to reduce overhead

## Troubleshooting

### Memory Metrics Not Collected

**Problem**: Memory metrics are not appearing in collected data.

**Solutions**:
1. Verify memory metrics are enabled at extension level: `services.AddNPipelineObservability(ObservabilityExtensionOptions.WithMemoryMetrics)`
2. Ensure nodes have memory tracking enabled: `.WithObservability(builder, ObservabilityOptions.Full)` or set `RecordMemoryUsage = true`
3. Memory metrics require both extension-level AND node-level configuration to be enabled

### Metrics Not Appearing

**Problem**: Metrics are not being logged or sent to external systems.

**Solutions**:
1. Verify observability is registered: `services.AddNPipelineObservability()`
2. Check that the pipeline is using `IObservablePipelineContextFactory` to create the context
3. Ensure logging is configured properly for `LoggingMetricsSink`
4. Verify sink implementations are not throwing exceptions

### Performance Degradation

**Problem**: Pipeline execution slows down when observability is enabled.

**Solutions**:
1. Use async sink implementations
2. Implement batching or aggregation for external calls
3. Disable memory tracking (`ObservabilityOptions.Default` instead of `Full`) if not needed
4. Consider sampling for high-volume scenarios

## License

MIT License - See LICENSE file in repository root
