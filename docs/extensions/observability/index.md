# NPipeline Observability Extension

Comprehensive metrics collection and monitoring for NPipeline pipelines.

## Overview

The `NPipeline.Extensions.Observability` extension provides comprehensive observability capabilities for NPipeline pipelines, including metrics collection and distributed tracing. It enables detailed tracking of pipeline execution, node performance, throughput, resource utilization, and exception handling, making it easier to identify bottlenecks, optimize performance, and troubleshoot issues in production environments.

### Key Features

- **Node-level metrics**: Track execution time, items processed/emitted, retries, memory usage, and processor time for each node
- **Pipeline-level metrics**: Aggregate metrics across all nodes, including total duration, throughput, and success/failure status
- **Distributed tracing**: OpenTelemetry-compatible tracing through `PipelineActivity` and `System.Diagnostics.Activity`
- **Thread-safe collection**: Metrics are collected safely across parallel and concurrent pipeline executions
- **Flexible sinks**: Built-in logging sinks with support for custom metrics sinks (e.g., Application Insights, Prometheus, OpenTelemetry)
- **Dependency injection integration**: Seamlessly integrates with Microsoft.Extensions.DependencyInjection
- **Execution observer pattern**: Hooks into the pipeline execution lifecycle to capture metrics without modifying node logic
- **Minimal overhead**: < 1% typically, with lock-free concurrent operations and memory-efficient storage

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability
```

The extension requires:

- `NPipeline` (core package)
- `Microsoft.Extensions.DependencyInjection.Abstractions` (10.0.1 or later)
- `Microsoft.Extensions.Logging.Abstractions` (10.0.1 or later)

## Quick Start

### Basic Setup with Default Logging

The simplest way to enable observability is to use the `IObservablePipelineContextFactory`, which automatically configures metrics collection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Add observability with default logging sinks
services.AddNPipelineObservability();

// Add NPipeline core services
services.AddNPipeline(Assembly.GetExecutingAssembly());

var serviceProvider = services.BuildServiceProvider();

// Run your pipeline - metrics will be automatically collected and logged
await using var scope = serviceProvider.CreateAsyncScope();
var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
var contextFactory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();
await using var context = contextFactory.Create();
await runner.RunAsync<MyPipelineDefinition>(context);
```

### Complete Example

Here's a fully working example with a pipeline definition and nodes:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Pipeline;

public class NumberPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<NumberSource, int>();
        var transform = builder.AddTransform<DoubleTransform, int, int>();
        var sink = builder.AddSink<NumberSink, int>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public sealed class NumberSource : SourceNode<int>
{
    public IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        static async IAsyncEnumerable<int> Generate()
        {
            for (int i = 1; i <= 100; i++)
            {
                yield return i;
            }
        }

        return new StreamingDataPipe<int>(Generate());
    }
}

public sealed class DoubleTransform : TransformNode<int, int>
{
    public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(item * 2);
    }
}

public sealed class NumberSink : SinkNode<int>
{
    public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, IPipelineActivity parentActivity, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Result: {item}");
        }
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((context, services) =>
            {
                services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());
                services.AddNPipeline(typeof(Program).Assembly);
                services.AddNPipelineObservability();
            })
            .Build();

        await using var scope = host.Services.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        var contextFactory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();
        var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        // Create context with automatic observability
        await using var pipelineContext = contextFactory.Create();

        // Run pipeline
        await runner.RunAsync<NumberPipeline>(pipelineContext);

        // Display metrics
        foreach (var metric in collector.GetNodeMetrics())
        {
            Console.WriteLine($"\nNode: {metric.NodeId}");
            Console.WriteLine($"  Duration: {metric.DurationMs}ms");
            Console.WriteLine($"  Success: {metric.Success}");
        }
    }
}
```

### What Gets Logged

With the default configuration, you'll see structured logs like:

```log
[Information] Pipeline MyPipeline (RunId: 123e4567-e89b-12d3-a456-426614174000) completed successfully. Processed 1000 items in 2500ms
[Information]   Node TransformNode: Processed 1000 items, emitted 950 items in 1200ms
[Information]   Node FilterNode: Processed 950 items, emitted 800 items in 800ms
[Information]   Node SinkNode: Processed 800 items, emitted 800 items in 500ms
[Information] Overall pipeline throughput: 400.00 items/sec
```

## Per-Node Configuration

You can customize observability settings for individual nodes:

```csharp
using NPipeline.Observability;
using NPipeline.Observability.Configuration;

public class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, int>();
        
        // Configure with default options
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
| --- | --- | --- | --- | --- | --- |
| `Default` | ✓ | ✓ | ✗ | ✓ | ✓ |
| `Full` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `Minimal` | ✓ | ✗ | ✗ | ✗ | ✗ |
| `Disabled` | ✗ | ✗ | ✗ | ✗ | ✗ |

**Note on Memory Metrics**: Memory metrics require both:

1. Extension-level configuration: `services.AddNPipelineObservability(ObservabilityExtensionOptions.WithMemoryMetrics)`
2. Node-level configuration: `.WithObservability(builder, ObservabilityOptions.Full)`

## Metrics Collected

When observability is enabled, the following metrics are automatically collected:

**Node Metrics**:

- Execution timing (start, end, duration)
- Items processed and emitted
- Success/failure status
- Retry attempts
- Throughput (items per second)
- Average processing time per item
- Thread ID (if enabled)
- Memory usage delta (if enabled)

**Pipeline Metrics**:

- Total execution time
- Overall success/failure status
- Total items processed across all nodes
- Individual node metrics

## Common Use Cases

1. **Performance Monitoring** - Track execution time and throughput
2. **Error Tracking** - Monitor failures and retry patterns
3. **Resource Usage** - Track memory and CPU consumption
4. **Capacity Planning** - Analyze historical performance data
5. **Debugging** - Identify bottlenecks and optimization opportunities
6. **SLA Compliance** - Ensure pipelines meet performance requirements
7. **Alerting** - Integrate with monitoring systems for real-time alerts

## Architecture

```text
┌─────────────────────────────────────────┐
│         Pipeline Execution              │
└──────────────┬──────────────────────────┘
               │
               ├─> IExecutionObserver
               │   (MetricsCollectingExecutionObserver)
               │
               ↓
        ┌─────────────────────────┐
        │ IObservabilityCollector │
        │  (Thread-safe)          │
        └──────────┬──────────────┘
                   │
                   ├─> Node Metrics
                   └─> Pipeline Metrics
                   │
                   ↓
        ┌──────────────────────┐
        │   IMetricsSink       │
        │ IPipelineMetricsSink │
        └──────────┬───────────┘
                   │
                   ├─> Logging
                   ├─> Prometheus
                   ├─> App Insights
                   └─> Custom Sinks
```

## Performance Characteristics

### Minimal Overhead

The extension is designed for production use with minimal performance impact:

- **Non-blocking metrics collection**: Metrics are recorded asynchronously without blocking pipeline execution
- **Efficient data structures**: Uses optimized collections for metrics aggregation
- **Optional observability**: Can be disabled entirely by not registering the services
- **Scoped lifetime**: Metrics are isolated per pipeline run, preventing memory leaks

### Memory Usage

- **Per-pipeline overhead**: Approximately 1-2 KB per node for metrics storage
- **Transient sinks**: Metrics sinks are created per pipeline run and disposed after use
- **No persistent storage**: Metrics are not retained in memory beyond the pipeline execution

### CPU Impact

- **Lightweight timing**: Uses high-resolution timers with minimal CPU overhead
- **Optional performance counters**: Memory and processor time collection can be disabled if not needed
- **Batch-friendly**: Metrics collection scales efficiently with large batch sizes

## Documentation

- **[Configuration Guide](./configuration.md)** - DI setup, custom sinks, and advanced registration options
- **[Advanced Patterns](./advanced-patterns.md)** - Advanced scenarios like pre-aggregation, rate-limiting, and circuit breakers
- **[Metrics Reference](./metrics.md)** - Complete metrics documentation
- **[Examples](./examples.md)** - Real-world usage examples and common patterns
- **[Distributed Tracing](./tracing.md)** - Core tracing abstraction and building custom tracers
- **[OpenTelemetry Integration](./opentelemetry.md)** - Production-grade tracing with OpenTelemetry backends
