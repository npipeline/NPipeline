# Observability Extension - Getting Started

This guide covers the essentials of using the NPipeline Observability extension for monitoring and metrics collection.

## Overview

The Observability extension provides comprehensive metrics collection for NPipeline pipelines, including:

- Automatic execution time and throughput measurements
- Resource usage tracking (memory, CPU)
- Error and retry tracking
- Per-node and pipeline-level aggregations

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability
```

## Basic Setup

The simplest way to enable observability is to use the `IObservablePipelineContextFactory`, which automatically configures metrics collection:

### 1. Register Services

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) =>
    {
        // Register NPipeline core services
        services.AddNPipeline(Assembly.GetExecutingAssembly());

        // Register observability services with automatic metrics collection
        // This registers:
        // - IObservabilityCollector (scoped)
        // - IExecutionObserver (scoped, automatically connected to collector)
        // - IObservablePipelineContextFactory (scoped, for creating observability-enabled contexts)
        services.AddNPipelineObservability();
    })
    .Build();
```

### 2. Create a Context with Observability

```csharp
await using var scope = host.Services.CreateAsyncScope();
var contextFactory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();

// Create a context with observability pre-configured
// No need to manually wire up the execution observer!
await using var context = contextFactory.Create();
```

### 3. Run Pipeline and Collect Metrics

```csharp
var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

// Run pipeline - metrics are collected automatically
await runner.RunAsync<MyPipeline>(context);

// Get node-level metrics
var nodeMetrics = collector.GetNodeMetrics();
foreach (var metric in nodeMetrics)
{
    Console.WriteLine($"Node {metric.NodeId}: {metric.ItemsProcessed} items in {metric.DurationMs}ms");
    if (metric.ThroughputItemsPerSec.HasValue)
    {
        Console.WriteLine($"  Throughput: {metric.ThroughputItemsPerSec:F2} items/sec");
    }
}
```

## Complete Example

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

## Configuring Per-Node Observability

You can customize observability settings for individual nodes using the `WithObservability()` extension method:

```csharp
using NPipeline.Observability;
using NPipeline.Observability.Configuration;

public class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, int>();
        
        // Use default observability options
        var transform = builder.AddTransform<MyTransform, int, string>()
            .WithObservability(builder);
        
        // Use full observability options (includes memory tracking)
        var sink = builder.AddSink<MySink, string>()
            .WithObservability(builder, ObservabilityOptions.Full);
        
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}
```

### Available Options

- **`ObservabilityOptions.Default`**: Timing, item counts, thread info, performance metrics (no memory)
- **`ObservabilityOptions.Full`**: All metrics including memory usage
- **`ObservabilityOptions.Minimal`**: Timing only
- **`ObservabilityOptions.Disabled`**: No metrics for this node

## Next Steps

- **[Configuration Guide](../observability-configuration.md)**: Learn about all registration options and custom sinks
- **[Usage Examples](../observability-examples.md)**: More complete examples and advanced scenarios
- **[Metrics Reference](../observability-metrics.md)**: Detailed reference for all available metrics

    
    public PrometheusMetricsSink()
    {
        _itemsProcessed = Metrics.CreateCounter("npipeline_items_processed", "Items processed");
        _executionDuration = Metrics.CreateHistogram("npipeline_execution_duration_ms", "Execution duration");
    }
    
    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        _itemsProcessed.Inc(nodeMetrics.ItemsProcessed);
        if (nodeMetrics.DurationMs.HasValue)
        {
            _executionDuration.Observe(nodeMetrics.DurationMs.Value);
        }
        return Task.CompletedTask;
    }
}

// Register custom sink
services.AddNPipelineObservability<PrometheusMetricsSink, PrometheusMetricsSink>();
```

### Manual Metrics Recording

For scenarios where you don't use execution observers:

```csharp
var collector = new ObservabilityCollector();

// Record node execution
collector.RecordNodeStart("node1", DateTimeOffset.UtcNow, threadId: 1);
// ... do work ...
collector.RecordItemMetrics("node1", itemsProcessed: 100, itemsEmitted: 95);
collector.RecordNodeEnd("node1", DateTimeOffset.UtcNow, success: true);
collector.RecordPerformanceMetrics("node1", throughputItemsPerSec: 1000.5, averageItemProcessingMs: 1.0);

// Get metrics
var metrics = collector.GetNodeMetrics("node1");
Console.WriteLine($"Average per item: {metrics?.AverageItemProcessingMs:F2} ms");
```

### Error Tracking

Metrics automatically capture failures:

```csharp
try
{
    await runner.RunAsync<MyPipeline>(context);
}
catch (Exception ex)
{
    var pipelineMetrics = collector.CreatePipelineMetrics(
        pipelineName: "MyPipeline",
        runId: Guid.NewGuid(),
        startTime: startTime,
        endTime: DateTimeOffset.UtcNow,
        success: false,
        exception: ex);
    
    // Failed nodes will have Success = false and Exception populated
    var failedNodes = pipelineMetrics.NodeMetrics.Where(m => !m.Success);
}
```

### Retry Tracking

Retries are automatically tracked by the observer:

```csharp
// Metrics will show retry count
var metrics = collector.GetNodeMetrics("retrying-node");
Console.WriteLine($"Node required {metrics.RetryCount} retry attempts");
```

## Performance Tips

1. **Scoped collector**: Always use scoped collector to avoid memory leaks across multiple pipeline runs
2. **Async sinks**: Implement sinks as async to avoid blocking pipeline execution
3. **Sampling**: For high-throughput pipelines, consider implementing sampling in custom sinks
4. **Memory**: Collector memory usage is proportional to number of nodes (typically ~1KB per node)

## Next Steps

- [Advanced Configuration](../../docs/extensions/observability-configuration.md) - Custom collectors, sinks, and factory delegates
- [Metrics Reference](../../docs/extensions/observability-metrics.md) - Complete metrics documentation
- [Examples](../../docs/extensions/observability-examples.md) - Real-world usage patterns
- [Main Documentation](../../docs/extensions/observability.md) - Comprehensive observability guide

## Troubleshooting

### Metrics Not Collected

1. Verify observer is added to `ExecutionObservers` in context
2. Ensure observability services are registered: `services.AddNPipelineObservability()`
3. Check that collector is resolved from DI correctly

### Missing Node Metrics

1. Node may not have started (check `RecordNodeStart` was called)
2. Use `GetNodeMetrics()` to see all collected nodes
3. Verify node ID matches between start and end calls

### Performance Issues

1. Review sink implementations for blocking I/O
2. Consider batching or async operations in custom sinks
3. Profile metrics collection overhead in production scenarios

## Support

For issues, questions, or contributions, visit the [NPipeline repository](https://github.com/NPipeline/NPipeline).
