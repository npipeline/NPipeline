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

You can customize observability settings for individual nodes using the [`WithObservability()`](../../src/NPipeline.Extensions.Observability/ObservabilityConfigurationExtensions.cs:56) extension method:

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

### Available Options

- **[`ObservabilityOptions.Default`](../../src/NPipeline.Extensions.Observability/Configuration/ObservabilityOptions.cs:87)**: Timing, item counts, thread info, performance metrics (no memory)
- **[`ObservabilityOptions.Full`](../../src/NPipeline.Extensions.Observability/Configuration/ObservabilityOptions.cs:92)**: All metrics including memory usage
- **[`ObservabilityOptions.Minimal`](../../src/NPipeline.Extensions.Observability/Configuration/ObservabilityOptions.cs:104)**: Timing only
- **[`ObservabilityOptions.Disabled`](../../src/NPipeline.Extensions.Observability/Configuration/ObservabilityOptions.cs:116)**: No metrics for this node

**Important**: Memory metrics require both:
1. Extension-level configuration: `services.AddNPipelineObservability(ObservabilityExtensionOptions.WithMemoryMetrics)`
2. Node-level configuration: `.WithObservability(builder, ObservabilityOptions.Full)` or set `RecordMemoryUsage = true`

If either level is disabled, memory metrics will not be collected.

### Metrics Collected

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

## Next Steps

- **[Configuration Guide](./configuration.md)**: Learn about all registration options and custom sinks
- **[Usage Examples](./examples.md)**: More complete examples and advanced scenarios
- **[Metrics Reference](./metrics.md)**: Detailed reference for all available metrics
