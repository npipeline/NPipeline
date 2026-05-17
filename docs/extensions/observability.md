---
title: "Observability"
description: "Pipeline and node metrics collection with pluggable sinks, custom collectors, and monitoring patterns."
order: 6
---

# Observability

The `NPipeline.Extensions.Observability` package provides pipeline and node-level metrics collection, execution observation, and pluggable sinks for monitoring. For distributed tracing, see the companion [OpenTelemetry](opentelemetry.md) package.

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability
```

## Quick Start

```csharp
services.AddNPipeline(builder => { ... });
services.AddNPipelineObservability();
```

This enables automatic metrics collection for every pipeline run - node execution times, throughput, retry counts, and pipeline lifecycle events.

### Using the Observable Context Factory

```csharp
var contextFactory = serviceProvider.GetRequiredService<IObservablePipelineContextFactory>();
await using var context = contextFactory.Create();
// ExecutionObserver is already attached - metrics are collected automatically
```

## Node Metrics

`INodeMetrics` captures per-node execution data:

| Property | Type | Description |
|----------|------|-------------|
| `NodeId` | `string` | Node identifier |
| `PipelineId` | `Guid` | Pipeline run ID |
| `StartTime` / `EndTime` | `DateTimeOffset?` | Execution timestamps |
| `DurationMs` | `double?` | Total execution time (ms) |
| `Success` | `bool` | Whether execution succeeded |
| `ItemsProcessed` | `long` | Items consumed |
| `ItemsEmitted` | `long` | Items produced |
| `Exception` | `Exception?` | Error, if any |
| `RetryCount` | `int` | Maximum retry attempts |
| `PeakMemoryUsageMb` | `double?` | Memory delta (optional) |
| `ProcessorTimeMs` | `double?` | CPU time (optional) |
| `ThroughputItemsPerSec` | `double?` | Items/sec |
| `AverageItemProcessingMs` | `double?` | Average time per item |
| `ThreadId` | `int?` | Thread ID |

All counters use `Interlocked` operations for thread safety.

## Pipeline Metrics

`IPipelineMetrics` captures pipeline-level data:

| Property | Type | Description |
|----------|------|-------------|
| `PipelineName` | `string` | Pipeline definition name |
| `RunId` | `Guid` | Unique execution identifier |
| `StartTime` / `EndTime` | `DateTimeOffset?` | Pipeline timestamps |
| `DurationMs` | `double?` | Total pipeline time |
| `Success` | `bool` | Overall success |
| `TotalItemsProcessed` | `long` | Sum across all nodes |
| `NodeMetrics` | `IReadOnlyList<INodeMetrics>` | Per-node breakdown |
| `Exception` | `Exception?` | Error, if any |

### Metrics Analysis

```csharp
// Find bottleneck nodes
var bottlenecks = pipelineMetrics.NodeMetrics
    .Where(m => m.DurationMs.HasValue)
    .OrderByDescending(m => m.DurationMs.Value)
    .Take(3);

// Find memory-intensive nodes
var memoryHeavy = pipelineMetrics.NodeMetrics
    .Where(m => m.PeakMemoryUsageMb.HasValue)
    .OrderByDescending(m => m.PeakMemoryUsageMb.Value)
    .Take(5);
```

## Configuration

### Options

```csharp
// Default (logging sinks, no memory metrics)
services.AddNPipelineObservability();

// Enable memory metrics (GC-based delta per node)
services.AddNPipelineObservability(ObservabilityExtensionOptions.WithMemoryMetrics);
```

`ObservabilityExtensionOptions`:

| Property | Default | Description |
|----------|---------|-------------|
| `EnableMemoryMetrics` | `false` | Track per-node memory allocation delta |

### Registration Methods

**Default (logging sinks):**

```csharp
services.AddNPipelineObservability();
```

**Custom sinks:**

```csharp
services.AddNPipelineObservability<PrometheusMetricsSink, PrometheusPipelineMetricsSink>();
```

**Factory delegates:**

```csharp
services.AddNPipelineObservability(
    sp => new PrometheusMetricsSink(sp.GetRequiredService<IMeterProvider>()),
    sp => new PrometheusPipelineMetricsSink());
```

**Custom collector:**

```csharp
services.AddNPipelineObservability<CustomCollector, LoggingMetricsSink, LoggingPipelineMetricsSink>();
```

**Custom collector with factory:**

```csharp
services.AddNPipelineObservability<LoggingMetricsSink, LoggingPipelineMetricsSink>(
    collectorFactory: sp => new CustomObservabilityCollector());
```

### Service Lifetimes

| Service | Lifetime | Rationale |
|---------|----------|-----------|
| `IObservabilityCollector` | Scoped | One instance per pipeline run |
| `IMetricsSink` | Scoped | New instance per pipeline run |
| `IPipelineMetricsSink` | Scoped | New instance per pipeline run |
| `IObservabilityFactory` | Scoped | Resolves scoped collector instances |
| `IObservabilitySurface` | Scoped | Orchestrates pipeline/node lifecycle |

## Metrics Sinks

### Built-in

| Sink | Description |
|------|-------------|
| `LoggingMetricsSink` | Logs node metrics via `ILogger` |
| `LoggingPipelineMetricsSink` | Logs pipeline metrics via `ILogger` |

### Custom Sink Example

```csharp
public sealed class ApplicationInsightsSink : IMetricsSink
{
    private readonly ITelemetryClient _client;

    public ApplicationInsightsSink(ITelemetryClient client) => _client = client;

    public Task RecordAsync(INodeMetrics metrics, CancellationToken ct)
    {
        _client.TrackEvent("NodeCompleted", new Dictionary<string, string>
        {
            ["NodeId"] = metrics.NodeId,
            ["Success"] = metrics.Success.ToString()
        }, new Dictionary<string, double>
        {
            ["DurationMs"] = metrics.DurationMs ?? 0,
            ["ItemsProcessed"] = metrics.ItemsProcessed,
            ["Throughput"] = metrics.ThroughputItemsPerSec ?? 0
        });
        return Task.CompletedTask;
    }
}
```

### Composite Sink (Multiple Destinations)

```csharp
public sealed class CompositeMetricsSink : IMetricsSink
{
    private readonly IEnumerable<IMetricsSink> _sinks;

    public CompositeMetricsSink(IEnumerable<IMetricsSink> sinks) => _sinks = sinks;

    public async Task RecordAsync(INodeMetrics metrics, CancellationToken ct)
    {
        await Task.WhenAll(_sinks.Select(s => s.RecordAsync(metrics, ct)));
    }
}
```

### Configuration-Based Sink Selection

```csharp
services.AddNPipelineObservability(
    metricsSinkFactory: sp =>
    {
        var config = sp.GetRequiredService<IConfiguration>();
        return config["Observability:SinkType"] switch
        {
            "AppInsights" => new ApplicationInsightsSink(...),
            "Prometheus" => new PrometheusSink(...),
            _ => new LoggingMetricsSink(...)
        };
    },
    pipelineMetricsSinkFactory: sp => new LoggingPipelineMetricsSink(...));
```

## Advanced Patterns

### Conditional Registration

```csharp
if (configuration.GetValue<bool>("Observability:Enabled", true))
    services.AddNPipelineObservability();
```

### Serilog Integration

```csharp
Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("logs/pipeline-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();

services.AddLogging(b => b.AddSerilog());
services.AddNPipelineObservability();
```

### Log Enrichment

```csharp
public sealed class EnrichedLoggingSink : IMetricsSink
{
    private readonly ILogger _logger;

    public Task RecordAsync(INodeMetrics metrics, CancellationToken ct)
    {
        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["NodeId"] = metrics.NodeId,
            ["Success"] = metrics.Success
        }))
        {
            _logger.LogInformation(
                "Node {NodeId}: {ItemsProcessed} items in {DurationMs}ms",
                metrics.NodeId, metrics.ItemsProcessed, metrics.DurationMs);
        }
        return Task.CompletedTask;
    }
}
```

## Best Practices

1. Use **scoped lifetimes** for collectors - one per pipeline run
2. **Handle cancellation** in async sinks
3. **Buffer writes** in custom sinks - avoid per-record I/O to external systems
4. **Batch persistence** for high-volume pipelines
5. **Use `EnableMemoryMetrics` sparingly** - GC-based measurement adds overhead

## See Also

- [OpenTelemetry](opentelemetry.md) - distributed tracing
- [Metrics & Monitoring Guide](../observability/metrics-and-monitoring.md) - step-by-step walkthrough
- [Extensions Overview](index.md)
