---
title: "Metrics and Monitoring"
description: "Collect node and pipeline metrics with built-in logging sinks or custom metric exporters."
order: 2
---

# Metrics and Monitoring

> **Prerequisites:** [Defining Pipelines](../guides/defining-pipelines.md), [Dependency Injection](../guides/dependency-injection.md)

The `NPipeline.Extensions.Observability` package collects timing, throughput, and optional memory metrics per node and per pipeline run.

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability
```

## Quick Setup

```csharp
services.AddNPipeline(Assembly.GetExecutingAssembly());
services.AddNPipelineObservability(); // logs metrics via ILogger
```

This registers the default sinks: `LoggingMetricsSink` (per-node metrics) and `LoggingPipelineMetricsSink` (per-pipeline summary).

## What Gets Collected

### Node Metrics

For each node, the `MetricsCollectingExecutionObserver` records:

| Metric | Description |
|--------|-------------|
| Start/end timestamps | Node timing window (for lazy stream nodes with `WithObservability`, end is finalized at dataflow completion) |
| Items processed/emitted | Count of input and output items |
| Throughput (items/sec) | Processing rate |
| Average item processing time | Mean time per item in milliseconds |
| Retry count | Number of retries (if resilience is enabled) |
| Processor time | CPU time consumed |
| Peak memory (optional) | Memory at node boundaries |

### Execution vs Dataflow Completion

For stream-heavy pipelines, node setup can complete before real work finishes. Observability now distinguishes:

- `OnNodeCompleted` - setup/execution delegate returned.
- `OnNodeDataflowCompleted` - downstream enumeration finished and stream scope disposed.

When dataflow completion is available, node duration and derived performance metrics are finalized from that later event so stream runtimes are attributed accurately.

### Pipeline Metrics

After each run, a `IPipelineMetrics` summary is emitted:

- Pipeline name, ID, and run ID
- Start time, end time, and duration
- Success/failure status
- Exception details (on failure)
- Per-node metric breakdown

## Per-Node Observability

Enable metrics collection on specific nodes:

```csharp
var transform = builder.AddTransform<MyTransform, In, Out>("transform");
transform.WithObservability(builder);  // default options
transform.WithObservability(builder, ObservabilityOptions.Full);  // full metrics
```

## Memory Metrics

Memory tracking is disabled by default to avoid overhead. Enable it when diagnosing memory issues:

```csharp
services.AddNPipelineObservability(new ObservabilityExtensionOptions
{
    EnableMemoryMetrics = true
});
```

This samples `GC.GetTotalMemory` at node start and end boundaries.

## Custom Metric Sinks

Replace the default logging sinks with your own implementations:

```csharp
services.AddNPipelineObservability<PrometheusMetricsSink, GrafanaPipelineMetricsSink>();
```

Or use a factory:

```csharp
services.AddNPipelineObservability(
    sp => new PrometheusMetricsSink(sp.GetRequiredService<IMeterFactory>()),
    sp => new GrafanaPipelineMetricsSink(sp.GetRequiredService<ILogger<GrafanaPipelineMetricsSink>>()));
```

Implement `IMetricsSink` for per-node metrics and `IPipelineMetricsSink` for pipeline-level summaries.

## Next Steps

- [Observability Extension Reference](../extensions/observability.md) - collector, surface, and sink details
- [OpenTelemetry Integration](opentelemetry.md) - distributed tracing
- [Data Lineage](data-lineage.md) - track data provenance
- [Pipeline Context](../guides/pipeline-context.md) - access loggers in nodes
