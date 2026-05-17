---
title: "OpenTelemetry"
description: "Distributed tracing for pipelines via OpenTelemetry ActivitySource integration."
order: 7
---

# OpenTelemetry

The `NPipeline.Extensions.Observability.OpenTelemetry` package provides distributed tracing for NPipeline using [OpenTelemetry](https://opentelemetry.io/). It wraps `System.Diagnostics.ActivitySource` to emit traces that can be exported to Jaeger, Zipkin, Azure Monitor, AWS X-Ray, and any OpenTelemetry-compatible backend.

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability.OpenTelemetry
```

**Dependencies:** [OpenTelemetry](https://www.nuget.org/packages/OpenTelemetry) 1.x, `NPipeline.Extensions.Observability`

## Quick Start

```csharp
// 1. Register the tracer
services.AddNPipelineObservability();
services.AddOpenTelemetryPipelineTracer("MyService");

// 2. Configure the OpenTelemetry SDK
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddNPipelineSource("MyService")
    .AddJaegerExporter()
    .Build();
```

## Key Types

### OpenTelemetryPipelineTracer

Implements `IPipelineTracer`. Creates `Activity` instances that map to OpenTelemetry spans.

```csharp
var tracer = new OpenTelemetryPipelineTracer("MyService");

// Start a trace (automatically establishes parent-child relationships)
using var activity = tracer.StartActivity("ProcessOrders");
activity.SetTag("pipeline.batch_size", 1000);
```

Each pipeline run creates a root activity. Node executions create child activities, forming a trace tree.

### PipelineActivity

Sealed wrapper around `System.Diagnostics.Activity` that implements `IPipelineActivity`:

- `SetTag(key, value)` - add structured metadata
- `RecordException(exception)` - record errors on the span

### TracerProviderBuilder Extensions

```csharp
// Register a single pipeline source
builder.AddNPipelineSource("MyService");

// Register multiple pipeline sources
builder.AddNPipelineSources("OrderService", "InventoryService", "ShippingService");
```

### Activity Inspection

```csharp
// Extract NPipeline metadata from an Activity
var info = activity.GetNPipelineInfo();
// Returns: NPipelineActivityInfo? { PipelineId, NodeId, ... }
```

## Trace Structure

A typical trace tree:

```
MyService: OrderPipeline
├── csv-source (SourceNode)
├── transform (TransformNode)
│   ├── retry-1 (if retries occurred)
│   └── retry-2
└── db-sink (SinkNode)
```

### Activity Hierarchy

| Level | Activity | Tags |
|-------|----------|------|
| Root | Pipeline run | `pipeline.name`, `pipeline.id`, `pipeline.run_id` |
| Child | Node execution | `node.id`, `node.type`, `node.kind` |
| Grandchild | Retry attempt | `retry.attempt`, `retry.reason` |

### Standard Tags

| Tag | Description | Example |
|-----|-------------|---------|
| `pipeline.name` | Pipeline definition name | `"OrderPipeline"` |
| `pipeline.id` | Pipeline type identifier | `"order-pipeline"` |
| `pipeline.run_id` | Unique run GUID | `"a1b2c3d4-..."` |
| `node.id` | Node identifier | `"csv-source"` |
| `node.type` | Node CLR type name | `"CsvSourceNode"` |
| `node.kind` | Node kind (Source/Transform/Sink) | `"Transform"` |
| `node.items_processed` | Items consumed | `1000` |
| `node.items_emitted` | Items produced | `950` |
| `node.duration_ms` | Execution time | `1234.5` |
| `otel.status_code` | `OK` or `ERROR` | `"OK"` |

## Exporter Examples

### Jaeger

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddNPipelineSource("MyService")
    .AddJaegerExporter(o => o.AgentHost = "localhost")
    .Build();
```

### Zipkin

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddNPipelineSource("MyService")
    .AddZipkinExporter(o => o.Endpoint = new Uri("http://localhost:9411/api/v2/spans"))
    .Build();
```

### Azure Monitor / Application Insights

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddNPipelineSource("MyService")
    .AddAzureMonitorTraceExporter(o =>
        o.ConnectionString = "InstrumentationKey=...")
    .Build();
```

### AWS X-Ray

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddNPipelineSource("MyService")
    .AddXRayTraceId()
    .AddOtlpExporter()
    .Build();
```

### Console (Development)

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddNPipelineSource("MyService")
    .AddConsoleExporter()
    .Build();
```

## Production Configuration

### Sampling

In production, sample traces to reduce overhead:

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddNPipelineSource("MyService")
    .SetSampler(new TraceIdRatioBasedSampler(0.1)) // 10% of traces
    .AddOtlpExporter(o => o.Endpoint = new Uri("http://collector:4317"))
    .Build();
```

### Batch Export

```csharp
using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .AddNPipelineSource("MyService")
    .AddOtlpExporter(o =>
    {
        o.Endpoint = new Uri("http://collector:4317");
        o.ExportProcessorType = ExportProcessorType.Batch;
        o.BatchExportProcessorOptions = new BatchExportProcessorOptions<Activity>
        {
            MaxQueueSize = 2048,
            ScheduledDelayMilliseconds = 5000,
            MaxExportBatchSize = 512
        };
    })
    .Build();
```

### Multi-Service Setup

When multiple services each run NPipeline, register separate sources per service:

```csharp
// Service A
services.AddOpenTelemetryPipelineTracer("OrderService");

// Service B
services.AddOpenTelemetryPipelineTracer("InventoryService");
```

Then register all sources in the SDK:

```csharp
builder.AddNPipelineSources("OrderService", "InventoryService");
```

Parent-child relationships are preserved across service boundaries via W3C trace context propagation.

## Troubleshooting

| Issue | Resolution |
|-------|------------|
| No traces appearing | Verify service name matches between `AddOpenTelemetryPipelineTracer` and `AddNPipelineSource` |
| Missing node spans | Ensure `AddNPipelineObservability()` is registered before the tracer |
| Exporter connection errors | Check endpoint URL and network connectivity |
| Too many traces in production | Add `SetSampler` with `TraceIdRatioBasedSampler` |
| Large trace payloads | Use batch export with appropriate queue/batch sizes |
| Activities not correlated | Verify `Activity.Current` is not null - avoid `Task.Run` without flow |

### Debug Logging

Enable OpenTelemetry internal logging to diagnose issues:

```csharp
OpenTelemetrySdk.SetDefaultTextMapPropagator(new CompositeTextMapPropagator(new[]
{
    new TraceContextPropagator(),
    new BaggagePropagator()
}));

// Enable self-diagnostics
Environment.SetEnvironmentVariable("OTEL_DIAGNOSTICS_ENABLED", "true");
```

## See Also

- [Observability](observability.md) - metrics collection
- [OpenTelemetry Guide](../observability/opentelemetry.md) - step-by-step walkthrough
- [Extensions Overview](index.md)
- [Extensions Overview](index.md)
