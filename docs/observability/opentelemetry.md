---
title: "OpenTelemetry Integration"
description: "Add distributed tracing to pipelines with OpenTelemetry and standard trace exporters."
order: 3
---

# OpenTelemetry Integration

> **Prerequisites:** [Metrics and Monitoring](metrics-and-monitoring.md)

The `NPipeline.Extensions.Observability.OpenTelemetry` package connects NPipeline to the OpenTelemetry tracing ecosystem. Each pipeline run and node execution creates `Activity` spans, compatible with Jaeger, Zipkin, Azure Monitor, and other OpenTelemetry exporters.

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability.OpenTelemetry
```

## Setup

Register the pipeline tracer and add the NPipeline source to your trace provider:

```csharp
// Register the pipeline tracer
services.AddOpenTelemetryPipelineTracer("MyServiceName");

// Configure OpenTelemetry to include NPipeline traces
services.AddOpenTelemetry()
    .WithTracing(builder =>
    {
        builder
            .AddNPipelineSource("MyServiceName")
            .AddJaegerExporter();  // or any other exporter
    });
```

### Multiple Services

If you have multiple service names:

```csharp
builder.AddNPipelineSources(new[] { "OrderService", "AnalyticsService" });
```

## What Gets Traced

`OpenTelemetryPipelineTracer` implements `IPipelineTracer` and creates `Activity` instances from an `ActivitySource`:

- **Pipeline span** - wraps the entire `RunAsync` call
- **Node spans** - one per node execution, nested under the pipeline span
- **Error annotations** - exceptions are recorded on the relevant span

Spans include tags for pipeline ID, run ID, node name, and item counts.

## Using with Exporters

The NPipeline traces are standard OpenTelemetry activities. Export them to any supported backend:

```csharp
builder.WithTracing(tracing =>
{
    tracing
        .AddNPipelineSource("MyService")
        .AddJaegerExporter()           // Jaeger
        .AddZipkinExporter()            // Zipkin
        .AddAzureMonitorTraceExporter() // Azure Monitor
        .AddOtlpExporter();            // OTLP (generic)
});
```

## Next Steps

- [OpenTelemetry Extension Reference](../extensions/opentelemetry.md) - tracer, activity, and exporter details
- [Metrics and Monitoring](metrics-and-monitoring.md) - per-node metrics collection
- [Data Lineage](data-lineage.md) - item-level data provenance
