---
title: Distributed Tracing
description: OpenTelemetry-compatible distributed tracing for NPipeline pipelines using System.Diagnostics.Activity.
sidebar_position: 7
---

# Distributed Tracing

NPipeline provides two approaches for distributed tracing:

1. **Core Tracing** (this page): Build your own tracer using `PipelineActivity` wrapper
2. **OpenTelemetry Integration**: Use the dedicated `NPipeline.Extensions.Observability.OpenTelemetry` package for seamless OpenTelemetry integration (see [OpenTelemetry Integration](./opentelemetry.md))

Both approaches use `System.Diagnostics.Activity` under the hood, making them compatible with .NET's native tracing infrastructure. Choose the approach that best fits your observability needs.

## Overview

Distributed tracing enables you to track the flow of data and execution across complex pipeline topologies. This is essential for:

- **Root cause analysis**: Understand where failures occur in multi-stage pipelines
- **Performance profiling**: Identify slow stages and optimization opportunities
- **Dependency correlation**: Trace related operations across different services and pipelines
- **Observability integration**: Export traces to OpenTelemetry-compatible backends (Jaeger, Zipkin, etc.)

## What is PipelineActivity?

`PipelineActivity` is an implementation of the `IPipelineActivity` interface that bridges NPipeline's activity abstraction with `System.Diagnostics.Activity`:

```csharp
public sealed class PipelineActivity(Activity activity) : IPipelineActivity
{
    public void SetTag(string key, object value) { ... }
    public void RecordException(Exception exception) { ... }
    public void Dispose() { ... }
}
```

The class delegates all operations to the underlying `Activity`, which is part of the .NET runtime and integrates seamlessly with OpenTelemetry instrumentation.

## When to Use Which Approach?

### Use Core Tracing (PipelineActivity) If:
- You want lightweight, custom tracing without external dependencies
- You're building your own observability solution
- You need full control over how activities are created and managed
- You're exporting traces manually or using a non-standard backend
- You want to understand the fundamentals of how tracing works

### Use OpenTelemetry Integration If:
- You need production-grade distributed tracing with multiple backends
- You want to export traces to Jaeger, Zipkin, Azure Application Insights, or AWS X-Ray
- You prefer standardized telemetry configuration via OpenTelemetry SDKs
- You're integrating with existing OpenTelemetry infrastructure
- You want dependency injection convenience with minimal configuration
- You need comprehensive activity filtering and sampling options

## Basic Usage

### Creating a Custom Tracer

To use distributed tracing with NPipeline, implement `IPipelineTracer`:

```csharp
using System.Diagnostics;
using NPipeline.Extensions.Observability.Tracing;
using NPipeline.Observability.Tracing;

public class SystemDiagnosticsTracer : IPipelineTracer
{
    private readonly string _serviceName;

    public SystemDiagnosticsTracer(string serviceName = "NPipeline")
    {
        _serviceName = serviceName;
    }

    public IPipelineActivity StartActivity(string name)
    {
        var activity = new Activity($"{_serviceName}.{name}")
            .Start();

        return activity != null 
            ? new PipelineActivity(activity)
            : new NullPipelineActivity();
    }
}
```

### Registering with Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Add observability
services.AddNPipelineObservability();

// Register the custom tracer as a singleton
services.AddSingleton<IPipelineTracer>(new SystemDiagnosticsTracer("MyPipeline"));

var provider = services.BuildServiceProvider();
```

### Using the Tracer in Your Pipeline

```csharp
var context = new PipelineContext(
    PipelineContextConfiguration.WithObservability(
        tracer: provider.GetRequiredService<IPipelineTracer>()
    )
);

await runner.RunAsync<MyPipeline>(context);
```

## Tag Guidelines

Tags are arbitrary key-value pairs that provide context for the activity. Use them to record:

```csharp
activity.SetTag("node_id", "source_stage");
activity.SetTag("batch_size", 100);
activity.SetTag("throughput_items_per_sec", 1234.5);
activity.SetTag("duration_ms", 5000);
activity.SetTag("item_count", 50);
activity.SetTag("error_count", 2);
```

### Standard Tags

While tags are flexible, consider using standard names for interoperability:

| Tag Name | Type | Purpose |
|----------|------|---------|
| `node_id` | string | Identifier of the executing node |
| `node_type` | string | Type of node (e.g., "SourceNode", "TransformNode") |
| `batch_size` | integer | Number of items in the current batch |
| `item_count` | integer | Number of items processed |
| `throughput_items_per_sec` | double | Items processed per second |
| `duration_ms` | integer | Duration in milliseconds |
| `memory_mb` | integer | Memory usage in megabytes |
| `processor_time_ms` | integer | Processor time in milliseconds |

## Exception Handling

When a pipeline node fails, record the exception to provide detailed error context:

```csharp
public IPipelineActivity StartActivity(string name)
{
    var activity = new Activity($"{name}").Start();
    return new PipelineActivity(activity);
}

// In error handling:
try
{
    // Node execution
}
catch (Exception ex)
{
    activity.RecordException(ex);
    throw;
}
```

The `RecordException` method automatically:

1. Sets the activity status to `Error`
2. Records the exception type, message, and stack trace
3. Adds an "exception" event with detailed information

## OpenTelemetry Integration

To export traces to OpenTelemetry backends like Jaeger or Zipkin:

```csharp
using OpenTelemetry;
using OpenTelemetry.Trace;

var tracerProvider = new TracerProviderBuilder()
    .AddSource("NPipeline") // Match your service name
    .AddJaegerExporter()     // Or AddZipkinExporter(), etc.
    .Build();

// Your pipeline execution will now export traces to Jaeger/Zipkin
```

### Jaeger Example

```bash
# Start a local Jaeger instance
docker run -p 4317:4317 -p 16686:16686 jaegertracing/jaeger:latest

# In your application
using var tracerProvider = new TracerProviderBuilder()
    .AddSource("MyPipeline")
    .AddOtlpExporter(opt =>
    {
        opt.Endpoint = new Uri("http://localhost:4317");
    })
    .Build();
```

Then visit `http://localhost:16686` to view traces.

## Activity Hierarchies

Activities can be nested to represent hierarchical execution:

```csharp
public class HierarchicalTracer : IPipelineTracer
{
    public IPipelineActivity StartActivity(string name)
    {
        // Activities automatically parent to the current Activity
        var activity = new Activity($"Pipeline.{name}")
            .Start();

        if (activity != null)
        {
            activity.SetTag("trace_id", activity.TraceId);
            activity.SetTag("span_id", activity.SpanId);
            return new PipelineActivity(activity);
        }

        return new NullPipelineActivity();
    }
}
```

Parent-child relationships are established automatically:
- When a new activity is started, it becomes a child of `Activity.Current`
- The trace ID is propagated to all child activities
- Span IDs create the parent-child hierarchy

## Testing Activities

When testing, use `NullPipelineActivity` to avoid side effects:

```csharp
[Fact]
public void ProcessNode_ShouldCompleteSuccessfully()
{
    // Use null activity for testing
    var activity = new NullPipelineActivity();
    
    var context = new PipelineContext(
        PipelineContextConfiguration.WithObservability(
            tracer: new NullPipelineTracer()
        )
    );

    // Test your pipeline without actual tracing overhead
    await runner.RunAsync<MyPipeline>(context);
}
```

## Best Practices

1. **Use meaningful activity names**: Name activities after the node or operation they represent
2. **Tag early and often**: Add tags throughout the execution, not just at the end
3. **Record all exceptions**: Ensure error paths are traced for debugging
4. **Keep tags lightweight**: Avoid recording large objects; use summaries instead
5. **Implement the tracer pattern**: Create a reusable tracer implementation for your organization
6. **Use correlation IDs**: Include correlation or request IDs in tags for cross-service tracing
7. **Monitor trace volume**: Large pipelines with fine-grained tracing can generate significant trace data

## See Also

- [Metrics Collection](./metrics.md): Learn about performance metrics collection
- [Configuration Guide](./configuration.md): Configure observability settings
- [OpenTelemetry Documentation](https://opentelemetry.io/): Learn about OpenTelemetry standards
