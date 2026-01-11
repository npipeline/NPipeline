---
title: OpenTelemetry Integration
description: Complete guide to using NPipeline.Extensions.Observability.OpenTelemetry for distributed tracing with OpenTelemetry SDKs.
sidebar_position: 2
---

# OpenTelemetry Integration

The `NPipeline.Extensions.Observability.OpenTelemetry` extension provides seamless integration between NPipeline's tracing capabilities and OpenTelemetry SDKs, enabling you to export pipeline traces to various observability backends like Jaeger, Zipkin, and others.

## Installation

Install the NuGet package:

```bash
dotnet add package NPipeline.Extensions.Observability.OpenTelemetry
```

This package requires:

- `NPipeline` (core package)
- `NPipeline.Extensions.Observability` (observability extension)
- `OpenTelemetry` (1.12.0 or later)
- `Microsoft.Extensions.DependencyInjection.Abstractions` (10.0.1 or later)

## Quick Start

### 1. Register the Tracer

Add the OpenTelemetry pipeline tracer to your dependency injection container:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.Observability.OpenTelemetry.DependencyInjection;

var services = new ServiceCollection();

// Add core observability
services.AddNPipelineObservability();

// Add OpenTelemetry-compatible tracing
services.AddOpenTelemetryPipelineTracer("MyPipelineService");

var provider = services.BuildServiceProvider();
```

### 2. Configure OpenTelemetry Export

Set up OpenTelemetry to export traces to your chosen backend:

```csharp
using OpenTelemetry.Trace;

// Configure OpenTelemetry with Jaeger exporter
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipelineService")
    .AddJaegerExporter()
    .Build();

// Your pipeline will now export traces to Jaeger
```

### 3. Use in Your Pipeline

```csharp
var context = new PipelineContext(
    PipelineContextConfiguration.WithObservability(
        tracer: provider.GetRequiredService<IPipelineTracer>()
    )
);

await runner.RunAsync<MyPipeline>(context);
```

## Key Components

### OpenTelemetryPipelineTracer

`OpenTelemetryPipelineTracer` is an implementation of `IPipelineTracer` that creates `System.Diagnostics.Activity` instances via an `ActivitySource` whose name matches your service name:

```csharp
public sealed class OpenTelemetryPipelineTracer : IPipelineTracer
{
    public OpenTelemetryPipelineTracer(string serviceName);
    
    public IPipelineActivity? CurrentActivity { get; }
    public IPipelineActivity StartActivity(string name);
}
```

**ActivitySource integration**:

- The tracer owns an `ActivitySource` whose `Name` is the `serviceName` you pass to the constructor.
- Each call to `StartActivity(name)` starts an `Activity` from that source with `OperationName = name`.
- OpenTelemetry providers configured with `.AddSource(serviceName)` will automatically capture these activities.
- When there are no listeners or sampling drops the activity, the tracer falls back to a no-op tracer to avoid unnecessary allocations.

### Dependency Injection Extensions

#### AddOpenTelemetryPipelineTracer(IServiceCollection, string)

Register with a specific service name:

```csharp
services.AddOpenTelemetryPipelineTracer("MyService");
```

#### AddOpenTelemetryPipelineTracer(IServiceCollection)

Register with the default service name ("NPipeline"):

```csharp
services.AddOpenTelemetryPipelineTracer();
```

#### AddOpenTelemetryPipelineTracer(IServiceCollection, Func\<IServiceProvider, OpenTelemetryPipelineTracer>)

Register with custom factory logic (useful for configuration-driven service names):

```csharp
services.AddOpenTelemetryPipelineTracer(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var serviceName = config["ServiceName"] ?? "DefaultService";
    return new OpenTelemetryPipelineTracer(serviceName);
});
```

### Tracer Provider Builder Extensions

#### AddNPipelineSource(TracerProviderBuilder, string)

Configure your tracer provider to capture activities from a specific NPipeline service by subscribing to its `ActivitySource`:

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddJaegerExporter()
    .Build();
```

#### AddNPipelineSources(TracerProviderBuilder, IEnumerable\<string>)

Configure your tracer provider to capture activities from multiple NPipeline services:

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSources(new[] { "Pipeline-A", "Pipeline-B", "Pipeline-C" })
    .AddJaegerExporter()
    .Build();
```

#### GetNPipelineInfo(Activity)

Extract NPipeline-specific information from activities in custom processors or exporters:

```csharp
var info = activity.GetNPipelineInfo();
if (info != null)
{
    Console.WriteLine($"Service: {info.ServiceName}, Activity: {info.ActivityName}");
}
```

`GetNPipelineInfo` primarily uses `activity.Source.Name` as the service name and the activity's display/operation name as the activity name, but it also supports older patterns where the display name is in the form `Service.Activity`.

## Common Backends

### Jaeger

Export traces to a local or remote Jaeger instance:

```bash
# Start local Jaeger
docker run -p 4317:4317 -p 16686:16686 jaegertracing/jaeger:latest
```

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddOtlpExporter(opt =>
    {
        opt.Endpoint = new Uri("http://localhost:4317");
    })
    .Build();
```

Then visit `http://localhost:16686` to view traces.

### Zipkin

Export traces to Zipkin:

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddZipkinExporter()
    .Build();
```

### Azure Application Insights

Export traces to Azure Monitor:

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddAzureMonitorTraceExporter(options =>
    {
        options.ConnectionString = "InstrumentationKey=...";
    })
    .Build();
```

### AWS X-Ray

Export traces to AWS X-Ray:

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddAwsXRayExporter()
    .Build();
```

## Activity Hierarchies

OpenTelemetry automatically manages activity hierarchies. When you start a new activity while one is active, it becomes a child of the current activity:

```csharp
// In outer pipeline activity
public async Task ExecutePipeline()
{
    var outerActivity = tracer.StartActivity("ExecutePipeline");
    
    // In nested operation
    var innerActivity = tracer.StartActivity("ProcessBatch");
    // This activity is automatically a child of outerActivity
    
    innerActivity.Dispose();
    outerActivity.Dispose();
}
```

The trace will show the complete hierarchy, making it easy to understand execution flow.

## Best Practices

1. **Use meaningful service names**: Choose names that identify your application or service uniquely
2. **Standardize activity names**: Use consistent naming for pipeline operations for better trace organization
3. **Add context tags**: Use `SetTag()` to add relevant context for debugging:

   ```csharp
   activity.SetTag("node_id", nodeIdentifier);
   activity.SetTag("batch_size", items.Count);
   ```

4. **Handle errors properly**: Ensure exceptions are recorded for visibility in traces
5. **Avoid high cardinality tags**: Don't use timestamps or request IDs as tag values; use baggage instead
6. **Sample high-volume traces**: For production pipelines with millions of items, consider sampling to avoid trace overhead
7. **Monitor trace volume**: Track the number of traces being exported to manage costs

## Configuration Examples

### Development Environment

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline.Dev")
    .AddConsoleExporter()  // Debug locally
    .Build();
```

### Production with Sampling

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline.Prod")
    .SetSampler(new ProbabilitySampler(0.1))  // Sample 10% of traces
    .AddOtlpExporter(opt =>
    {
        opt.Endpoint = new Uri("https://your-otel-collector:4317");
    })
    .Build();
```

### Multi-Service Setup

```csharp
var services = new ServiceCollection();

// Configure each pipeline service
services.AddOpenTelemetryPipelineTracer("OrderProcessing");

// Single tracer provider for all services
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSources(new[]
    {
        "OrderProcessing",
        "PaymentProcessing",
        "NotificationService"
    })
    .AddJaegerExporter()
    .Build();
```

## Troubleshooting

### Traces Not Appearing

1. **Verify service name matches**: The service name in `AddOpenTelemetryPipelineTracer()` must match one of the sources in `AddNPipelineSource()`
2. **Check exporter configuration**: Ensure your backend (Jaeger, Zipkin, etc.) is running and properly configured
3. **Enable debug logging**: Set up OpenTelemetry logging to debug connection issues:

   ```csharp
   .AddOtlpExporter(opt =>
   {
       opt.Endpoint = new Uri("http://localhost:4317");
       opt.Headers = "Authorization=Bearer your-token";
   })
   ```

### Performance Issues

1. **Use sampling**: High-volume pipelines should use sampling to reduce trace overhead
2. **Batch exports**: Configure batching in your exporter to reduce network calls
3. **Filter activities**: Use `IActivityListener` to filter which activities are processed

## See Also

- [Distributed Tracing](./tracing.md): Learn about the core tracing abstraction
- [Metrics Collection](./metrics.md): Combine tracing with metrics for complete observability
- [OpenTelemetry Documentation](https://opentelemetry.io/): Official OpenTelemetry documentation
- [OpenTelemetry .NET](https://github.com/open-telemetry/opentelemetry-dotnet): .NET implementation details
