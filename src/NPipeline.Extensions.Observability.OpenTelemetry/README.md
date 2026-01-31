# NPipeline.Extensions.Observability.OpenTelemetry

OpenTelemetry integration for NPipeline observability - provides seamless integration with OpenTelemetry SDKs for distributed tracing.

## Overview

This package enables NPipeline pipelines to export traces to OpenTelemetry-compatible backends such as Jaeger, Zipkin, Azure Monitor, AWS X-Ray, and others. It
implements the `IPipelineTracer` interface using .NET's `System.Diagnostics.ActivitySource` and `Activity` APIs, following OpenTelemetry best practices for .NET
applications.

### Key Features

- **Seamless OpenTelemetry Integration**: Uses standard `ActivitySource` pattern for compatibility with all OpenTelemetry exporters
- **Service Name Prefixing**: Activities are prefixed with a service name for easy identification in trace visualizers
- **Automatic Parent-Child Relationships**: Respects `Activity.Current` context for hierarchical tracing
- **Null-Optimized**: Falls back to null tracer when sampling drops activities, avoiding unnecessary allocations
- **Multi-Service Support**: Configure multiple pipeline services through a single tracer provider
- **Extension Methods**: Fluent API for configuring OpenTelemetry with NPipeline sources

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability.OpenTelemetry
```

### Requirements

- **.NET 8.0**, **9.0**, or **10.0**
- **NPipeline.Extensions.Observability** (automatically included as a dependency)
- **OpenTelemetry** SDKs for your chosen exporter

## Quick Start

### Basic Setup with Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Extensions.Observability.OpenTelemetry;
using NPipeline.Observability.DependencyInjection;

// Register observability services
var services = new ServiceCollection();
services.AddNPipelineObservability();
services.AddOpenTelemetryPipelineTracer("MyPipeline");

// Configure OpenTelemetry export to Jaeger
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddJaegerExporter()
    .Build();

var serviceProvider = services.BuildServiceProvider();
```

### Running a Pipeline with Tracing

```csharp
// Get the tracer and context factory
var tracer = serviceProvider.GetRequiredService<IPipelineTracer>();
var contextFactory = serviceProvider.GetRequiredService<IObservablePipelineContextFactory>();

// Create a context with tracing enabled
await using var context = contextFactory.Create(
    PipelineContextConfiguration.WithObservability(tracer: tracer)
);

// Run your pipeline - traces are automatically exported
var runner = serviceProvider.GetRequiredService<IPipelineRunner>();
await runner.RunAsync<MyPipeline>(context);
```

## Core Components

### OpenTelemetryPipelineTracer

The main tracer implementation that creates `Activity` instances from an `ActivitySource`.

**Why this design**: Using `ActivitySource` is the recommended OpenTelemetry pattern for .NET. It ensures activities are captured by providers configured with
`AddSource(serviceName)`, providing automatic integration with all OpenTelemetry exporters.

```csharp
var tracer = new OpenTelemetryPipelineTracer("MyPipeline");

// The tracer creates activities with the service name as the source
var activity = tracer.StartActivity("ProcessOrder");
// Activity is automatically exported to configured OpenTelemetry backends
```

**Key behaviors**:

- Creates activities from an `ActivitySource` named after the service
- Returns null activities when sampling drops them (no allocations)
- Automatically establishes parent-child relationships via `Activity.Current`
- Disposes the `ActivitySource` when the tracer is disposed

### TracerProviderBuilder Extensions

Fluent extension methods for configuring OpenTelemetry to capture NPipeline traces.

#### AddNPipelineSource

Configures the tracer provider to listen for activities from a specific NPipeline service.

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")  // Must match tracer service name
    .AddJaegerExporter()
    .Build();
```

**Why this is needed**: The service name used when creating `OpenTelemetryPipelineTracer` must match the source name added to the tracer provider. This
extension ensures consistency and prevents configuration errors.

#### AddNPipelineSources

Configures multiple NPipeline services at once.

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSources(new[] { "PipelineA", "PipelineB", "PipelineC" })
    .AddZipkinExporter()
    .Build();
```

**Use case**: Ideal for microservices architectures where multiple pipeline services share a single OpenTelemetry configuration.

#### GetNPipelineInfo

Extracts NPipeline-specific metadata from activities for custom exporters or processors.

```csharp
public class CustomExporter : BaseExporter<Activity>
{
    public override ExportResult Export(in Batch<Activity> batch)
    {
        foreach (var activity in batch)
        {
            var info = activity.GetNPipelineInfo();
            if (info != null)
            {
                Console.WriteLine($"Service: {info.ServiceName}, Activity: {info.ActivityName}");
            }
        }
        return ExportResult.Success;
    }
}
```

## Configuration Examples

### Jaeger Exporter

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddJaegerExporter(o =>
    {
        o.AgentHost = "localhost";
        o.AgentPort = 6831;
    })
    .Build();
```

### Zipkin Exporter

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddZipkinExporter(o =>
    {
        o.Endpoint = new Uri("http://localhost:9411/api/v2/spans");
    })
    .Build();
```

### OTLP (OpenTelemetry Protocol) Exporter

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddOtlpExporter(o =>
    {
        o.Endpoint = new Uri("http://localhost:4317");
    })
    .Build();
```

### Azure Monitor Exporter

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddAzureMonitorTraceExporter(o =>
    {
        o.ConnectionString = "InstrumentationKey=your-key";
    })
    .Build();
```

### AWS X-Ray Exporter

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddAWSXRayTraceExporter(o =>
    {
        o.SetResourceResourceDetector(new AWSEBSDetector());
    })
    .Build();
```

## Architecture

### Activity Flow

```
Pipeline Execution
    ↓
OpenTelemetryPipelineTracer.StartActivity()
    ↓
ActivitySource.StartActivity(name)
    ↓
Activity created (or null if sampled out)
    ↓
PipelineActivity wrapper (or NullPipelineActivity)
    ↓
Exported to configured OpenTelemetry backend
```

### Service Name Matching

The service name serves as the bridge between the tracer and the exporter:

1. **Tracer Creation**: `new OpenTelemetryPipelineTracer("MyPipeline")`
2. **Exporter Configuration**: `builder.AddNPipelineSource("MyPipeline")`
3. **Activity Source**: Activities have `Source.Name = "MyPipeline"`

If these don't match, traces won't be captured. The extension methods enforce this contract.

### Null Activity Handling

When OpenTelemetry sampling drops an activity (returns null from `StartActivity`), the tracer automatically falls back to `NullPipelineTracer.Instance`. This
design choice:

- **Prevents allocations**: No `PipelineActivity` wrapper is created for dropped activities
- **Maintains consistency**: The null tracer provides the same interface without side effects
- **Respects sampling**: Allows OpenTelemetry to control trace sampling efficiently

## Best Practices

### 1. Use Consistent Service Names

```csharp
// Good: Service name is a constant
const string ServiceName = "OrderProcessingPipeline";

services.AddOpenTelemetryPipelineTracer(ServiceName);
builder.AddNPipelineSource(ServiceName);
```

### 2. Register as Singleton

```csharp
// Register the tracer as a singleton
services.AddSingleton<IPipelineTracer>(sp =>
    new OpenTelemetryPipelineTracer("MyPipeline"));
```

### 3. Dispose Tracer Provider

```csharp
// Ensure proper disposal of the tracer provider
await using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .AddJaegerExporter()
    .Build();

try
{
    // Run pipelines
}
finally
{
    // Tracer provider is disposed automatically
}
```

### 4. Configure Sampling

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .SetSampler(new TraceIdRatioBasedSampler(0.1)) // 10% sampling
    .AddJaegerExporter()
    .Build();
```

### 5. Add Resource Attributes

```csharp
using var tracerProvider = new TracerProviderBuilder()
    .AddNPipelineSource("MyPipeline")
    .ConfigureResource(r => r
        .AddService("MyPipeline", "1.0.0")
        .AddAttributes(new Dictionary<string, object>
        {
            ["environment"] = "production",
            ["deployment.region"] = "us-west-2"
        }))
    .AddJaegerExporter()
    .Build();
```

## Performance Considerations

- **Allocation Optimization**: Null activities don't allocate `PipelineActivity` wrappers
- **Sampling**: Use OpenTelemetry sampling to reduce trace volume in high-throughput scenarios
- **Batch Exporting**: Most exporters batch traces for efficient network usage
- **Async Export**: Exporters typically use async I/O to avoid blocking pipeline execution

## Troubleshooting

### Traces Not Appearing in Backend

**Problem**: Traces are not visible in Jaeger, Zipkin, or other backends.

**Solutions**:

1. Verify service name matching between tracer and exporter configuration
2. Check that the tracer provider is built before pipeline execution
3. Ensure the exporter endpoint is accessible
4. Verify sampling configuration (may be dropping all traces)
5. Check exporter logs for connection errors

### Activities Not Created

**Problem**: `StartActivity` returns null activities.

**Solutions**:

1. Verify `ActivitySource` is registered with the tracer provider
2. Check sampling configuration
3. Ensure `OpenTelemetryPipelineTracer` is properly initialized
4. Verify the service name is not empty or whitespace

### High Memory Usage

**Problem**: Tracing causes increased memory consumption.

**Solutions**:

1. Implement aggressive sampling (e.g., 1-5% trace rate)
2. Use batch exporters with appropriate batch sizes
3. Consider exporting to a backend with efficient storage
4. Monitor and adjust exporter buffer sizes

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Extensions.Observability](https://www.nuget.org/packages/NPipeline.Extensions.Observability)** - Core observability extension
- **[OpenTelemetry](https://www.nuget.org/packages/OpenTelemetry)** - OpenTelemetry SDK for .NET

## License

MIT License - see LICENSE file for details.
