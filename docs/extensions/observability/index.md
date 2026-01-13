# NPipeline Observability Extension

Comprehensive metrics collection and monitoring for NPipeline pipelines.

## Contents

### Getting Started

- **[Getting Started Guide](./getting-started.md)** - Quick start and basic usage
- **[Advanced Patterns](./advanced-patterns.md)** - Advanced scenarios and integrations

### Reference Documentation

- **[Configuration](./configuration.md)** - DI setup and configuration options
- **[Metrics Reference](./metrics.md)** - Complete metrics documentation
- **[Distributed Tracing](./tracing.md)** - Core tracing abstraction and building custom tracers
- **[OpenTelemetry Integration](./opentelemetry.md)** - Production-grade tracing with OpenTelemetry backends
- **[Examples](./examples.md)** - Real-world usage examples
- **[Overview](./overview.md)** - Comprehensive guide

## Features Overview

### Metrics Collection

- Node-level execution metrics
- Pipeline-level aggregations
- Thread-safe concurrent collection
- Automatic throughput calculations

### Flexible Sinks

- Built-in logging sinks
- Custom sink implementations
- Integration with monitoring systems
- Batching and buffering support

### DI Integration

- Scoped collectors per pipeline run
- Transient or custom sink lifetimes
- Factory delegates for complex setups
- Seamless ASP.NET Core integration

### Performance

- Minimal overhead (< 1% typically)
- Lock-free concurrent operations
- Memory-efficient storage
- Optimized for high-throughput scenarios

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

## Installation

```bash
dotnet add package NPipeline.Extensions.Observability
```

## Minimal Example

```csharp
// Setup
services.AddNPipelineObservability();

// Use
var contextFactory = serviceProvider.GetRequiredService<IObservablePipelineContextFactory>();
await using var context = contextFactory.Create();

await runner.RunAsync<MyPipeline>(context);

// View Results
var collector = serviceProvider.GetRequiredService<IObservabilityCollector>();
var nodeMetrics = collector.GetNodeMetrics();

foreach (var metric in nodeMetrics)
{
    Console.WriteLine($"Node {metric.NodeId}: {metric.ItemsProcessed} items, {metric.DurationMs}ms");
}
```
