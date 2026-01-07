# NPipeline Observability Extension

Comprehensive metrics collection and monitoring for NPipeline pipelines.

## Contents

### Getting Started

- **[Getting Started Guide](./getting-started.md)** - Quick start and basic usage
- **[Advanced Patterns](./advanced-patterns.md)** - Advanced scenarios and integrations

### Reference Documentation

- **[Configuration](../../docs/extensions/observability-configuration.md)** - DI setup and configuration options
- **[Metrics Reference](../../docs/extensions/observability-metrics.md)** - Complete metrics documentation
- **[Examples](../../docs/extensions/observability-examples.md)** - Real-world usage examples
- **[Main Documentation](../../docs/extensions/observability.md)** - Comprehensive guide

## Quick Links

- [NPipeline GitHub Repository](https://github.com/NPipeline/NPipeline)
- [NuGet Package](https://www.nuget.org/packages/NPipeline.Extensions.Observability)
- [Project README](../../../src/NPipeline.Extensions.Observability/README.md)

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

```
┌─────────────────────────────────────────┐
│         Pipeline Execution              │
└──────────────┬──────────────────────────┘
               │
               ├─> IExecutionObserver
               │   (MetricsCollectingExecutionObserver)
               │
               ↓
        ┌──────────────────┐
        │ IObservabilityCollector │
        │  (Thread-safe)    │
        └──────────┬──────────┘
                   │
                   ├─> Node Metrics
                   └─> Pipeline Metrics
                   │
                   ↓
        ┌──────────────────┐
        │   IMetricsSink    │
        │ IPipelineMetricsSink │
        └──────────┬──────────┘
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
var collector = serviceProvider.GetRequiredService<IObservabilityCollector>();
var observer = new MetricsCollectingExecutionObserver(collector);
var context = PipelineContext.Default with
{
    ExecutionObservers = new List<IExecutionObserver> { observer }
};

await runner.RunAsync<MyPipeline>(context);

// View Results
var metrics = collector.CreatePipelineMetrics(
    "MyPipeline",
    Guid.NewGuid(),
    startTime,
    DateTimeOffset.UtcNow,
    true);

Console.WriteLine($"Processed {metrics.TotalItemsProcessed} items");
```

## Contributing

Contributions are welcome! Please see the main [NPipeline contributing guide](../../../CONTRIBUTING.md) for details.

## License

MIT License - See [LICENSE](../../../LICENSE) file for details.
