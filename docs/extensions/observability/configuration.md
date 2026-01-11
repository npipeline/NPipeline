---
title: Observability Configuration
description: Detailed guide to configuring NPipeline observability with dependency injection, custom sinks, and advanced scenarios.
---

# Observability Configuration

This guide covers all configuration options for the NPipeline Observability extension, including dependency injection registration, custom metrics sinks, and integration patterns.

## Quick Start: Automatic Metrics Collection

The simplest way to enable observability is to use the `IObservablePipelineContextFactory`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var host = Host.CreateDefaultBuilder()
    .ConfigureServices((context, services) =>
    {
        services.AddNPipelineObservability(); // Registers everything automatically
        services.AddNPipeline(Assembly.GetExecutingAssembly());
    })
    .Build();

// Create context with observability pre-configured
await using var scope = host.Services.CreateAsyncScope();
var contextFactory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();
await using var context = contextFactory.Create(); // ExecutionObserver is already set!

var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
await runner.RunAsync<MyPipeline>(context);
```

No need to manually create or wire up `MetricsCollectingExecutionObserver` - it's all handled automatically!

## Registration Methods

The extension provides multiple registration methods to accommodate different scenarios and requirements.

### 1. Default Registration with Logging Sinks

The simplest approach uses built-in logging sinks that output metrics to `ILogger`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.DependencyInjection;

var services = new ServiceCollection();

// Register with default logging sinks
services.AddNPipelineObservability();

// Register NPipeline core services
services.AddNPipeline(Assembly.GetExecutingAssembly());

var serviceProvider = services.BuildServiceProvider();
```

**What gets registered:**
- `IObservabilityCollector` (scoped) - collects metrics during execution
- `IExecutionObserver` (scoped) - automatically wired to observer pipeline events
- `IObservablePipelineContextFactory` (scoped) - creates contexts with observability enabled
- `IMetricsSink` → `LoggingMetricsSink` (transient) - outputs metrics via ILogger
- `IPipelineMetricsSink` → `LoggingPipelineMetricsSink` (transient) - outputs pipeline-level metrics
- `IObservabilityFactory` → `DiObservabilityFactory` (scoped) - factory for sink resolution

**Use when:**
- You want quick observability without additional infrastructure
- You're already using structured logging (Serilog, NLog, etc.)
- You need metrics for local development or debugging

### 2. Custom Metrics Sinks

Register your own implementations of metrics sinks:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Observability.Metrics;

var services = new ServiceCollection();

// Register with custom sinks
services.AddNPipelineObservability<CustomMetricsSink, CustomPipelineMetricsSink>();

// Register your custom sinks if they have dependencies
services.AddSingleton<ITelemetryClient, TelemetryClient>();
services.AddTransient<CustomMetricsSink>();
services.AddTransient<CustomPipelineMetricsSink>();

var serviceProvider = services.BuildServiceProvider();
```

**Custom sink implementations:**

```csharp
public sealed class CustomMetricsSink : IMetricsSink
{
    private readonly ITelemetryClient _telemetryClient;

    public CustomMetricsSink(ITelemetryClient telemetryClient)
    {
        _telemetryClient = telemetryClient;
    }

    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        // Emit metrics to your monitoring system
        var properties = new Dictionary<string, string>
        {
            ["NodeId"] = nodeMetrics.NodeId,
            ["Success"] = nodeMetrics.Success.ToString()
        };

        var metrics = new Dictionary<string, double>
        {
            ["DurationMs"] = nodeMetrics.DurationMs ?? 0,
            ["ItemsProcessed"] = nodeMetrics.ItemsProcessed,
            ["Throughput"] = nodeMetrics.ThroughputItemsPerSec ?? 0
        };

        _telemetryClient.TrackEvent("NodeCompleted", properties, metrics);
        return Task.CompletedTask;
    }
}

public sealed class CustomPipelineMetricsSink : IPipelineMetricsSink
{
    private readonly ITelemetryClient _telemetryClient;

    public CustomPipelineMetricsSink(ITelemetryClient telemetryClient)
    {
        _telemetryClient = telemetryClient;
    }

    public Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken)
    {
        var properties = new Dictionary<string, string>
        {
            ["PipelineName"] = pipelineMetrics.PipelineName,
            ["RunId"] = pipelineMetrics.RunId.ToString(),
            ["Success"] = pipelineMetrics.Success.ToString()
        };

        var metrics = new Dictionary<string, double>
        {
            ["DurationMs"] = pipelineMetrics.DurationMs ?? 0,
            ["TotalItemsProcessed"] = pipelineMetrics.TotalItemsProcessed
        };

        _telemetryClient.TrackEvent("PipelineCompleted", properties, metrics);
        return Task.CompletedTask;
    }
}
```

**Use when:**
- You need to integrate with Application Insights, Prometheus, OpenTelemetry, or other monitoring systems
- You want to transform or enrich metrics before emission
- You need to send metrics to multiple destinations

### 3. Factory Delegate Registration

Use factory delegates for complex initialization scenarios:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Observability.Metrics;

var services = new ServiceCollection();

// Register with factory delegates
services.AddNPipelineObservability(
    metricsSinkFactory: serviceProvider =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<CustomMetricsSink>>();
        var config = serviceProvider.GetRequiredService<IConfiguration>();
        var endpoint = config["Metrics:Endpoint"];
        return new CustomMetricsSink(logger, endpoint);
    },
    pipelineMetricsSinkFactory: serviceProvider =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<CustomPipelineMetricsSink>>();
        var config = serviceProvider.GetRequiredService<IConfiguration>();
        var endpoint = config["Metrics:PipelineEndpoint"];
        return new CustomPipelineMetricsSink(logger, endpoint);
    }
);

var serviceProvider = services.BuildServiceProvider();
```

**Use when:**
- Your sinks require complex initialization logic
- You need to resolve multiple dependencies from the service provider
- You want to conditionally create sinks based on configuration

### 4. Custom Collector Implementation

For specialized metrics collection scenarios, provide your own collector:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Observability.Metrics;

// Custom collector implementation
public sealed class CustomObservabilityCollector : IObservabilityCollector
{
    private readonly ObservabilityCollector _baseCollector;
    private readonly ILogger _logger;

    public CustomObservabilityCollector(ILogger<CustomObservabilityCollector> logger)
    {
        _baseCollector = new ObservabilityCollector();
        _logger = logger;
    }

    public void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, long? initialMemoryMb = null)
    {
        _logger.LogInformation("Node {NodeId} started at {Timestamp}", nodeId, timestamp);
        _baseCollector.RecordNodeStart(nodeId, timestamp, threadId, initialMemoryMb);
    }

    public void RecordNodeEnd(string nodeId, DateTimeOffset timestamp, bool success, Exception? exception = null, 
        long? peakMemoryMb = null, long? processorTimeMs = null)
    {
        _baseCollector.RecordNodeEnd(nodeId, timestamp, success, exception, peakMemoryMb, processorTimeMs);
        
        if (!success)
        {
            _logger.LogError(exception, "Node {NodeId} failed", nodeId);
        }
    }

    public void RecordItemMetrics(string nodeId, long itemsProcessed, long itemsEmitted)
    {
        _baseCollector.RecordItemMetrics(nodeId, itemsProcessed, itemsEmitted);
    }

    public void RecordRetry(string nodeId, int retryCount, string? reason = null)
    {
        _logger.LogWarning("Node {NodeId} retry attempt {RetryCount}. Reason: {Reason}", nodeId, retryCount, reason);
        _baseCollector.RecordRetry(nodeId, retryCount, reason);
    }

    public void RecordPerformanceMetrics(string nodeId, double throughputItemsPerSec, double averageItemProcessingMs)
    {
        _baseCollector.RecordPerformanceMetrics(nodeId, throughputItemsPerSec, averageItemProcessingMs);
    }

    public IReadOnlyList<INodeMetrics> GetNodeMetrics()
    {
        return _baseCollector.GetNodeMetrics();
    }

    public INodeMetrics? GetNodeMetrics(string nodeId)
    {
        return _baseCollector.GetNodeMetrics(nodeId);
    }

    public IPipelineMetrics CreatePipelineMetrics(string pipelineName, Guid runId, DateTimeOffset startTime, 
        DateTimeOffset? endTime, bool success, Exception? exception = null)
    {
        return _baseCollector.CreatePipelineMetrics(pipelineName, runId, startTime, endTime, success, exception);
    }
}

// Registration
var services = new ServiceCollection();

services.AddNPipelineObservability<CustomObservabilityCollector, LoggingMetricsSink, LoggingPipelineMetricsSink>();

var serviceProvider = services.BuildServiceProvider();
```

**Use when:**
- You need to add custom logging or side effects during metrics collection
- You want to transform or filter metrics before they're stored
- You need to integrate with custom observability infrastructure

### 5. Custom Collector with Factory Delegate

Combine custom collector with factory delegate initialization:

```csharp
services.AddNPipelineObservability<LoggingMetricsSink, LoggingPipelineMetricsSink>(
    collectorFactory: serviceProvider =>
    {
        var logger = serviceProvider.GetRequiredService<ILogger<CustomObservabilityCollector>>();
        var config = serviceProvider.GetRequiredService<IConfiguration>();
        var enableDetailedMetrics = config.GetValue<bool>("Observability:DetailedMetrics");
        return new CustomObservabilityCollector(logger, enableDetailedMetrics);
    }
);
```

## Service Lifetimes

Understanding service lifetimes is crucial for proper configuration:

| Service | Default Lifetime | Rationale |
|---------|------------------|-----------|
| `IObservabilityCollector` | Scoped | One instance per pipeline run for isolation |
| `IMetricsSink` | Scoped | New instance per pipeline run to avoid state sharing |
| `IPipelineMetricsSink` | Scoped | New instance per pipeline run to avoid state sharing |
| `IObservabilityFactory` | Scoped | Resolves scoped collector instances |

### Why Scoped Collector?

The collector is scoped to ensure:
- **Isolation**: Each pipeline run gets its own metrics, preventing cross-contamination
- **Memory management**: Metrics are automatically disposed when the scope ends
- **Thread safety**: Concurrent pipeline runs don't interfere with each other

### Why Scoped Sinks?

Sinks are scoped because:
- **Stateless operation**: Most sinks don't maintain state between uses
- **Dependency resolution**: Allows sinks to receive scoped dependencies
- **Flexibility**: Enables different sink instances for different pipeline runs

## Configuration Options

### ObservabilityExtensionOptions

The [`ObservabilityExtensionOptions`](../../src/NPipeline.Extensions.Observability/DependencyInjection/ObservabilityExtensionOptions.cs:10) class controls global behavior of the observability extension:

```csharp
public sealed record ObservabilityExtensionOptions
{
    /// Whether to automatically collect memory metrics (peak memory usage) for each node
    bool EnableMemoryMetrics { get; init; }

    /// Default observability extension options with memory metrics disabled
    public static ObservabilityExtensionOptions Default => new() { EnableMemoryMetrics = false };

    /// Observability extension options with memory metrics enabled
    public static ObservabilityExtensionOptions WithMemoryMetrics => new() { EnableMemoryMetrics = true };
}
```

**Default**: `EnableMemoryMetrics = false` (memory metrics disabled by default)

**Important**: Memory metrics require BOTH:
1. Extension-level configuration: `services.AddNPipelineObservability(ObservabilityExtensionOptions.WithMemoryMetrics)`
2. Node-level configuration: `.WithObservability(builder, ObservabilityOptions.Full)` or set `RecordMemoryUsage = true`

If either level is disabled, memory metrics will not be collected.

### Default Configuration (Logging Sinks)

### Conditional Registration

Enable observability based on configuration:

```csharp
var services = new ServiceCollection();
var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .Build();

var enableObservability = configuration.GetValue<bool>("Observability:Enabled", true);

if (enableObservability)
{
    services.AddNPipelineObservability();
}
```

### Multiple Sinks

Send metrics to multiple destinations by implementing composite sinks:

```csharp
public sealed class CompositeMetricsSink : IMetricsSink
{
    private readonly IEnumerable<IMetricsSink> _sinks;

    public CompositeMetricsSink(IEnumerable<IMetricsSink> sinks)
    {
        _sinks = sinks;
    }

    public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        var tasks = _sinks.Select(sink => sink.RecordAsync(nodeMetrics, cancellationToken));
        await Task.WhenAll(tasks);
    }
}

// Register multiple sinks
services.AddSingleton<IMetricsSink, LoggingMetricsSink>();
services.AddSingleton<IMetricsSink, ApplicationInsightsSink>();
services.AddSingleton<IMetricsSink, PrometheusSink>();

// Register composite sink
services.AddNPipelineObservability<CompositeMetricsSink, LoggingPipelineMetricsSink>();
```

### Configuration-Based Sink Selection

Choose sinks based on configuration:

```csharp
services.AddNPipelineObservability(
    metricsSinkFactory: serviceProvider =>
    {
        var config = serviceProvider.GetRequiredService<IConfiguration>();
        var sinkType = config["Observability:SinkType"];

        return sinkType switch
        {
            "Logging" => new LoggingMetricsSink(),
            "ApplicationInsights" => new ApplicationInsightsSink(/* ... */),
            "Prometheus" => new PrometheusSink(/* ... */),
            _ => throw new InvalidOperationException($"Unknown sink type: {sinkType}")
        };
    },
    pipelineMetricsSinkFactory: serviceProvider =>
    {
        var config = serviceProvider.GetRequiredService<IConfiguration>();
        var sinkType = config["Observability:PipelineSinkType"];

        return sinkType switch
        {
            "Logging" => new LoggingPipelineMetricsSink(),
            "ApplicationInsights" => new ApplicationInsightsPipelineSink(/* ... */),
            _ => throw new InvalidOperationException($"Unknown sink type: {sinkType}")
        };
    }
);
```

## Integration with Existing Logging Infrastructure

The extension integrates seamlessly with Microsoft.Extensions.Logging:

### Structured Logging with Serilog

```csharp
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.Console()
    .WriteTo.File("logs/pipeline-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();

var services = new ServiceCollection();
services.AddLogging(loggingBuilder => loggingBuilder.AddSerilog());

services.AddNPipelineObservability();
```

### Enriching Logs with Context

```csharp
public sealed class EnrichedLoggingMetricsSink : IMetricsSink
{
    private readonly ILogger _logger;
    private readonly IHttpContextAccessor _httpContextAccessor;

    public EnrichedLoggingMetricsSink(
        ILogger<EnrichedLoggingMetricsSink> logger,
        IHttpContextAccessor httpContextAccessor)
    {
        _logger = logger;
        _httpContextAccessor = httpContextAccessor;
    }

    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        var correlationId = _httpContextAccessor.HttpContext?.TraceIdentifier;
        
        using (_logger.BeginScope(new Dictionary<string, object?>
        {
            ["NodeId"] = nodeMetrics.NodeId,
            ["Success"] = nodeMetrics.Success,
            ["CorrelationId"] = correlationId
        }))
        {
            _logger.LogInformation(
                "Node {NodeId} completed. Processed {ItemsProcessed} items in {DurationMs}ms",
                nodeMetrics.NodeId,
                nodeMetrics.ItemsProcessed,
                nodeMetrics.DurationMs);
        }

        return Task.CompletedTask;
    }
}
```

## Best Practices

### 1. Use Appropriate Service Lifetimes

- Keep collectors scoped for pipeline isolation
- Use transient sinks to avoid state sharing
- Register singleton sinks only if they're truly stateless

### 2. Handle Cancellation

Always respect cancellation tokens in async sink implementations:

```csharp
public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
{
    try
    {
        await _telemetryClient.TrackEventAsync(
            "NodeCompleted",
            properties,
            metrics,
            cancellationToken);
    }
    catch (OperationCanceledException)
    {
        // Log cancellation and exit gracefully
        _logger.LogWarning("Metrics recording cancelled");
    }
}
```

### 3. Implement Retry Logic for External Systems

When sending metrics to external systems, implement retry logic:

```csharp
public sealed class ResilientMetricsSink : IMetricsSink
{
    private readonly ITelemetryClient _telemetryClient;
    private readonly ILogger _logger;
    private readonly AsyncRetryPolicy _retryPolicy;

    public ResilientMetricsSink(ITelemetryClient telemetryClient, ILogger<ResilientMetricsSink> logger)
    {
        _telemetryClient = telemetryClient;
        _logger = logger;
        
        _retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    _logger.LogWarning(
                        exception,
                        "Retry {RetryCount} after {Delay}s for metrics recording",
                        retryCount,
                        timeSpan.TotalSeconds);
                });
    }

    public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        await _retryPolicy.ExecuteAsync(async () =>
        {
            await _telemetryClient.TrackEventAsync(
                "NodeCompleted",
                CreateProperties(nodeMetrics),
                CreateMetrics(nodeMetrics),
                cancellationToken);
        });
    }
}
```

### 4. Filter Metrics in Development

Reduce noise in development environments:

```csharp
public sealed class DevelopmentMetricsSink : IMetricsSink
{
    private readonly IMetricsSink _innerSink;
    private readonly bool _isDevelopment;

    public DevelopmentMetricsSink(IMetricsSink innerSink, IHostEnvironment environment)
    {
        _innerSink = innerSink;
        _isDevelopment = environment.IsDevelopment();
    }

    public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        if (_isDevelopment)
        {
            // Only log errors and warnings in development
            if (!nodeMetrics.Success || nodeMetrics.RetryCount > 0)
            {
                await _innerSink.RecordAsync(nodeMetrics, cancellationToken);
            }
        }
        else
        {
            // Log everything in production
            await _innerSink.RecordAsync(nodeMetrics, cancellationToken);
        }
    }
}
```

### 5. Aggregate Metrics for High-Volume Scenarios

For high-throughput pipelines, aggregate metrics before emission:

```csharp
public sealed class AggregatingMetricsSink : IMetricsSink
{
    private readonly ConcurrentDictionary<string, AggregatedMetrics> _aggregates = new();
    private readonly Timer _flushTimer;
    private readonly IMetricsSink _innerSink;

    public AggregatingMetricsSink(IMetricsSink innerSink)
    {
        _innerSink = innerSink;
        _flushTimer = new Timer(FlushAggregates, null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
    }

    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        var aggregate = _aggregates.GetOrAdd(nodeMetrics.NodeId, _ => new AggregatedMetrics());
        aggregate.Add(nodeMetrics);
        return Task.CompletedTask;
    }

    private async void FlushAggregates(object? state)
    {
        foreach (var kvp in _aggregates)
        {
            var aggregatedMetrics = kvp.Value.Build();
            await _innerSink.RecordAsync(aggregatedMetrics, CancellationToken.None);
            _aggregates.TryRemove(kvp.Key, out _);
        }
    }
}
```

## Troubleshooting

### Metrics Not Appearing

**Problem**: Metrics are not being logged or sent to external systems.

**Solutions**:
1. Verify observability is registered: `services.AddNPipelineObservability()`
2. Check that the pipeline is using DI: `await serviceProvider.RunPipelineAsync<T>()`
3. Ensure logging is configured properly
4. Verify sink implementations are not throwing exceptions

### Memory Leaks

**Problem**: Memory usage increases over time with long-running pipelines.

**Solutions**:
1. Ensure collectors are scoped, not singleton
2. Verify sinks are transient and don't retain references
3. Check for circular dependencies in sink implementations
4. Review custom collector implementations for proper disposal

### Performance Degradation

**Problem**: Pipeline execution slows down when observability is enabled.

**Solutions**:
1. Use async sink implementations
2. Implement batching or aggregation for external calls
3. Consider disabling expensive metrics (memory, processor time) in production
4. Use sampling for high-volume scenarios

## Architecture and Design

### System Architecture

The extension follows a layered architecture for separation of concerns:

```text
Pipeline Execution
        ↓
IExecutionObserver
        ↓
MetricsCollectingExecutionObserver
        ↓
IObservabilityCollector (Thread-safe)
        ├→ Node Metrics
        └→ Pipeline Metrics
        ↓
IMetricsSink / IPipelineMetricsSink
        ├→ LoggingMetricsSink
        ├→ Custom Sinks
        └→ Composite Sinks
```

### Key Components

- **`MetricsCollectingExecutionObserver`**: Hooks into pipeline execution lifecycle to capture node start/end events and delegate to the collector
- **`IObservabilityCollector`**: Thread-safe collector that aggregates metrics from all nodes and provides query interfaces
- **`IMetricsSink` / `IPipelineMetricsSink`**: Abstractions for emitting metrics to various destinations (logging, monitoring systems, etc.)
- **`IObservabilityFactory`**: DI-aware factory for resolving and configuring observability components

### Thread-Safety Guarantees

The extension provides strong thread-safety guarantees for production environments:

- **Concurrent metrics collection**: Multiple nodes can record metrics simultaneously without race conditions
- **Atomic counter updates**: Item counts use `Interlocked.Add` for thread-safe increments
- **Immutable metric records**: Once built, metric records are immutable and safe to share across threads
- **Scoped isolation**: Each pipeline run gets its own collector instance, preventing cross-contamination between runs
- **ConcurrentDictionary**: Internal metrics storage uses `ConcurrentDictionary` for lock-free operations

## Performance Characteristics

### Minimal Overhead

The extension is designed for production use with minimal performance impact:

- **Non-blocking metrics collection**: Metrics are recorded asynchronously without blocking pipeline execution
- **Efficient data structures**: Uses optimized collections (ConcurrentDictionary) for metrics aggregation
- **Optional observability**: Can be disabled entirely by not registering the services
- **Scoped lifetime**: Metrics are isolated per pipeline run, preventing memory leaks

### Memory Usage

- **Per-pipeline overhead**: Approximately 1-2 KB per node for metrics storage
- **Transient sinks**: Metrics sinks are created per pipeline run and disposed after use
- **No persistent storage**: Metrics are not retained in memory beyond the pipeline execution scope

### CPU Impact

- **Lightweight timing**: Uses high-resolution `Stopwatch` timers with minimal CPU overhead
- **Optional performance counters**: Memory and processor time collection can be disabled if not needed via `ObservabilityOptions.Minimal`
- **Batch-friendly**: Metrics collection scales efficiently with large batch sizes and doesn't degrade with parallelism

## Related Topics

- **[Getting Started](./index.md)**: Quick start and basic usage
- **[Metrics Reference](./metrics.md)**: Detailed metrics documentation
- **[Usage Examples](./examples.md)**: Complete code examples
- **[Advanced Patterns](./advanced-patterns.md)**: Advanced scenarios and custom implementations
- **[Distributed Tracing](./tracing.md)**: Core tracing abstraction
