---
title: Observability Usage Examples
description: Complete code examples demonstrating various scenarios for using NPipeline Observability extension, from basic setup to advanced monitoring patterns.
---

# Observability Usage Examples

This section provides complete, working code examples for common scenarios when using the NPipeline Observability extension.

## Basic Usage with Automatic Metrics Collection

The simplest way to enable observability is using the `IObservablePipelineContextFactory` with the default registration.

### Example 1: Quick Start with Automatic Metrics

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Pipeline;

// Define a simple pipeline
public class SimplePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<NumberSource, int>();
        var transform = builder.AddTransform<DoubleTransform, int, int>();
        var sink = builder.AddSink<NumberSink, int>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

// Source node that generates numbers
public sealed class NumberSource : SourceNode<int>
{
    public IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        static async IAsyncEnumerable<int> Generate()
        {
            for (int i = 1; i <= 100; i++)
            {
                yield return i;
            }
        }

        return new StreamingDataPipe<int>(Generate());
    }
}

// Transform node that doubles numbers
public sealed class DoubleTransform : TransformNode<int, int>
{
    public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(item * 2);
    }
}

// Sink node that outputs numbers
public sealed class NumberSink : SinkNode<int>
{
    public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, IPipelineActivity parentActivity, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Received: {item}");
        }
    }
}

// Program setup
public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((context, services) =>
            {
                // Add logging
                services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

                // Add observability - automatically sets up metrics collection
                services.AddNPipelineObservability();

                // Register NPipeline core services
                services.AddNPipeline(typeof(Program).Assembly);
            })
            .Build();

        // Create an async scope for proper resource management
        await using var scope = host.Services.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        var contextFactory = scope.ServiceProvider.GetRequiredService<IObservablePipelineContextFactory>();
        var collector = scope.ServiceProvider.GetRequiredService<IObservabilityCollector>();

        // Create context with automatic observability enabled
        await using var pipelineContext = contextFactory.Create();

        // Run the pipeline - metrics are automatically collected!
        await runner.RunAsync<SimplePipeline>(pipelineContext);

        // Display metrics
        Console.WriteLine("\n=== Metrics Collected ===");
        foreach (var metric in collector.GetNodeMetrics())
        {
            Console.WriteLine($"Node {metric.NodeId}:");
            Console.WriteLine($"  Duration: {metric.DurationMs}ms");
            Console.WriteLine($"  Items Processed: {metric.ItemsProcessed}");
            Console.WriteLine($"  Success: {metric.Success}");
        }
    }
}
```

**What happens automatically:**
- `IObservablePipelineContextFactory` creates a context with `ExecutionObserver` pre-configured
- No need to manually create `MetricsCollectingExecutionObserver` or wire it up
- Metrics are collected by the framework during node execution
- Default logging sink outputs metrics to the configured logger

## Custom Metrics Sink Example

Create a custom metrics sink to send metrics to external monitoring systems like Application Insights, Prometheus, or a custom API.

### Example 2: Application Insights Integration

```csharp
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.Metrics;


// Custom sink for Application Insights
public sealed class ApplicationInsightsMetricsSink : IMetricsSink
{
    private readonly TelemetryClient _telemetryClient;

    public ApplicationInsightsMetricsSink(TelemetryClient telemetryClient)
    {
        _telemetryClient = telemetryClient ?? throw new ArgumentNullException(nameof(telemetryClient));
    }

    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        // Create a custom event with metrics
        var properties = new Dictionary<string, string>
        {
            ["NodeId"] = nodeMetrics.NodeId,
            ["Success"] = nodeMetrics.Success.ToString(),
            ["HasException"] = (nodeMetrics.Exception != null).ToString()
        };

        var metrics = new Dictionary<string, double>
        {
            ["DurationMs"] = nodeMetrics.DurationMs ?? 0,
            ["ItemsProcessed"] = nodeMetrics.ItemsProcessed,
            ["ItemsEmitted"] = nodeMetrics.ItemsEmitted,
            ["ThroughputItemsPerSec"] = nodeMetrics.ThroughputItemsPerSec ?? 0,
            ["RetryCount"] = nodeMetrics.RetryCount
        };

        if (nodeMetrics.PeakMemoryUsageMb.HasValue)
        {
            metrics["PeakMemoryUsageMb"] = nodeMetrics.PeakMemoryUsageMb.Value;
        }

        if (nodeMetrics.ProcessorTimeMs.HasValue)
        {
            metrics["ProcessorTimeMs"] = nodeMetrics.ProcessorTimeMs.Value;
        }

        _telemetryClient.TrackEvent("NodeCompleted", properties, metrics);

        // If node failed, track the exception
        if (!nodeMetrics.Success && nodeMetrics.Exception != null)
        {
            _telemetryClient.TrackException(nodeMetrics.Exception, properties);
        }

        return Task.CompletedTask;
    }
}

// Custom pipeline metrics sink
public sealed class ApplicationInsightsPipelineMetricsSink : IPipelineMetricsSink
{
    private readonly TelemetryClient _telemetryClient;

    public ApplicationInsightsPipelineMetricsSink(TelemetryClient telemetryClient)
    {
        _telemetryClient = telemetryClient ?? throw new ArgumentNullException(nameof(telemetryClient));
    }

    public Task RecordAsync(IPipelineMetrics pipelineMetrics, CancellationToken cancellationToken)
    {
        var properties = new Dictionary<string, string>
        {
            ["PipelineName"] = pipelineMetrics.PipelineName,
            ["RunId"] = pipelineMetrics.RunId.ToString(),
            ["Success"] = pipelineMetrics.Success.ToString(),
            ["NodeCount"] = pipelineMetrics.NodeMetrics.Count.ToString()
        };

        var metrics = new Dictionary<string, double>
        {
            ["DurationMs"] = pipelineMetrics.DurationMs ?? 0,
            ["TotalItemsProcessed"] = pipelineMetrics.TotalItemsProcessed
        };

        // Calculate overall throughput
        if (pipelineMetrics.DurationMs.HasValue && pipelineMetrics.DurationMs.Value > 0)
        {
            var throughput = (double)pipelineMetrics.TotalItemsProcessed / (pipelineMetrics.DurationMs.Value / 1000.0);
            metrics["ThroughputItemsPerSec"] = throughput;
        }

        _telemetryClient.TrackEvent("PipelineCompleted", properties, metrics);

        if (!pipelineMetrics.Success && pipelineMetrics.Exception != null)
        {
            _telemetryClient.TrackException(pipelineMetrics.Exception, properties);
        }

        return Task.CompletedTask;
    }
}

// Registration
public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // Configure Application Insights
        var telemetryConfig = TelemetryConfiguration.CreateDefault();
        telemetryConfig.ConnectionString = "InstrumentationKey=YOUR_KEY";
        var telemetryClient = new TelemetryClient(telemetryConfig);
        services.AddSingleton(telemetryClient);

        // Register custom sinks
        services.AddNPipelineObservability<ApplicationInsightsMetricsSink, ApplicationInsightsPipelineMetricsSink>();

        // Register NPipeline
        services.AddNPipeline(typeof(Program).Assembly);

        var serviceProvider = services.BuildServiceProvider();

        await serviceProvider.RunPipelineAsync<MyPipeline>();
    }
}
```

### Example 3: Prometheus Metrics Exporter

```csharp
using Prometheus;
using NPipeline.Observability.Metrics;

// Prometheus metrics definitions
public sealed class PrometheusMetricsSink : IMetricsSink
{
    private static readonly Counter NodeExecutions = Metrics
        .CreateCounter("npipeline_node_executions_total", "Total number of node executions", "node_id", "success");

    private static readonly Histogram NodeDuration = Metrics
        .CreateHistogram("npipeline_node_duration_seconds", "Node execution duration in seconds", "node_id");

    private static readonly Gauge NodeThroughput = Metrics
        .CreateGauge("npipeline_node_throughput_items_per_second", "Node throughput in items per second", "node_id");

    private static readonly Counter NodeRetries = Metrics
        .CreateCounter("npipeline_node_retries_total", "Total number of node retries", "node_id");

    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        // Record execution count
        NodeExecutions.WithLabels(nodeMetrics.NodeId, nodeMetrics.Success.ToString()).Inc();

        // Record duration
        if (nodeMetrics.DurationMs.HasValue)
        {
            NodeDuration.WithLabels(nodeMetrics.NodeId).Observe(nodeMetrics.DurationMs.Value / 1000.0);
        }

        // Record throughput
        if (nodeMetrics.ThroughputItemsPerSec.HasValue)
        {
            NodeThroughput.WithLabels(nodeMetrics.NodeId).Set(nodeMetrics.ThroughputItemsPerSec.Value);
        }

        // Record retries
        if (nodeMetrics.RetryCount > 0)
        {
            NodeRetries.WithLabels(nodeMetrics.NodeId).Inc(nodeMetrics.RetryCount);
        }

        return Task.CompletedTask;
    }
}

// Registration
public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // Start Prometheus metrics server
        var metricServer = new KestrelMetricServer(port: 9090);
        metricServer.Start();

        services.AddNPipelineObservability<PrometheusMetricsSink, LoggingPipelineMetricsSink>();
        services.AddNPipeline(typeof(Program).Assembly);

        var serviceProvider = services.BuildServiceProvider();

        await serviceProvider.RunPipelineAsync<MyPipeline>();

        metricServer.Stop();
    }
}
```

## Dependency Injection Integration Example

Integrate observability with complex DI scenarios, including scoped services and constructor injection.

### Example 4: Pipeline with Scoped Services

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Observability.DependencyInjection;

// Scoped service for database access
public interface IDataRepository
{
    Task SaveAsync<T>(T item);
}

public class DataRepository : IDataRepository
{
    private readonly ILogger<DataRepository> _logger;

    public DataRepository(ILogger<DataRepository> logger)
    {
        _logger = logger;
    }

    public async Task SaveAsync<T>(T item)
    {
        _logger.LogInformation("Saving item of type {ItemType}", typeof(T).Name);
        await Task.Delay(10); // Simulate database operation
    }
}

// Node that uses scoped service
public sealed class DatabaseSink<T> : SinkNode<T>
{
    private readonly IDataRepository _repository;

    public DatabaseSink(IDataRepository repository)
    {
        _repository = repository ?? throw new ArgumentNullException(nameof(repository));
    }

    public async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, IPipelineActivity parentActivity, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            await _repository.SaveAsync(item);
        }
    }
}

// Pipeline definition
public class DataProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, DataItem>();
        var transform = builder.AddTransform<DataTransform, DataItem, ProcessedData>();
        var sink = builder.AddSink<DatabaseSink<ProcessedData>, ProcessedData>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

// Program setup
public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // Add logging
        services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

        // Add observability
        services.AddNPipelineObservability();

        // Register scoped services
        services.AddScoped<IDataRepository, DataRepository>();

        // Register NPipeline with DI
        services.AddNPipeline(typeof(Program).Assembly);

        var serviceProvider = services.BuildServiceProvider();

        // Run pipeline - each run gets its own scoped services
        await serviceProvider.RunPipelineAsync<DataProcessingPipeline>();
    }
}
```

## Advanced Scenarios

### Example 5: Multiple Observers (Composite Pattern)

Use multiple observers to collect different types of metrics or send metrics to multiple destinations.

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Observability.DependencyInjection;

// Custom observer for error tracking
public sealed class ErrorTrackingObserver : IExecutionObserver
{
    private readonly ILogger _logger;

    public ErrorTrackingObserver(ILogger<ErrorTrackingObserver> logger)
    {
        _logger = logger;
    }

    public void OnNodeStarted(NodeExecutionStarted e)
    {
        // No action needed
    }

    public void OnNodeCompleted(NodeExecutionCompleted e)
    {
        if (!e.Success && e.Error != null)
        {
            _logger.LogError(e.Error, "Node {NodeId} failed after {Duration}ms", e.NodeId, e.Duration.TotalMilliseconds);
        }
    }

    public void OnRetry(NodeRetryEvent e)
    {
        _logger.LogWarning("Node {NodeId} retry attempt {Attempt}. Last error: {Error}", 
            e.NodeId, e.Attempt, e.LastException?.Message);
    }

    public void OnDrop(QueueDropEvent e)
    {
        _logger.LogWarning("Dropped {ItemCount} items from queue {QueueId} due to backpressure", 
            e.ItemCount, e.QueueId);
    }

    public void OnQueueMetrics(QueueMetricsEvent e)
    {
        // Track queue depth
        _logger.LogDebug("Queue {QueueId} depth: {Depth}", e.QueueId, e.Depth);
    }
}

// Composite observer that combines multiple observers
public sealed class CompositeObserver : IExecutionObserver
{
    private readonly IEnumerable<IExecutionObserver> _observers;

    public CompositeObserver(IEnumerable<IExecutionObserver> observers)
    {
        _observers = observers ?? throw new ArgumentNullException(nameof(observers));
    }

    public void OnNodeStarted(NodeExecutionStarted e)
    {
        foreach (var observer in _observers)
        {
            observer.OnNodeStarted(e);
        }
    }

    public void OnNodeCompleted(NodeExecutionCompleted e)
    {
        foreach (var observer in _observers)
        {
            observer.OnNodeCompleted(e);
        }
    }

    public void OnRetry(NodeRetryEvent e)
    {
        foreach (var observer in _observers)
        {
            observer.OnRetry(e);
        }
    }

    public void OnDrop(QueueDropEvent e)
    {
        foreach (var observer in _observers)
        {
            observer.OnDrop(e);
        }
    }

    public void OnQueueMetrics(QueueMetricsEvent e)
    {
        foreach (var observer in _observers)
        {
            observer.OnQueueMetrics(e);
        }
    }
}

// Registration
public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

        // Add observability
        services.AddNPipelineObservability();

        // Register custom observers
        services.AddSingleton<IExecutionObserver, ErrorTrackingObserver>();
        services.AddSingleton<IExecutionObserver, MetricsCollectingExecutionObserver>();
        services.AddSingleton<IExecutionObserver, CompositeObserver>();

        services.AddNPipeline(typeof(Program).Assembly);

        var serviceProvider = services.BuildServiceProvider();

        await serviceProvider.RunPipelineAsync<MyPipeline>();
    }
}
```

### Example 6: Custom Metrics Collection with Enrichment

Create a custom collector that enriches metrics with additional context.

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability;
using NPipeline.Observability.Metrics;

// Custom collector that enriches metrics
public sealed class EnrichedObservabilityCollector : IObservabilityCollector
{
    private readonly ObservabilityCollector _baseCollector;
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly ILogger _logger;

    public EnrichedObservabilityCollector(
        IHttpContextAccessor httpContextAccessor,
        ILogger<EnrichedObservabilityCollector> logger)
    {
        _baseCollector = new ObservabilityCollector();
        _httpContextAccessor = httpContextAccessor;
        _logger = logger;
    }

    public void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, long? initialMemoryMb = null)
    {
        var correlationId = _httpContextAccessor.HttpContext?.TraceIdentifier;
        _logger.LogInformation("Node {NodeId} started. CorrelationId: {CorrelationId}", nodeId, correlationId);
        _baseCollector.RecordNodeStart(nodeId, timestamp, threadId, initialMemoryMb);
    }

    public void RecordNodeEnd(string nodeId, DateTimeOffset timestamp, bool success, Exception? exception = null, 
        long? peakMemoryMb = null, long? processorTimeMs = null)
    {
        _baseCollector.RecordNodeEnd(nodeId, timestamp, success, exception, peakMemoryMb, processorTimeMs);
    }

    public void RecordItemMetrics(string nodeId, long itemsProcessed, long itemsEmitted)
    {
        _baseCollector.RecordItemMetrics(nodeId, itemsProcessed, itemsEmitted);
    }

    public void RecordRetry(string nodeId, int retryCount, string? reason = null)
    {
        _logger.LogWarning("Node {NodeId} retry {RetryCount}. Reason: {Reason}", nodeId, retryCount, reason);
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
        var baseMetrics = _baseCollector.CreatePipelineMetrics(pipelineName, runId, startTime, endTime, success, exception);
        
        // Enrich with additional context
        var correlationId = _httpContextAccessor.HttpContext?.TraceIdentifier;
        _logger.LogInformation("Pipeline {PipelineName} completed. CorrelationId: {CorrelationId}", 
            pipelineName, correlationId);
        
        return baseMetrics;
    }
}

// Registration
public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());
        services.AddHttpContextAccessor();

        // Register custom collector
        services.AddNPipelineObservability<EnrichedObservabilityCollector, LoggingMetricsSink, LoggingPipelineMetricsSink>();

        services.AddNPipeline(typeof(Program).Assembly);

        var serviceProvider = services.BuildServiceProvider();

        await serviceProvider.RunPipelineAsync<MyPipeline>();
    }
}
```

### Example 7: Performance Monitoring with Alerts

Create a metrics sink that monitors performance and triggers alerts when thresholds are exceeded.

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.Metrics;

// Configuration for performance thresholds
public sealed class PerformanceThresholds
{
    public double MaxNodeDurationMs { get; set; } = 5000;
    public double MinThroughputItemsPerSec { get; set; } = 100;
    public int MaxRetryCount { get; set; } = 3;
}

// Metrics sink with alerting
public sealed class AlertingMetricsSink : IMetricsSink
{
    private readonly PerformanceThresholds _thresholds;
    private readonly ILogger _logger;

    public AlertingMetricsSink(PerformanceThresholds thresholds, ILogger<AlertingMetricsSink> logger)
    {
        _thresholds = thresholds ?? throw new ArgumentNullException(nameof(thresholds));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        // Check for slow nodes
        if (nodeMetrics.DurationMs.HasValue && nodeMetrics.DurationMs.Value > _thresholds.MaxNodeDurationMs)
        {
            _logger.LogWarning(
                "PERFORMANCE ALERT: Node {NodeId} exceeded duration threshold. " +
                "Actual: {ActualMs}ms, Threshold: {ThresholdMs}ms",
                nodeMetrics.NodeId,
                nodeMetrics.DurationMs.Value,
                _thresholds.MaxNodeDurationMs);
        }

        // Check for low throughput
        if (nodeMetrics.ThroughputItemsPerSec.HasValue && 
            nodeMetrics.ThroughputItemsPerSec.Value < _thresholds.MinThroughputItemsPerSec)
        {
            _logger.LogWarning(
                "PERFORMANCE ALERT: Node {NodeId} below throughput threshold. " +
                "Actual: {ActualItems}/sec, Threshold: {ThresholdItems}/sec",
                nodeMetrics.NodeId,
                nodeMetrics.ThroughputItemsPerSec.Value,
                _thresholds.MinThroughputItemsPerSec);
        }

        // Check for excessive retries
        if (nodeMetrics.RetryCount > _thresholds.MaxRetryCount)
        {
            _logger.LogWarning(
                "RELIABILITY ALERT: Node {NodeId} exceeded retry threshold. " +
                "Actual: {ActualRetries}, Threshold: {ThresholdRetries}",
                nodeMetrics.NodeId,
                nodeMetrics.RetryCount,
                _thresholds.MaxRetryCount);
        }

        return Task.CompletedTask;
    }
}

// Registration
public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

        // Configure thresholds
        var thresholds = new PerformanceThresholds
        {
            MaxNodeDurationMs = 3000,
            MinThroughputItemsPerSec = 50,
            MaxRetryCount = 2
        };
        services.AddSingleton(thresholds);

        // Register alerting sink
        services.AddNPipelineObservability<AlertingMetricsSink, LoggingPipelineMetricsSink>();

        services.AddNPipeline(typeof(Program).Assembly);

        var serviceProvider = services.BuildServiceProvider();

        await serviceProvider.RunPipelineAsync<MyPipeline>();
    }
}
```

### Example 8: Error Tracking and Analysis

Track and analyze errors across pipeline executions to identify patterns.

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.Metrics;
using System.Collections.Concurrent;

// Error tracker for analyzing patterns
public sealed class ErrorTracker
{
    private readonly ConcurrentDictionary<string, ErrorStats> _errorStats = new();

    public void TrackError(string nodeId, Exception exception)
    {
        var errorType = exception.GetType().Name;
        var stats = _errorStats.GetOrAdd(errorType, _ => new ErrorStats());
        stats.Increment(nodeId);
    }

    public IEnumerable<ErrorStats> GetErrorStats()
    {
        return _errorStats.Values.OrderByDescending(s => s.Count);
    }
}

public sealed class ErrorStats
{
    private readonly ConcurrentDictionary<string, int> _nodeCounts = new();

    public int Count { get; private set; }
    public string? MostFrequentNode { get; private set; }

    public void Increment(string nodeId)
    {
        Interlocked.Increment(ref Count);
        _nodeCounts.AddOrUpdate(nodeId, 1, (_, count) => count + 1);

        MostFrequentNode = _nodeCounts
            .OrderByDescending(kvp => kvp.Value)
            .FirstOrDefault().Key;
    }
}

// Metrics sink that tracks errors
public sealed class ErrorTrackingMetricsSink : IMetricsSink
{
    private readonly ErrorTracker _errorTracker;
    private readonly ILogger _logger;

    public ErrorTrackingMetricsSink(ErrorTracker errorTracker, ILogger<ErrorTrackingMetricsSink> logger)
    {
        _errorTracker = errorTracker ?? throw new ArgumentNullException(nameof(errorTracker));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        if (!nodeMetrics.Success && nodeMetrics.Exception != null)
        {
            _errorTracker.TrackError(nodeMetrics.NodeId, nodeMetrics.Exception);
            _logger.LogError(
                nodeMetrics.Exception,
                "Error in node {NodeId}: {ErrorMessage}",
                nodeMetrics.NodeId,
                nodeMetrics.Exception.Message);
        }

        return Task.CompletedTask;
    }
}

// Registration
public class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());

        // Register error tracker as singleton to persist across runs
        services.AddSingleton<ErrorTracker>();

        // Register error tracking sink
        services.AddNPipelineObservability<ErrorTrackingMetricsSink, LoggingPipelineMetricsSink>();

        services.AddNPipeline(typeof(Program).Assembly);

        var serviceProvider = services.BuildServiceProvider();

        // Run pipeline multiple times to collect error patterns
        for (int i = 0; i < 10; i++)
        {
            await serviceProvider.RunPipelineAsync<MyPipeline>();
        }

        // Analyze error patterns
        var errorTracker = serviceProvider.GetRequiredService<ErrorTracker>();
        foreach (var errorStat in errorTracker.GetErrorStats())
        {
            Console.WriteLine($"Error Type: {errorStat.GetType().Name}, Count: {errorStat.Count}, " +
                $"Most Frequent Node: {errorStat.MostFrequentNode}");
        }
    }
}
```

## Complete Working Example

### Example 9: End-to-End ETL Pipeline with Observability

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Observability.DependencyInjection;
using NPipeline.Pipeline;

// Data models
public sealed record RawData(int Id, string Value);
public sealed record ProcessedData(int Id, string Value, DateTime ProcessedAt);

// Pipeline definition
public sealed class EtlPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<CsvDataSource, RawData>();
        var validate = builder.AddTransform<DataValidationTransform, RawData, RawData>();
        var transform = builder.AddTransform<DataTransform, RawData, ProcessedData>();
        var sink = builder.AddSink<DatabaseSink, ProcessedData>();

        builder.Connect(source, validate);
        builder.Connect(validate, transform);
        builder.Connect(transform, sink);
    }
}

// Source: Read from CSV
public sealed class CsvDataSource : SourceNode<RawData>
{
    public IDataPipe<RawData> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        static async IAsyncEnumerable<RawData> ReadFromCsv()
        {
            // Simulate reading from CSV file
            for (int i = 1; i <= 1000; i++)
            {
                await Task.Delay(1, cancellationToken);
                yield return new RawData(i, $"Value_{i}");
            }
        }

        return new StreamingDataPipe<RawData>(ReadFromCsv());
    }
}

// Transform: Validate data
public sealed class DataValidationTransform : TransformNode<RawData, RawData>
{
    public override Task<RawData> ExecuteAsync(RawData item, PipelineContext context, CancellationToken cancellationToken = default)
    {
        // Simulate validation
        if (item.Id <= 0)
        {
            throw new ArgumentException($"Invalid ID: {item.Id}");
        }

        return Task.FromResult(item);
    }
}

// Transform: Process data
public sealed class DataTransform : TransformNode<RawData, ProcessedData>
{
    public override Task<ProcessedData> ExecuteAsync(RawData item, PipelineContext context, CancellationToken cancellationToken = default)
    {
        var processed = new ProcessedData(
            item.Id,
            item.Value.ToUpper(),
            DateTime.UtcNow);

        return Task.FromResult(processed);
    }
}

// Sink: Write to database
public sealed class DatabaseSink : SinkNode<ProcessedData>
{
    private readonly ILogger<DatabaseSink> _logger;

    public DatabaseSink(ILogger<DatabaseSink> logger)
    {
        _logger = logger;
    }

    public async Task ExecuteAsync(IDataPipe<ProcessedData> input, PipelineContext context, 
        IPipelineActivity parentActivity, CancellationToken cancellationToken = default)
    {
        int count = 0;
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            // Simulate database write
            await Task.Delay(1, cancellationToken);
            count++;
        }

        _logger.LogInformation("Wrote {Count} items to database", count);
    }
}

// Program
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // Configure logging
        services.AddLogging(loggingBuilder =>
        {
            loggingBuilder.AddConsole();
            loggingBuilder.SetMinimumLevel(LogLevel.Information);
        });

        // Add observability with default logging sinks
        services.AddNPipelineObservability();

        // Register NPipeline
        services.AddNPipeline(typeof(Program).Assembly);

        var serviceProvider = services.BuildServiceProvider();

        Console.WriteLine("Starting ETL Pipeline with Observability...");
        Console.WriteLine();

        // Run the pipeline
        await serviceProvider.RunPipelineAsync<EtlPipeline>();

        Console.WriteLine();
        Console.WriteLine("Pipeline execution completed. Check logs for detailed metrics.");
    }
}
```

## Related Topics

- **[Observability Overview](./index.md)**: Introduction to observability features
- **[Configuration Guide](./configuration.md)**: Setup and configuration options
- **[Metrics Reference](./metrics.md)**: Detailed metrics documentation