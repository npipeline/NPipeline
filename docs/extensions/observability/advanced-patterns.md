# Advanced Observability Patterns

This guide covers advanced patterns and real-world scenarios for the NPipeline Observability extension.

## Custom Metrics Collectors

### Implementing a Custom Collector

Create custom collectors when you need to track additional metadata or metrics beyond the standard interface. This example captures host information and process details for distributed tracing scenarios.

```csharp
public class DetailedObservabilityCollector : IObservabilityCollector
{
    private readonly ConcurrentDictionary<string, DetailedNodeMetrics> _metrics = new();
    private readonly ILogger<DetailedObservabilityCollector> _logger;
    
    public DetailedObservabilityCollector(ILogger<DetailedObservabilityCollector> logger)
    {
        _logger = logger;
    }
    
    public void RecordNodeStart(string nodeId, DateTimeOffset timestamp, int? threadId = null, long? initialMemoryMb = null)
    {
        var detailed = new DetailedNodeMetrics
        {
            NodeId = nodeId,
            StartTime = timestamp,
            ThreadId = threadId,
            InitialMemoryMb = initialMemoryMb,
            // Add custom tracking
            HostName = Environment.MachineName,
            ProcessId = Environment.ProcessId
        };
        
        _metrics.TryAdd(nodeId, detailed);
        _logger.LogDebug("Node {NodeId} started on host {HostName}", nodeId, detailed.HostName);
    }
    
    // Implement other IObservabilityCollector methods...
}
```

### Using Custom Collector

Register your custom collector in the dependency injection container to replace the default implementation with your specialized version.

```csharp
services.AddNPipelineObservability<
    DetailedObservabilityCollector,
    LoggingMetricsSink,
    LoggingPipelineMetricsSink>();
```

## Integration with Monitoring Systems

### Application Insights Integration

Send pipeline metrics directly to Azure Application Insights for cloud-native monitoring, alerting, and analysis within the Azure ecosystem.

```csharp
public class ApplicationInsightsMetricsSink : IMetricsSink
{
    private readonly TelemetryClient _telemetryClient;
    
    public ApplicationInsightsMetricsSink(TelemetryClient telemetryClient)
    {
        _telemetryClient = telemetryClient;
    }
    
    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        var properties = new Dictionary<string, string>
        {
            ["NodeId"] = nodeMetrics.NodeId,
            ["Success"] = nodeMetrics.Success.ToString(),
            ["ThreadId"] = nodeMetrics.ThreadId?.ToString() ?? "N/A"
        };
        
        var metrics = new Dictionary<string, double>();
        
        if (nodeMetrics.DurationMs.HasValue)
            metrics["DurationMs"] = nodeMetrics.DurationMs.Value;
            
        if (nodeMetrics.ThroughputItemsPerSec.HasValue)
            metrics["ThroughputItemsPerSec"] = nodeMetrics.ThroughputItemsPerSec.Value;
        
        _telemetryClient.TrackEvent($"NodeExecution_{nodeMetrics.NodeId}", properties, metrics);
        
        if (!nodeMetrics.Success && nodeMetrics.Exception != null)
        {
            _telemetryClient.TrackException(nodeMetrics.Exception);
        }
        
        return Task.CompletedTask;
    }
}
```

### Prometheus Integration

Expose metrics in Prometheus format for integration with Grafana dashboards, alerting rules, and time-series analysis in Kubernetes and on-premises environments.

```csharp
public class PrometheusMetricsSink : IMetricsSink
{
    private static readonly Counter ItemsProcessedCounter = Metrics
        .CreateCounter("npipeline_items_processed_total", "Total items processed", "node_id");
    
    private static readonly Histogram ExecutionDurationHistogram = Metrics
        .CreateHistogram("npipeline_execution_duration_seconds", "Node execution duration", "node_id");
    
    private static readonly Gauge ActiveNodesGauge = Metrics
        .CreateGauge("npipeline_active_nodes", "Currently executing nodes");
    
    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        ItemsProcessedCounter.WithLabels(nodeMetrics.NodeId).Inc(nodeMetrics.ItemsProcessed);
        
        if (nodeMetrics.DurationMs.HasValue)
        {
            ExecutionDurationHistogram.WithLabels(nodeMetrics.NodeId)
                .Observe(nodeMetrics.DurationMs.Value / 1000.0);
        }
        
        return Task.CompletedTask;
    }
}

// Configure in DI
services.AddNPipelineObservability<PrometheusMetricsSink, PrometheusMetricsSink>();
```

### OpenTelemetry Integration

Use the OpenTelemetry standards for vendor-agnostic instrumentation, allowing you to switch monitoring backends without code changes.

```csharp
public class OpenTelemetryMetricsSink : IMetricsSink
{
    private readonly Meter _meter;
    private readonly Counter<long> _itemsProcessed;
    private readonly Histogram<double> _executionDuration;
    
    public OpenTelemetryMetricsSink(IMeterFactory meterFactory)
    {
        _meter = meterFactory.Create("NPipeline.Observability");
        _itemsProcessed = _meter.CreateCounter<long>("npipeline.items.processed");
        _executionDuration = _meter.CreateHistogram<double>("npipeline.execution.duration");
    }
    
    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        var tags = new TagList
        {
            { "node.id", nodeMetrics.NodeId },
            { "node.success", nodeMetrics.Success }
        };
        
        _itemsProcessed.Add(nodeMetrics.ItemsProcessed, tags);
        
        if (nodeMetrics.DurationMs.HasValue)
        {
            _executionDuration.Record(nodeMetrics.DurationMs.Value, tags);
        }
        
        return Task.CompletedTask;
    }
}
```

## Batching and Buffering

### Buffered Metrics Sink

Buffer metrics in an in-memory channel before sending them to the underlying sink, reducing network calls and improving throughput for high-volume pipelines.

```csharp
public class BufferedMetricsSink : IMetricsSink, IAsyncDisposable
{
    private readonly Channel<INodeMetrics> _channel;
    private readonly Task _processingTask;
    private readonly IMetricsSink _underlyingSink;
    
    public BufferedMetricsSink(IMetricsSink underlyingSink, int bufferSize = 1000)
    {
        _underlyingSink = underlyingSink;
        _channel = Channel.CreateBounded<INodeMetrics>(bufferSize);
        _processingTask = ProcessBatchesAsync();
    }
    
    public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        await _channel.Writer.WriteAsync(nodeMetrics, cancellationToken);
    }
    
    private async Task ProcessBatchesAsync()
    {
        await foreach (var metrics in _channel.Reader.ReadAllAsync())
        {
            try
            {
                await _underlyingSink.RecordAsync(metrics, CancellationToken.None);
            }
            catch (Exception ex)
            {
                // Log error but don't stop processing
                Console.Error.WriteLine($"Error recording metrics: {ex.Message}");
            }
        }
    }
    
    public async ValueTask DisposeAsync()
    {
        _channel.Writer.Complete();
        await _processingTask;
    }
}
```

## Sampling Strategies

### Rate-Limited Sink

Enforce a maximum metrics submission rate to prevent overwhelming downstream systems while maintaining visibility into pipeline behavior.

```csharp
public class RateLimitedMetricsSink : IMetricsSink
{
    private readonly IMetricsSink _underlyingSink;
    private readonly SemaphoreSlim _rateLimiter;
    private readonly Timer _resetTimer;
    
    public RateLimitedMetricsSink(IMetricsSink underlyingSink, int maxPerSecond = 100)
    {
        _underlyingSink = underlyingSink;
        _rateLimiter = new SemaphoreSlim(maxPerSecond, maxPerSecond);
        _resetTimer = new Timer(_ => ResetLimit(), null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
    }
    
    public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        if (await _rateLimiter.WaitAsync(0, cancellationToken))
        {
            await _underlyingSink.RecordAsync(nodeMetrics, cancellationToken);
        }
        // Else: silently drop the metric
    }
    
    private void ResetLimit()
    {
        while (_rateLimiter.CurrentCount < 100)
        {
            _rateLimiter.Release();
        }
    }
}
```

### Sampling Sink

Reduce metric volume by probabilistically recording only a sample of successful operations while always capturing failures for debugging.

```csharp
public class SamplingMetricsSink : IMetricsSink
{
    private readonly IMetricsSink _underlyingSink;
    private readonly double _sampleRate; // 0.0 to 1.0
    private readonly Random _random = new();
    
    public SamplingMetricsSink(IMetricsSink underlyingSink, double sampleRate = 0.1)
    {
        _underlyingSink = underlyingSink;
        _sampleRate = Math.Clamp(sampleRate, 0.0, 1.0);
    }
    
    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        // Always record failures
        if (!nodeMetrics.Success)
        {
            return _underlyingSink.RecordAsync(nodeMetrics, cancellationToken);
        }
        
        // Sample successes
        if (_random.NextDouble() < _sampleRate)
        {
            return _underlyingSink.RecordAsync(nodeMetrics, cancellationToken);
        }
        
        return Task.CompletedTask;
    }
}
```

## Composite Sinks

### Multi-Destination Sink

Send metrics to multiple monitoring systems simultaneously (e.g., Prometheus and Application Insights) with error isolation so one sink's failure doesn't affect others.

```csharp
public class CompositeMetricsSink : IMetricsSink
{
    private readonly IReadOnlyList<IMetricsSink> _sinks;
    
    public CompositeMetricsSink(params IMetricsSink[] sinks)
    {
        _sinks = sinks;
    }
    
    public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        var tasks = _sinks.Select(sink => 
            RecordWithErrorHandling(sink, nodeMetrics, cancellationToken));
        
        await Task.WhenAll(tasks);
    }
    
    private async Task RecordWithErrorHandling(
        IMetricsSink sink,
        INodeMetrics nodeMetrics,
        CancellationToken cancellationToken)
    {
        try
        {
            await sink.RecordAsync(nodeMetrics, cancellationToken);
        }
        catch (Exception ex)
        {
            // Log but don't fail the entire operation
            Console.Error.WriteLine($"Sink {sink.GetType().Name} failed: {ex.Message}");
        }
    }
}

// Usage
services.AddNPipelineObservability(
    sp => new CompositeMetricsSink(
        sp.GetRequiredService<LoggingMetricsSink>(),
        sp.GetRequiredService<PrometheusMetricsSink>(),
        sp.GetRequiredService<ApplicationInsightsMetricsSink>()
    ),
    sp => new LoggingPipelineMetricsSink(sp.GetService<ILogger<LoggingPipelineMetricsSink>>()));
```

## Performance Optimization

### Pre-Aggregation Sink

Aggregate metrics from multiple executions of the same node before flushing, significantly reducing the number of events sent to downstream systems.

```csharp
public class PreAggregationMetricsSink : IMetricsSink
{
    private readonly ConcurrentDictionary<string, AggregatedMetrics> _aggregates = new();
    private readonly Timer _flushTimer;
    private readonly IMetricsSink _underlyingSink;
    
    public PreAggregationMetricsSink(IMetricsSink underlyingSink, TimeSpan flushInterval)
    {
        _underlyingSink = underlyingSink;
        _flushTimer = new Timer(_ => FlushAggregates(), null, flushInterval, flushInterval);
    }
    
    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        _aggregates.AddOrUpdate(
            nodeMetrics.NodeId,
            _ => new AggregatedMetrics(nodeMetrics),
            (_, existing) => existing.Merge(nodeMetrics));
        
        return Task.CompletedTask;
    }
    
    private async void FlushAggregates()
    {
        var snapshots = _aggregates.ToArray();
        _aggregates.Clear();
        
        foreach (var (nodeId, aggregate) in snapshots)
        {
            var aggregatedMetrics = aggregate.ToNodeMetrics();
            await _underlyingSink.RecordAsync(aggregatedMetrics, CancellationToken.None);
        }
    }
}
```

## Error Handling and Resilience

### Retry Sink

Automatically retry failed metric submissions with exponential backoff to handle transient failures in downstream monitoring systems.

```csharp
public class RetrySink : IMetricsSink
{
    private readonly IMetricsSink _underlyingSink;
    private readonly int _maxRetries;
    private readonly TimeSpan _retryDelay;
    
    public RetrySink(IMetricsSink underlyingSink, int maxRetries = 3)
    {
        _underlyingSink = underlyingSink;
        _maxRetries = maxRetries;
        _retryDelay = TimeSpan.FromMilliseconds(100);
    }
    
    public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        for (int attempt = 0; attempt <= _maxRetries; attempt++)
        {
            try
            {
                await _underlyingSink.RecordAsync(nodeMetrics, cancellationToken);
                return;
            }
            catch (Exception) when (attempt < _maxRetries)
            {
                await Task.Delay(_retryDelay * (attempt + 1), cancellationToken);
            }
        }
    }
}
```

### Circuit Breaker Sink

Stop attempting to send metrics when a monitoring system repeatedly fails, preventing cascading failures and allowing time for recovery.

```csharp
public class CircuitBreakerSink : IMetricsSink
{
    private readonly IMetricsSink _underlyingSink;
    private int _failureCount;
    private DateTime _lastFailure;
    private const int FailureThreshold = 5;
    private static readonly TimeSpan RecoveryTimeout = TimeSpan.FromMinutes(1);
    
    public CircuitBreakerSink(IMetricsSink underlyingSink)
    {
        _underlyingSink = underlyingSink;
    }
    
    public async Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        if (IsCircuitOpen())
        {
            // Circuit is open, don't attempt
            return;
        }
        
        try
        {
            await _underlyingSink.RecordAsync(nodeMetrics, cancellationToken);
            ResetFailureCount();
        }
        catch (Exception)
        {
            IncrementFailureCount();
            throw;
        }
    }
    
    private bool IsCircuitOpen()
    {
        if (_failureCount >= FailureThreshold)
        {
            if (DateTime.UtcNow - _lastFailure > RecoveryTimeout)
            {
                ResetFailureCount();
                return false;
            }
            return true;
        }
        return false;
    }
    
    private void IncrementFailureCount()
    {
        Interlocked.Increment(ref _failureCount);
        _lastFailure = DateTime.UtcNow;
    }
    
    private void ResetFailureCount()
    {
        Interlocked.Exchange(ref _failureCount, 0);
    }
}
```

## Testing Custom Implementations

### Test Sink

Capture all recorded metrics in memory during unit tests to verify observability behavior and assert on pipeline execution details.

```csharp
public class TestMetricsSink : IMetricsSink
{
    public List<INodeMetrics> RecordedMetrics { get; } = new();
    public int CallCount => RecordedMetrics.Count;
    
    public Task RecordAsync(INodeMetrics nodeMetrics, CancellationToken cancellationToken)
    {
        RecordedMetrics.Add(nodeMetrics);
        return Task.CompletedTask;
    }
    
    public void Clear() => RecordedMetrics.Clear();
}

// Usage in tests
[Fact]
public async Task Pipeline_Should_RecordMetrics()
{
    var testSink = new TestMetricsSink();
    services.AddSingleton<IMetricsSink>(testSink);
    services.AddNPipelineObservability<TestMetricsSink, TestMetricsSink>();
    
    // Run pipeline...
    
    Assert.True(testSink.CallCount > 0);
    Assert.All(testSink.RecordedMetrics, m => Assert.True(m.Success));
}
```

## Best Practices

1. **Async All The Way**: Always implement sinks as truly async to avoid blocking
2. **Error Isolation**: Never let sink failures affect pipeline execution
3. **Buffering**: Use buffering for high-throughput scenarios
4. **Sampling**: Consider sampling for extremely high-volume pipelines
5. **Monitoring**: Monitor your monitoring - track sink performance
6. **Testing**: Test sinks in isolation with realistic metrics volumes
7. **Graceful Degradation**: Have fallback strategies when primary sinks fail

## See Also

- [Getting Started](./index.md) - Basic setup and usage
- [Configuration](./configuration.md) - DI configuration options
- [Metrics Reference](./metrics.md) - Complete metrics documentation
