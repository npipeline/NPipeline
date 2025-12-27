---
title: Troubleshooting
description: Diagnose and resolve common resilience issues in NPipeline with symptom-based troubleshooting and debugging guidance.
sidebar_position: 6
---

# Troubleshooting

>**Related Documentation**
>This guide covers **resilience-specific issues** (retries, node restarts, materialization). For general pipeline execution issues, see [General Troubleshooting](../../reference/troubleshooting.md).

This guide covers diagnosing and resolving common issues with resilience configuration in NPipeline. It provides symptom-based troubleshooting, debugging techniques, and solutions for common anti-patterns.

## Quick Diagnosis: Use Build-Time Validation

**Start here first:** Before debugging at runtime, run the pipeline builder's validation to catch configuration issues early:

```csharp
var builder = new PipelineBuilder();
// ... configure pipeline ...

// Validate before building
var result = builder.Validate();
if (!result.IsValid)
{
    Console.WriteLine("Configuration errors:");
    foreach (var error in result.Errors)
        Console.WriteLine($"  ❌ {error}");
}

if (result.Warnings.Count > 0)
{
    Console.WriteLine("Configuration warnings:");
    foreach (var warning in result.Warnings)
        Console.WriteLine($"  WARNING: {warning}");
}

var pipeline = builder.Build();
```

The **ResilienceConfigurationRule** automatically validates that your resilience setup is complete:

- **Error Handler Registered?** Required to make restart decisions
- **MaxNodeRestartAttempts > 0?** Controls restart capability
- **MaxMaterializedItems Set?** Prevents unbounded memory growth

See [Resilience Configuration Rule Details](../pipeline-validation.md#resilience-configuration-rule-details) for complete validation documentation.

## Symptom-Based Troubleshooting

### Symptom: Node Doesn't Restart Despite Failures

**Possible Causes:**

1. Missing `ResilientExecutionStrategy`
2. No materialization configured for streaming inputs
3. Error handler not returning `RestartNode`
4. Retry limits exhausted

**Diagnostic Steps:**

```csharp
// 1. Check if ResilientExecutionStrategy is applied
var nodeDefinition = pipeline.GetNodeDefinition("problematicNode");
var hasResilientStrategy = nodeDefinition.ExecutionConfig.ExecutionStrategy is ResilientExecutionStrategy;
Console.WriteLine($"Has ResilientExecutionStrategy: {hasResilientStrategy}");

// 2. Check materialization configuration
var retryOptions = context.RetryOptions;
Console.WriteLine($"MaxMaterializedItems: {retryOptions.MaxMaterializedItems}");
if (retryOptions.MaxMaterializedItems == null)
{
    Console.WriteLine("ERROR: No materialization configured for streaming inputs");
}

// 3. Add logging to error handler
public class DebuggingErrorHandler : IPipelineErrorHandler
{
    public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        Console.WriteLine($"Error handler called for node: {nodeId}");
        Console.WriteLine($"Exception type: {error.GetType().Name}");
        Console.WriteLine($"Exception message: {error.Message}");
        
        var decision = await HandleError(nodeId, error, context, ct);
        Console.WriteLine($"Decision: {decision}");
        
        return decision;
    }
}

// 4. Check retry counts
Console.WriteLine($"MaxItemRetries: {retryOptions.MaxItemRetries}");
Console.WriteLine($"MaxNodeRestartAttempts: {retryOptions.MaxNodeRestartAttempts}");
Console.WriteLine($"MaxSequentialNodeAttempts: {retryOptions.MaxSequentialNodeAttempts}");
```

**Solutions:**

```csharp
// Solution 1: Apply ResilientExecutionStrategy
var problematicHandle = builder
    .AddTransform<ProblematicTransform, Input, Output>("problematicNode")
    .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
        new SequentialExecutionStrategy()
    ));

// Solution 2: Configure materialization
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 5
);

// Solution 3: Fix error handler
public class FixedErrorHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        return error switch
        {
            TimeoutException => Task.FromResult(PipelineErrorDecision.RestartNode),
            NetworkException => Task.FromResult(PipelineErrorDecision.RestartNode),
            _ => Task.FromResult(PipelineErrorDecision.FailPipeline)
        };
    }
}
```

### Symptom: OutOfMemoryException with Resilient Nodes

**Possible Causes:**

1. Unbounded materialization (`MaxMaterializedItems = null`)
2. Buffer size too large for available memory
3. Large items being buffered
4. Memory leaks in custom components

**Diagnostic Steps:**

```csharp
// 1. Monitor memory usage
public class MemoryMonitor : IExecutionObserver
{
    public void OnBufferUsage(string nodeId, int currentItems, int maxItems)
    {
        var memoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
        Console.WriteLine($"Node {nodeId}: {currentItems}/{maxItems} items, Memory: {memoryMB}MB");
        
        if (memoryMB > 1000) // 1GB threshold
        {
            Console.WriteLine($"WARNING: High memory usage: {memoryMB}MB");
        }
    }
}

// 2. Check buffer configuration
var retryOptions = context.RetryOptions;
if (retryOptions.MaxMaterializedItems == null)
{
    Console.WriteLine("WARNING: Unbounded materialization may cause memory issues");
}

// 3. Estimate memory requirements
public static long EstimateMemoryUsage(int itemCount, long itemSizeBytes)
{
    return itemCount * itemSizeBytes * 2; // Factor in overhead
}

var estimatedMemory = EstimateMemoryUsage(
    retryOptions.MaxMaterializedItems ?? 0,
    estimatedItemSizeBytes
);
Console.WriteLine($"Estimated memory usage: {estimatedMemory / (1024 * 1024)}MB");
```

**Solutions:**

```csharp
// Solution 1: Set appropriate buffer limits
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 5,
    MaxMaterializedItems: CalculateOptimalBufferSize()
);

private static int CalculateOptimalBufferSize()
{
    var availableMemoryMB = GetAvailableMemoryMB();
    var estimatedItemSizeKB = EstimateItemSize();
    var memoryBudgetMB = availableMemoryMB / 4; // Use 25% of available memory
    return (memoryBudgetMB * 1024) / estimatedItemSizeKB;
}

// Solution 2: Implement memory-aware error handling
public class MemoryAwareErrorHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        // Check memory pressure
        var memoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
        if (memoryMB > 2000) // 2GB threshold
        {
            Console.WriteLine("High memory pressure - avoiding restart");
            return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
        }
        
        return Task.FromResult(PipelineErrorDecision.RestartNode);
    }
}
```

### Symptom: Buffer Overflow Exceptions

**Possible Causes:**

1. `MaxMaterializedItems` set too low
2. High throughput with small buffer
3. Processing bottlenecks downstream
4. Infinite loops in processing logic

**Diagnostic Steps:**

```csharp
// 1. Monitor buffer utilization
public class BufferMonitor : IExecutionObserver
{
    public void OnBufferUsage(string nodeId, int currentItems, int maxItems)
    {
        var usagePercent = (currentItems * 100) / maxItems;
        if (usagePercent > 80)
        {
            Console.WriteLine($"WARNING: Node {nodeId} buffer at {usagePercent}% capacity");
        }
    }
}

// 2. Check throughput vs. processing rate
public class ThroughputMonitor
{
    private readonly Dictionary<string, (int Input, int Output)> _counters = new();
    
    public void RecordInput(string nodeId)
    {
        _counters.TryGetValue(nodeId, out var counter);
        _counters[nodeId] = (counter.Input + 1, counter.Output);
    }
    
    public void RecordOutput(string nodeId)
    {
        _counters.TryGetValue(nodeId, out var counter);
        _counters[nodeId] = (counter.Input, counter.Output + 1);
    }
    
    public void AnalyzeBackpressure()
    {
        foreach (var (nodeId, (input, output)) in _counters)
        {
            var backlog = input - output;
            if (backlog > 1000)
            {
                Console.WriteLine($"WARNING: Node {nodeId} has backlog of {backlog} items");
            }
        }
    }
}
```

**Solutions:**

```csharp
// Solution 1: Increase buffer size
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 5,
    MaxMaterializedItems: 5000 // Increased buffer size
);

// Solution 2: Implement adaptive buffering
public class AdaptiveRetryOptions : PipelineRetryOptions
{
    private int _currentMaxItems;
    
    public AdaptiveRetryOptions() : base(3, 2, 5, 1000)
    {
        _currentMaxItems = MaxMaterializedItems ?? 1000;
    }
    
    public void AdjustBufferSize(int currentUsage, int maxCapacity)
    {
        var usagePercent = (currentUsage * 100) / maxCapacity;
        if (usagePercent > 90)
        {
            _currentMaxItems = (int)(_currentMaxItems * 1.5); // Increase by 50%
            Console.WriteLine($"Increased buffer size to {_currentMaxItems}");
        }
    }
}

// Solution 3: Add circuit breaker for buffer overflow
public class BufferOverflowAwareErrorHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        if (error.Message.Contains("Resilience materialization exceeded MaxMaterializedItems"))
        {
            Console.WriteLine($"Buffer overflow for node {nodeId} - skipping restart");
            return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
        }
        
        return Task.FromResult(PipelineErrorDecision.RestartNode);
    }
}
```

### Symptom: Circuit Breaker Tripping Too Frequently

**Possible Causes:**

1. Failure threshold set too low
2. Persistent infrastructure issues
3. Incorrect error classification
4. Resource contention
5. Wrong threshold type for your scenario (e.g., using ConsecutiveFailures when rate-based would be better)

**Diagnostic Steps:**

```csharp
// 1. Monitor circuit breaker state
public class CircuitBreakerMonitor : IExecutionObserver
{
    public void OnRetry(NodeRetryEvent retryEvent)
    {
        if (retryEvent.RetryKind == RetryKind.NodeRestart)
        {
            Console.WriteLine($"Node restart: {retryEvent.NodeId}, Attempt: {retryEvent.Attempt}");
        }
    }
}

// 2. Check circuit breaker statistics
if (context.Items.TryGetValue(PipelineContextKeys.CircuitBreakerManager, out var managerObj) &&
    managerObj is ICircuitBreakerManager manager)
{
    var circuitBreaker = manager.GetCircuitBreaker(nodeId, circuitBreakerOptions);
    var stats = circuitBreaker.GetStatistics();
    Console.WriteLine($"Circuit breaker stats: {stats.TotalOperations} total, " +
                     $"{stats.FailureCount} failures, {stats.FailureRate:P2} failure rate");
}

// 3. Analyze failure patterns
public class FailureAnalyzer
{
    private readonly Dictionary<string, List<Exception>> _failures = new();
    
    public void RecordFailure(string nodeId, Exception error)
    {
        if (!_failures.ContainsKey(nodeId))
            _failures[nodeId] = new List<Exception>();
        
        _failures[nodeId].Add(error);
        
        // Analyze pattern after 10 failures
        if (_failures[nodeId].Count >= 10)
        {
            AnalyzeFailurePattern(nodeId);
        }
    }
    
    private void AnalyzeFailurePattern(string nodeId)
    {
        var failures = _failures[nodeId];
        var errorTypes = failures.GroupBy(e => e.GetType().Name)
                              .ToDictionary(g => g.Key, g => g.Count());
        
        Console.WriteLine($"Failure pattern for {nodeId}:");
        foreach (var (errorType, count) in errorTypes)
        {
            Console.WriteLine($"  {errorType}: {count} occurrences");
        }
    }
}
```

**Solutions:**

```csharp
// Solution 1: Adjust circuit breaker options
var circuitBreakerOptions = new PipelineCircuitBreakerOptions
{
    Enabled: true,
    FailureThreshold: 10, // Increased from default
    OpenDuration: TimeSpan.FromMinutes(2),
    SamplingWindow: TimeSpan.FromMinutes(5),
    ThresholdType: CircuitBreakerThresholdType.RollingWindowRate, // Try rate-based
    FailureRateThreshold: 0.2 // 20% failure rate
};

// Solution 2: Implement smart error classification
public class SmartErrorHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        // Don't restart for permanent failures
        if (IsPermanentFailure(error))
        {
            return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
        }
        
        // Only restart for transient failures
        if (IsTransientFailure(error))
        {
            return Task.FromResult(PipelineErrorDecision.RestartNode);
        }
        
        return Task.FromResult(PipelineErrorDecision.FailPipeline);
    }
    
    private bool IsPermanentFailure(Exception ex)
    {
        return ex is AuthenticationException or 
               AuthorizationException or 
               NotFoundException;
    }
    
    private bool IsTransientFailure(Exception ex)
    {
        return ex is TimeoutException or 
               NetworkException or 
               TemporaryServiceException;
    }
}

// Solution 3: Try different threshold types based on your scenario
// For high-volume, rate-sensitive scenarios:
var rateBasedOptions = new PipelineCircuitBreakerOptions
{
    FailureThreshold: 100,
    OpenDuration: TimeSpan.FromMinutes(2),
    SamplingWindow: TimeSpan.FromMinutes(5),
    ThresholdType: CircuitBreakerThresholdType.RollingWindowRate,
    FailureRateThreshold: 0.05 // 5% failure rate
};

// For scenarios where both count and rate matter:
var hybridOptions = new PipelineCircuitBreakerOptions
{
    FailureThreshold: 5,
    OpenDuration: TimeSpan.FromMinutes(1),
    SamplingWindow: TimeSpan.FromMinutes(5),
    ThresholdType: CircuitBreakerThresholdType.Hybrid,
    FailureRateThreshold: 0.3 // 30% failure rate
};
```

## Debugging Techniques

### 1. Enable Detailed Logging

```csharp
public class ResilienceLogger : IPipelineLogger
{
    private readonly ILogger _logger;
    
    public ResilienceLogger(ILogger logger)
    {
        _logger = logger;
    }
    
    public void LogDebug(string message, params object[] args)
    {
        _logger.LogDebug(message, args);
    }
    
    public void LogInformation(string message, params object[] args)
    {
        _logger.LogInformation(message, args);
    }
    
    public void LogWarning(string message, params object[] args)
    {
        _logger.LogWarning(message, args);
    }
    
    public void LogError(Exception exception, string message, params object[] args)
    {
        _logger.LogError(exception, message, args);
    }
}

// Configure in context
var context = PipelineContext.WithObservability(
    loggerFactory: new ResilienceLoggerFactory()
);
```

### 2. Add Custom Observability

```csharp
public class ResilienceObserver : IExecutionObserver
{
    public void OnRetry(NodeRetryEvent retryEvent)
    {
        Console.WriteLine($"[{DateTime.UtcNow:O}] Retry: {retryEvent.NodeId}, " +
                         $"Kind: {retryEvent.RetryKind}, " +
                         $"Attempt: {retryEvent.Attempt}, " +
                         $"Error: {retryEvent.Error?.Message}");
    }
    
    public void OnBufferUsage(string nodeId, int currentItems, int maxItems)
    {
        var usagePercent = (currentItems * 100) / maxItems;
        Console.WriteLine($"[{DateTime.UtcNow:O}] Buffer: {nodeId}, " +
                         $"{currentItems}/{maxItems} ({usagePercent}%)");
    }
}

// Register observer
var context = PipelineContext.Default;
context.ExecutionObserver = new ResilienceObserver();
```

### 3. Create Test Scenarios

```csharp
public class ResilienceTestHarness
{
    public async Task TestRestartScenario()
    {
        var flakySource = new FlakyDataSource(failureRate: 0.3);
        var pipeline = CreateResilientPipeline(flakySource);
        
        var context = PipelineContext.WithRetry(new PipelineRetryOptions(
            MaxItemRetries: 3,
            MaxNodeRestartAttempts: 2,
            MaxSequentialNodeAttempts: 5,
            MaxMaterializedItems: 1000
        ));
        
        var result = await pipeline.RunAsync(context);
        
        Assert.IsTrue(result.IsSuccess);
        Assert.Greater(flakySource.ProcessedItems, 0);
    }
    
    public async Task TestBufferOverflowScenario()
    {
        var highVolumeSource = new HighVolumeSource(itemsPerSecond: 1000);
        var pipeline = CreateResilientPipeline(highVolumeSource);
        
        var context = PipelineContext.WithRetry(new PipelineRetryOptions(
            MaxMaterializedItems: 100 // Small buffer to trigger overflow
        ));
        
        try
        {
            await pipeline.RunAsync(context);
            Assert.Fail("Expected buffer overflow exception");
        }
        catch (InvalidOperationException ex) when ex.Message.Contains("MaxMaterializedItems")
        {
            // Expected exception
        }
    }
}
```

## Common Anti-Patterns and Solutions

### Anti-Pattern 1: Blind Retry Everything

```csharp
// WRONG: Retry everything, including permanent failures
public class BlindRetryHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        return Task.FromResult(PipelineErrorDecision.RestartNode); // Always restarts!
    }
}

// CORRECT: Smart error classification
public class SmartRetryHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        return error switch
        {
            // Transient failures - retry
            TimeoutException => Task.FromResult(PipelineErrorDecision.RestartNode),
            NetworkException => Task.FromResult(PipelineErrorDecision.RestartNode),
            HttpRequestException http when IsTransientHttpError(http) => 
                Task.FromResult(PipelineErrorDecision.RestartNode),
            
            // Permanent failures - don't retry
            AuthenticationException => Task.FromResult(PipelineErrorDecision.FailPipeline),
            ValidationException => Task.FromResult(PipelineErrorDecision.Skip),
            NotFoundException => Task.FromResult(PipelineErrorDecision.ContinueWithoutNode),
            
            // Unknown failures - fail safe
            _ => Task.FromResult(PipelineErrorDecision.FailPipeline)
        };
    }
    
    private bool IsTransientHttpError(HttpRequestException ex)
    {
        return ex.StatusCode is HttpStatusCode.ServiceUnavailable or 
               HttpStatusCode.RequestTimeout or 
               HttpStatusCode.TooManyRequests;
    }
}
```

### Anti-Pattern 2: Unbounded Buffering

```csharp
// WRONG: Unbounded materialization
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 5,
    MaxMaterializedItems: null // No limit - potential OOM
);

// CORRECT: Calculated buffer limits
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 5,
    MaxMaterializedItems: CalculateSafeBufferLimit()
);

private static int CalculateSafeBufferLimit()
{
    var availableMemoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
    var estimatedItemSizeKB = 10; // Estimate based on your data
    var memoryBudgetMB = Math.Min(availableMemoryMB / 4, 1000); // Max 1GB
    return (memoryBudgetMB * 1024) / estimatedItemSizeKB;
}
```

### Anti-Pattern 3: Ignoring Memory Pressure

```csharp
// WRONG: No memory awareness
public class MemoryObliviousHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        return Task.FromResult(PipelineErrorDecision.RestartNode); // Always restarts
    }
}

// CORRECT: Memory-aware error handling
public class MemoryAwareHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId, Exception error, PipelineContext context, CancellationToken ct)
    {
        // Check memory pressure
        var memoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
        if (memoryMB > 2000) // 2GB threshold
        {
            Console.WriteLine($"High memory usage ({memoryMB}MB) - avoiding restart");
            return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
        }
        
        return error switch
        {
            TimeoutException => Task.FromResult(PipelineErrorDecision.RestartNode),
            _ => Task.FromResult(PipelineErrorDecision.FailPipeline)
        };
    }
}
```

### Anti-Pattern 4: One-Size-Fits-All Configuration

```csharp
// WRONG: Same configuration for all nodes
var defaultOptions = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 5,
    MaxMaterializedItems: 1000
);
var context = PipelineContext.WithRetry(defaultOptions);

// CORRECT: Node-specific configuration using per-node retry options
var betterDefaultOptions = new PipelineRetryOptions(
    MaxItemRetries: 2,
    MaxNodeRestartAttempts: 1,
    MaxSequentialNodeAttempts: 3,
    MaxMaterializedItems: 500
);
var betterContext = PipelineContext.WithRetry(betterDefaultOptions);

var criticalNodeHandle = builder
    .AddTransform<CriticalTransform, Input, Output>("criticalNode");
var highVolumeHandle = builder
    .AddTransform<HighVolumeTransform, Input, Output>("highVolumeNode");

builder
    .WithRetryOptions(criticalNodeHandle, new PipelineRetryOptions(
        MaxItemRetries: 5,
        MaxNodeRestartAttempts: 5,
        MaxSequentialNodeAttempts: 10,
        MaxMaterializedItems: 2000
    ))
    .WithRetryOptions(highVolumeHandle, new PipelineRetryOptions(
        MaxItemRetries: 1,
        MaxNodeRestartAttempts: 2,
        MaxSequentialNodeAttempts: 4,
        MaxMaterializedItems: 10000
    ));
```

## Monitoring and Alerting

### 1. Key Metrics to Monitor

```csharp
public class ResilienceMetrics
{
    private readonly IMetrics _metrics;
    
    public ResilienceMetrics(IMetrics metrics)
    {
        _metrics = metrics;
    }
    
    public void RecordNodeRestart(string nodeId)
    {
        _metrics.Counter("node_restarts", new[] { ("node_id", nodeId) }).Increment();
    }
    
    public void RecordBufferUsage(string nodeId, int current, int max)
    {
        var usagePercent = (current * 100) / max;
        _metrics.Gauge("buffer_usage_percent", usagePercent, new[] { ("node_id", nodeId) });
    }
    
    public void RecordMemoryUsage(long bytes)
    {
        _metrics.Gauge("memory_usage_bytes", bytes);
    }
    
    public void RecordCircuitBreakerTrip(string nodeId)
    {
        _metrics.Counter("circuit_breaker_trips", new[] { ("node_id", nodeId) }).Increment();
    }
}
```

### 2. Alert Thresholds

| Metric | Warning Threshold | Critical Threshold | Action |
|---------|------------------|-------------------|--------|
| Node restarts per minute | >5 | >10 | Investigate infrastructure |
| Buffer usage percent | >80% | >95% | Increase buffer size |
| Memory usage | >1GB | >2GB | Scale horizontally |
| Circuit breaker trips per hour | >2 | >5 | Review error classification |

### 3. Health Checks

```csharp
public class ResilienceHealthCheck : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var memoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
        var issues = new List<string>();
        
        if (memoryMB > 2000)
            issues.Add($"High memory usage: {memoryMB}MB");
        
        if (GetFailedNodeCount() > 5)
            issues.Add($"High failure rate: {GetFailedNodeCount()} nodes");
        
        return issues.Count == 0
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Degraded(string.Join(", ", issues));
    }
}
```

## Next Steps

- **[Error Handling Guide](error-handling.md)**: Review proper configuration patterns
- **[Getting Started with Resilience](getting-started.md)**: Understand critical prerequisite relationships
- **[Error Codes Reference](../../reference/error-codes.md)**: Look up specific NPipeline error codes (NP01xx-NP05xx)

