---
title: Circuit Breaker Advanced Configuration
description: Advanced circuit breaker configuration patterns and examples for production scenarios in NPipeline.
sidebar_position: 3
---

# Circuit Breaker Advanced Configuration

This guide covers advanced circuit breaker configuration patterns for production scenarios. For basic configuration, see [Circuit Breaker Configuration](circuit-breaker-configuration.md).

## Overview

Circuit breakers in NPipeline protect your pipelines from cascading failures by temporarily blocking operations when failure thresholds are exceeded. This guide shows how to configure breakers for different scenarios.

## Basic Configuration

The simplest way to add circuit breaker protection is with the fluent API:

```csharp
var builder = new PipelineBuilder();

// Simple configuration: trip after 3 consecutive failures
builder.WithCircuitBreaker(
    failureThreshold: 3,
    openDuration: TimeSpan.FromMinutes(1),
    samplingWindow: TimeSpan.FromMinutes(5)
);
```

## Circuit Breaker Threshold Types

NPipeline supports four different threshold types for detecting failures:

### 1. Consecutive Failures (Default)

Tracks consecutive failures without considering time:

```csharp
// Trip after any 3 consecutive failures
builder.WithCircuitBreaker(
    failureThreshold: 3,
    openDuration: TimeSpan.FromMinutes(1),
    samplingWindow: TimeSpan.FromMinutes(5)
);
```

**Use when:** You want simple, immediate failure detection. Best for services where even a few failures indicate a problem.

### 2. Rolling Window Count

Tracks total failures within a time window:

```csharp
// Trip after 10 failures within a 5-minute window
var options = new PipelineCircuitBreakerOptions(
    FailureThreshold: 10,
    OpenDuration: TimeSpan.FromMinutes(2),
    SamplingWindow: TimeSpan.FromMinutes(5),
    Enabled: true,
    ThresholdType: CircuitBreakerThresholdType.RollingWindowCount
);
```

**Use when:** You care about the absolute number of failures in a time period, regardless of how they're distributed.

### 3. Rolling Window Rate

Tracks failure rate (failures/total operations) within a time window:

```csharp
// Trip if more than 10% of operations fail in a 5-minute window
var options = new PipelineCircuitBreakerOptions(
    FailureThreshold: 100,  // Minimum operations before rate is evaluated
    OpenDuration: TimeSpan.FromMinutes(2),
    SamplingWindow: TimeSpan.FromMinutes(5),
    Enabled: true,
    ThresholdType: CircuitBreakerThresholdType.RollingWindowRate,
    FailureRateThreshold: 0.1  // 10% failure rate
);
```

**Use when:** You want to tolerate occasional failures but trip on consistent degradation.

### 4. Hybrid

Combines count and rate thresholds - trips if either is exceeded:

```csharp
// Trip if EITHER 5 failures in window OR 30% failure rate
var options = new PipelineCircuitBreakerOptions(
    FailureThreshold: 5,
    OpenDuration: TimeSpan.FromMinutes(1),
    SamplingWindow: TimeSpan.FromMinutes(5),
    Enabled: true,
    ThresholdType: CircuitBreakerThresholdType.Hybrid,
    FailureRateThreshold: 0.3  // 30% failure rate
);
```

**Use when:** You want comprehensive protection against both sudden failures and gradual degradation.

## Recovery Configuration

Control how your circuit breaker recovers after tripping:

```csharp
var options = new PipelineCircuitBreakerOptions(
    FailureThreshold: 3,
    OpenDuration: TimeSpan.FromMinutes(1),
    SamplingWindow: TimeSpan.FromMinutes(5),
    Enabled: true,
    HalfOpenSuccessThreshold: 2,     // Require 2 consecutive successes
    HalfOpenMaxAttempts: 5            // Allow up to 5 attempts in Half-Open
);
```

**State transitions:**
- **Closed → Open**: Failure threshold exceeded
- **Open → Half-Open**: After `OpenDuration` expires
- **Half-Open → Closed**: After `HalfOpenSuccessThreshold` consecutive successes
- **Half-Open → Open**: On any failure, or exceeding `HalfOpenMaxAttempts`

## Real-World Scenarios

### Scenario 1: High-Throughput API Integration

For operations calling external APIs with occasional glitches:

```csharp
builder.WithCircuitBreaker(
    failureThreshold: 50,  // Allow some failures before tripping
    openDuration: TimeSpan.FromSeconds(30),  // Quick recovery window
    samplingWindow: TimeSpan.FromMinutes(5)
);
```

### Scenario 2: Critical Database Operations

For operations where any failure indicates a serious problem:

```csharp
var options = new PipelineCircuitBreakerOptions(
    FailureThreshold: 2,
    OpenDuration: TimeSpan.FromSeconds(10),
    SamplingWindow: TimeSpan.FromMinutes(1),
    Enabled: true,
    ThresholdType: CircuitBreakerThresholdType.ConsecutiveFailures,
    HalfOpenSuccessThreshold: 1,
    HalfOpenMaxAttempts: 2
);
```

### Scenario 3: Bursty Service with Degradation Tolerance

For services that can tolerate occasional failures but need protection from persistent issues:

```csharp
var options = new PipelineCircuitBreakerOptions(
    FailureThreshold: 100,
    OpenDuration: TimeSpan.FromMinutes(2),
    SamplingWindow: TimeSpan.FromMinutes(5),
    Enabled: true,
    ThresholdType: CircuitBreakerThresholdType.RollingWindowRate,
    FailureRateThreshold: 0.2  // 20% failure rate threshold
);
```

### Scenario 4: Multi-Level Protection

Combine multiple strategies across pipeline:

```csharp
var builder = new PipelineBuilder();

// Strict protection on critical transform
var criticalOptions = new PipelineCircuitBreakerOptions(
    FailureThreshold: 2,
    OpenDuration: TimeSpan.FromSeconds(30),
    SamplingWindow: TimeSpan.FromMinutes(1),
    Enabled: true
);

// Lenient protection on resilient transform
var resilientOptions = new PipelineCircuitBreakerOptions(
    FailureThreshold: 100,
    OpenDuration: TimeSpan.FromMinutes(5),
    SamplingWindow: TimeSpan.FromMinutes(10),
    Enabled: true,
    ThresholdType: CircuitBreakerThresholdType.RollingWindowRate,
    FailureRateThreshold: 0.25
);
```

## Memory Management and Cleanup

Circuit breaker instances are cached per node ID to maintain state across calls. In long-running applications or pipelines with many dynamic nodes, inactive breakers should be cleaned up to prevent memory leaks.

### Default Behavior

By default, NPipeline automatically manages circuit breaker memory:

- **Cleanup interval**: 5 minutes
- **Inactivity threshold**: 30 minutes  
- **Max tracked breakers**: 1,000
- **Automatic cleanup**: Enabled

A circuit breaker is considered inactive if it hasn't been accessed (checked or updated) within the inactivity threshold. Inactive breakers are removed automatically.

### When to Customize Memory Management

**Customize cleanup for these scenarios:**

| Scenario | Issue | Solution |
|----------|-------|----------|
| Many short-lived nodes | Memory grows unbounded | Lower `InactivityThreshold` and `MaxTrackedCircuitBreakers` |
| High-frequency cleanup overhead | Cleanup CPU cost too high | Increase `CleanupInterval` |
| Rapid node creation/destruction | Eviction happens too often | Increase `MaxTrackedCircuitBreakers` |
| Long-running pipelines | Need stricter memory control | Enable aggressive cleanup with lower thresholds |

### Configuration Examples

#### Scenario 1: Short-Lived Nodes (e.g., Request-Based Pipelines)

For pipelines where nodes are created per request and disposed after:

```csharp
builder.ConfigureCircuitBreakerMemoryManagement(opts =>
    opts with
    {
        InactivityThreshold = TimeSpan.FromMinutes(5),    // Clean up faster
        MaxTrackedCircuitBreakers = 100,                  // Limit tracked breakers
        CleanupInterval = TimeSpan.FromSeconds(30)        // Check frequently
    }
);
```

When the limit is reached, the least-recently-used breaker is evicted.

#### Scenario 2: Large Pipelines with Many Nodes

For pipelines with hundreds of nodes that may or may not be accessed:

```csharp
builder.ConfigureCircuitBreakerMemoryManagement(opts =>
    opts with
    {
        MaxTrackedCircuitBreakers = 5000,                 // Allow more breakers
        InactivityThreshold = TimeSpan.FromHours(1),      // Longer inactivity window
        CleanupInterval = TimeSpan.FromMinutes(10)        // Less frequent cleanup
    }
);
```

#### Scenario 3: Disable Automatic Cleanup

For controlled scenarios where you manage cleanup manually:

```csharp
builder.ConfigureCircuitBreakerMemoryManagement(opts =>
    opts with
    {
        EnableAutomaticCleanup = false
    }
);

// Later, manually trigger cleanup when appropriate
// (Requires access to CircuitBreakerManager from pipeline context)
if (context.Items.TryGetValue(PipelineContextKeys.CircuitBreakerManager, out var manager) &&
    manager is ICircuitBreakerManager cbManager)
{
    int removedCount = cbManager.TriggerCleanup();
    logger.LogInformation("Cleaned up {Count} inactive circuit breakers", removedCount);
}
```

#### Scenario 4: Aggressive Memory Management

For memory-constrained environments:

```csharp
builder.ConfigureCircuitBreakerMemoryManagement(opts =>
    opts with
    {
        MaxTrackedCircuitBreakers = 50,                   // Very strict limit
        InactivityThreshold = TimeSpan.FromMinutes(2),    // Aggressive cleanup
        CleanupInterval = TimeSpan.FromSeconds(10),       // Frequent checks
        EnableAutomaticCleanup = true
    }
);
```

### Cleanup Behavior

When cleanup runs, NPipeline:

1. **Identifies inactive breakers**: Breakers not accessed within `InactivityThreshold`
2. **Removes inactive breakers**: Disposes stale circuit breaker instances
3. **Evicts if necessary**: If `MaxTrackedCircuitBreakers` is exceeded, removes least-recently-used (LRU) breaker
4. **Logs activity**: Records cleanup events at DEBUG/WARNING levels

### Monitoring Cleanup

Track circuit breaker lifecycle in your observability infrastructure:

```csharp
// Get current tracking count
if (context.Items.TryGetValue(PipelineContextKeys.CircuitBreakerManager, out var manager) &&
    manager is ICircuitBreakerManager cbManager)
{
    int trackedCount = cbManager.GetTrackedCircuitBreakerCount();
    metrics.Gauge("circuitbreaker.tracked_count", trackedCount);
}
```

### Performance Impact

Memory management is designed to be lightweight:

- **Cleanup operation**: O(n) where n = tracked breakers; typically < 10ms
- **Tracking overhead**: ~100 bytes per circuit breaker (timestamp + node ID)
- **LRU eviction**: O(n) scan to find least-recently-used

For most applications, defaults provide good balance. Customize only if:

- Monitoring shows excessive memory growth
- Cleanup CPU cost is significant (> 5% of pipeline execution time)
- You have specific memory constraints

### Memory Management Notes

- **Access updates**: Any access (check or state change) updates the breaker's last-accessed timestamp
- **Persistent state**: Circuit breaker state is **not** restored after removal and recreation—it starts as Closed
- **Thread-safe**: All cleanup operations are thread-safe; no synchronization needed

## Disabling Circuit Breaker

If needed, you can disable circuit breaking:

```csharp
var options = new PipelineCircuitBreakerOptions(
    FailureThreshold: 5,
    OpenDuration: TimeSpan.FromMinutes(1),
    SamplingWindow: TimeSpan.FromMinutes(5),
    Enabled: false  // Disabled - no circuit breaker checks
);
```

Or use the predefined disabled option:

```csharp
var disabledOptions = PipelineCircuitBreakerOptions.Disabled;
```

## Important Notes

### Distinction from Retry Options

Circuit breaker configuration is **separate** from retry configuration:

- **`WithCircuitBreaker()`**: Prevents repeated attempts when failure threshold is reached
- **`WithRetryOptions()`**: Controls how many retries are allowed per item or per node

Both can be used together for comprehensive resilience:

```csharp
var builder = new PipelineBuilder();

// Control retries at the item/node level
builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 3));

// Prevent cascade failures across the pipeline
builder.WithCircuitBreaker(failureThreshold: 5);
```

### Thread Safety

Circuit breakers are fully thread-safe and designed for concurrent pipeline execution. Multiple threads can safely check and update breaker state simultaneously.

### Performance Considerations

- Circuit breakers add minimal overhead (~50-100ns per check in Closed state)
- Operation tracking uses efficient rolling windows with automatic cleanup
- No significant memory overhead for typical usage patterns

## Troubleshooting

**Circuit breaker trips too frequently:**

- Increase `FailureThreshold` to tolerate more failures
- Use `RollingWindowRate` to tolerate occasional failures
- Extend `SamplingWindow` to smooth out bursty failures

**Circuit breaker never trips:**

- Reduce `FailureThreshold`
- Use `ConsecutiveFailures` for immediate detection
- Reduce `SamplingWindow` for faster detection

**Slow recovery:**

- Reduce `OpenDuration`
- Lower `HalfOpenSuccessThreshold` to recover faster

## See Also

- [Circuit Breaker Configuration](circuit-breaker-configuration.md) - Configuration reference
- [Resilience Patterns](./index.md) - General resilience patterns
- [Error Handling](../../architecture/error-handling-architecture.md) - Error handling strategies
