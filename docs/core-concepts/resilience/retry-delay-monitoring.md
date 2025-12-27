---
title: Monitoring Retry Metrics
description: Observe and measure retry behavior in production NPipeline systems
sidebar_position: 8.6
---

# Monitoring Retry Metrics

Production systems require visibility into retry behavior. Monitor metrics to understand failure patterns, tune retry strategies, and detect emerging issues.

## Key Metrics to Track

### Retry Rate
Percentage of operations that required at least one retry:

```csharp
public class RetryRateMetric
{
    public int TotalOperations { get; set; }
    public int OperationsWithRetry { get; set; }
    
    public double RetryRate => 
        (double)OperationsWithRetry / TotalOperations;
}
```

**Interpretation:**
- 0-5%: Healthy (occasional transient failures)
- 5-10%: Watch (increasing trend concerning)
- 10%+: Problem (systematic issues likely)

### Retry Counts
Distribution of retry attempts:

```csharp
public class RetryCountMetrics
{
    public Dictionary<int, int> AttemptDistribution { get; set; }
    
    public double AverageRetries { get; set; }
    public int MaxRetries { get; set; }
    public int SuccessOnFirstAttempt { get; set; }
}
```

**Useful for:**
- Tuning max retry limits
- Identifying systematic issues
- Capacity planning

### Delay Analysis
Time spent waiting between retries:

```csharp
public class DelayMetrics
{
    public TimeSpan TotalRetryTime { get; set; }
    public TimeSpan AverageDelay { get; set; }
    public TimeSpan MaxDelay { get; set; }
    public TimeSpan MinDelay { get; set; }
}
```

**Monitor for:**
- Delays longer than configured max
- Increasing delay times (hint of degradation)
- Consistent patterns vs. random variations

## Basic Metrics Collection

### Simple Retry Collector

```csharp
public class RetryMetricsCollector
{
    private readonly ConcurrentDictionary<string, RetryNodeMetrics> _nodeMetrics = new();
    
    public void RecordAttempt(string nodeId, int attemptNumber, TimeSpan delay)
    {
        var nodeMetrics = _nodeMetrics.GetOrAdd(
            nodeId,
            _ => new RetryNodeMetrics());

        nodeMetrics.TotalAttempts++;
        nodeMetrics.DelayHistory.Add(delay);
        
        if (attemptNumber == 0)
            nodeMetrics.FirstAttemptCount++;
    }

    public RetryNodeMetrics GetMetrics(string nodeId)
    {
        return _nodeMetrics.TryGetValue(nodeId, out var metrics)
            ? metrics
            : null;
    }

    public void PrintReport()
    {
        foreach (var (nodeId, metrics) in _nodeMetrics)
        {
            var retryRate = 1.0 - (metrics.FirstAttemptCount / (double)metrics.TotalAttempts);
            var avgDelay = metrics.DelayHistory.Count > 0
                ? TimeSpan.FromMilliseconds(
                    metrics.DelayHistory.Average(d => d.TotalMilliseconds))
                : TimeSpan.Zero;

            Console.WriteLine($"Node: {nodeId}");
            Console.WriteLine($"  Total attempts: {metrics.TotalAttempts}");
            Console.WriteLine($"  Retry rate: {retryRate:P}");
            Console.WriteLine($"  Average delay: {avgDelay.TotalMilliseconds:F2}ms");
        }
    }
}

public class RetryNodeMetrics
{
    public int TotalAttempts { get; set; }
    public int FirstAttemptCount { get; set; }
    public List<TimeSpan> DelayHistory { get; } = new();
}
```

## Structured Logging

Log retry events with full context for analysis:

```csharp
public class RetryEventLogger
{
    private readonly ILogger<RetryEventLogger> _logger;

    public async Task LogRetryAsync(
        string nodeId,
        int attempt,
        Exception error,
        TimeSpan delay,
        PipelineContext context)
    {
        _logger.LogWarning(
            new EventId(1001, "NodeRetry"),
            "Node {NodeId} retry attempt {Attempt}: " +
            "Error={ErrorType}, Message={ErrorMessage}, " +
            "NextDelay={NextDelayMs}ms",
            nodeId,
            attempt,
            error.GetType().Name,
            error.Message,
            delay.TotalMilliseconds);
    }

    public void LogRetryExhausted(string nodeId, int maxRetries, Exception finalError)
    {
        _logger.LogError(
            new EventId(1002, "RetriesExhausted"),
            "Node {NodeId} exhausted {MaxRetries} retries: {Error}",
            nodeId,
            maxRetries,
            finalError.Message);
    }
}
```

## Aggregated Metrics

Collect metrics by error type and time window:

```csharp
public class AggregatedRetryMetrics
{
    private readonly ConcurrentDictionary<string, ErrorTypeMetrics> _byErrorType = new();
    private readonly ConcurrentDictionary<string, TimeWindowMetrics> _byTimeWindow = new();

    public void RecordRetry(string errorType, int attemptNumber, TimeSpan delay)
    {
        // By error type
        var errorMetrics = _byErrorType.GetOrAdd(errorType, _ => new ErrorTypeMetrics());
        errorMetrics.RetryCount++;
        errorMetrics.Attempts.Add(attemptNumber);

        // By time window
        var window = GetTimeWindow();
        var windowMetrics = _byTimeWindow.GetOrAdd(window, _ => new TimeWindowMetrics());
        windowMetrics.RetryCount++;
    }

    public void PrintErrorTypeReport()
    {
        foreach (var (errorType, metrics) in _byErrorType.OrderByDescending(x => x.Value.RetryCount))
        {
            var avgAttempt = metrics.Attempts.Average();
            Console.WriteLine($"{errorType}: {metrics.RetryCount} retries, " +
                            $"avg attempt: {avgAttempt:F1}");
        }
    }

    private string GetTimeWindow()
    {
        var now = DateTime.UtcNow;
        return $"{now:yyyy-MM-dd HH:00}"; // Hourly buckets
    }

    private class ErrorTypeMetrics
    {
        public int RetryCount { get; set; }
        public List<int> Attempts { get; } = new();
    }

    private class TimeWindowMetrics
    {
        public int RetryCount { get; set; }
        public DateTime WindowStart { get; set; }
    }
}
```

## Strategy Performance Comparison

Compare actual retry behavior across strategies:

```csharp
public class StrategyPerformanceAnalyzer
{
    private readonly Dictionary<string, StrategyMetrics> _strategyMetrics = new();

    public void RecordStrategyUsage(
        string strategyName,
        int attemptNumber,
        TimeSpan actualDelay,
        TimeSpan configuredDelay)
    {
        var metrics = _strategyMetrics.GetOrAdd(strategyName, _ => new StrategyMetrics());
        
        metrics.TotalUsages++;
        metrics.AverageDelay = 
            (metrics.AverageDelay * (metrics.TotalUsages - 1) + actualDelay.TotalMilliseconds) 
            / metrics.TotalUsages;
        
        metrics.MaxDelay = Math.Max(metrics.MaxDelay, actualDelay.TotalMilliseconds);
        metrics.DeviationSum += Math.Abs(
            actualDelay.TotalMilliseconds - configuredDelay.TotalMilliseconds);
    }

    public void PrintComparison()
    {
        Console.WriteLine("Strategy Performance Comparison:");
        foreach (var (strategy, metrics) in _strategyMetrics)
        {
            var avgDeviation = metrics.DeviationSum / metrics.TotalUsages;
            Console.WriteLine($"  {strategy}:");
            Console.WriteLine($"    - Uses: {metrics.TotalUsages}");
            Console.WriteLine($"    - Avg delay: {metrics.AverageDelay:F2}ms");
            Console.WriteLine($"    - Max delay: {metrics.MaxDelay:F2}ms");
            Console.WriteLine($"    - Avg deviation: {avgDeviation:F2}ms");
        }
    }

    private class StrategyMetrics
    {
        public int TotalUsages { get; set; }
        public double AverageDelay { get; set; }
        public double MaxDelay { get; set; }
        public double DeviationSum { get; set; }
    }
}
```

## Production Monitoring Dashboard

Example metrics for a monitoring dashboard:

```csharp
public class RetryDashboardMetrics
{
    public class HealthIndicators
    {
        public double RetryRatePercentage { get; set; } // 0-100
        public int HealthScore { get; set; } // 0-100, 100 = healthy
        
        public string Status => HealthScore switch
        {
            >= 90 => "Healthy",
            >= 70 => "Warning",
            _ => "Critical"
        };
    }

    public class TimeSeriesData
    {
        public DateTime Timestamp { get; set; }
        public int RetryCount { get; set; }
        public double AverageDelayMs { get; set; }
        public int MaxConsecutiveRetries { get; set; }
    }

    public class NodeSummary
    {
        public string NodeName { get; set; }
        public int TotalOperations { get; set; }
        public int FailedOperations { get; set; }
        public double SuccessRate { get; set; }
        public int AverageRetriesPerFailure { get; set; }
    }
}

// Usage
public class DashboardPublisher
{
    public async Task PublishMetricsAsync(RetryDashboardMetrics metrics)
    {
        var health = new RetryDashboardMetrics.HealthIndicators
        {
            RetryRatePercentage = 3.5, // 3.5% of operations retried
            HealthScore = 92 // Healthy
        };

        // Publish to monitoring system (Prometheus, AppInsights, etc.)
        await PublishToMonitoringAsync("pipeline.retry.health", health);
    }

    private async Task PublishToMonitoringAsync(string metric, object value)
    {
        // Implementation depends on monitoring platform
        await Task.CompletedTask;
    }
}
```

## Alerts and Thresholds

Define alerting rules:

```csharp
public class RetryAlertingPolicy
{
    private readonly ILogger<RetryAlertingPolicy> _logger;

    public void EvaluateAndAlert(RetryMetricsSnapshot snapshot)
    {
        // Alert: High retry rate
        if (snapshot.RetryRatePercentage > 10)
        {
            _logger.LogError("Alert: High retry rate {RetryRate}% (threshold: 10%)",
                snapshot.RetryRatePercentage);
        }

        // Alert: Long delays
        if (snapshot.AverageDelayMs > 5000)
        {
            _logger.LogWarning("Alert: Average delay {DelayMs}ms exceeds threshold",
                snapshot.AverageDelayMs);
        }

        // Alert: Max retries consistently hit
        if (snapshot.ExhaustedRetriesPercentage > 5)
        {
            _logger.LogError("Alert: {Percent}% of retries exhausted",
                snapshot.ExhaustedRetriesPercentage);
        }

        // Alert: Increasing trend
        if (snapshot.RetryRateTrend > 0.2) // 20% increase
        {
            _logger.LogWarning("Alert: Retry rate increasing {TrendPercent}%",
                snapshot.RetryRateTrend * 100);
        }
    }

    public class RetryMetricsSnapshot
    {
        public double RetryRatePercentage { get; set; }
        public double AverageDelayMs { get; set; }
        public double ExhaustedRetriesPercentage { get; set; }
        public double RetryRateTrend { get; set; }
    }
}
```

## Best Practices

1. **Track retry rate by node**: Identify problematic nodes
2. **Monitor delay distributions**: Detect strategy mismatches
3. **Set up alerts**: Act on rising retry rates
4. **Compare strategies**: Measure real-world performance
5. **Log error types**: Understand failure patterns
6. **Use time windows**: Detect temporal patterns
7. **Dashboard visibility**: Make metrics accessible to team

## Related Topics

- [Retry Configuration](retries.md) - Configuration options
- [Retry Delays](retry-delays.md) - Strategy overview
- [Exponential Backoff](retry-delay-exponential.md) - Exponential strategy
- [Linear Backoff](retry-delay-linear.md) - Linear strategy
- [Fixed Delay](retry-delay-fixed.md) - Fixed delay strategy
- [Advanced Patterns](retry-delay-advanced.md) - Custom strategies
- [Testing Retries](retry-delay-testing.md) - Testing strategies
