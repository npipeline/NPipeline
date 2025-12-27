---
title: Advanced Retry Patterns
description: Implement advanced retry delay patterns including decorrelated jitter, custom strategies, and fallback patterns
sidebar_position: 8.4
---

# Advanced Retry Patterns

Beyond the basic strategies (exponential, linear, fixed), NPipeline supports advanced patterns for sophisticated scenarios.

## Decorrelated Jitter

Decorrelated jitter automatically adapts retry delays based on system conditions, providing better load distribution than full jitter in highly concurrent environments.

### When to Use

- High concurrency scenarios (thousands of concurrent operations)
- Load-adaptive systems
- Distributed systems with variable load

### Implementation

```csharp
var context = PipelineContext.Default;
context.UseExponentialBackoffDelay(
    baseDelay: TimeSpan.FromMilliseconds(100),
    multiplier: 2.0,
    maxDelay: TimeSpan.FromSeconds(30),
    jitterStrategy: JitterStrategies.DecorrelatedJitter());
```

### How It Works

1. Initial delay calculated from strategy (exponential/linear/fixed)
2. Jitter applied adaptively based on previous retry timing
3. Subsequent delays influenced by system responsiveness
4. Reduces synchronized retries more effectively than full jitter

### Example: Adaptive Load Distribution

```csharp
// With decorrelated jitter:
// - If system recovers quickly, next delay is shorter
// - If system is slow, next delay is longer
// - Naturally adapts to actual recovery times

var strategy = new DecorrelatedJitterStrategy(
    baseDelay: TimeSpan.FromSeconds(1),
    multiplier: 2.0);

for (int attempt = 0; attempt < 5; attempt++)
{
    var delay = await strategy.GetDelayAsync(attempt);
    // First attempt: ~1s
    // Second attempt: adapts to actual recovery
    // Continues adapting...
}
```

## Custom Backoff Strategies

Create completely custom backoff logic for domain-specific scenarios:

### Fibonacci Backoff

```csharp
public class FibonacciBackoffStrategy : IRetryDelayStrategy
{
    private readonly TimeSpan _maxDelay;

    public FibonacciBackoffStrategy(TimeSpan maxDelay)
    {
        _maxDelay = maxDelay;
    }

    public ValueTask<TimeSpan> GetDelayAsync(
        int attemptNumber,
        CancellationToken cancellationToken = default)
    {
        var fibValue = CalculateFibonacci(attemptNumber + 1);
        var delay = TimeSpan.FromSeconds(Math.Min(fibValue, _maxDelay.TotalSeconds));
        return new ValueTask<TimeSpan>(delay);
    }

    private double CalculateFibonacci(int n)
    {
        if (n <= 1) return 1;
        var prev = 1.0;
        var curr = 1.0;
        for (int i = 2; i < n; i++)
        {
            var next = prev + curr;
            prev = curr;
            curr = next;
        }
        return curr;
    }
}

// Usage:
var fibonacciStrategy = new FibonacciBackoffStrategy(TimeSpan.FromMinutes(1));
```

### Polynomial Backoff

```csharp
public class PolynomialBackoffStrategy : IRetryDelayStrategy
{
    private readonly double _exponent;
    private readonly TimeSpan _baseDelay;
    private readonly TimeSpan _maxDelay;

    public PolynomialBackoffStrategy(double exponent, TimeSpan baseDelay, TimeSpan maxDelay)
    {
        _exponent = exponent;
        _baseDelay = baseDelay;
        _maxDelay = maxDelay;
    }

    public ValueTask<TimeSpan> GetDelayAsync(
        int attemptNumber,
        CancellationToken cancellationToken = default)
    {
        var factor = Math.Pow(attemptNumber + 1, _exponent);
        var totalMs = _baseDelay.TotalMilliseconds * factor;
        var cappedMs = Math.Min(totalMs, _maxDelay.TotalMilliseconds);
        return new ValueTask<TimeSpan>(TimeSpan.FromMilliseconds(cappedMs));
    }
}

// Usage - quadratic backoff (^2):
var quadraticStrategy = new PolynomialBackoffStrategy(
    exponent: 2.0,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1));
```

### Time-of-Day Based Strategy

```csharp
public class TimeOfDayAwareBackoffStrategy : IRetryDelayStrategy
{
    private readonly IRetryDelayStrategy _peakHoursStrategy;
    private readonly IRetryDelayStrategy _offPeakStrategy;

    public TimeOfDayAwareBackoffStrategy(
        IRetryDelayStrategy peakHoursStrategy,
        IRetryDelayStrategy offPeakStrategy)
    {
        _peakHoursStrategy = peakHoursStrategy;
        _offPeakStrategy = offPeakStrategy;
    }

    public async ValueTask<TimeSpan> GetDelayAsync(
        int attemptNumber,
        CancellationToken cancellationToken = default)
    {
        var hour = DateTime.UtcNow.Hour;
        var isPeakHours = hour >= 8 && hour < 18; // 8 AM - 6 PM

        var strategy = isPeakHours ? _peakHoursStrategy : _offPeakStrategy;
        return await strategy.GetDelayAsync(attemptNumber, cancellationToken);
    }
}

// Usage:
var timeAwareStrategy = new TimeOfDayAwareBackoffStrategy(
    peakHoursStrategy: new ExponentialBackoffStrategy(
        TimeSpan.FromSeconds(2), 2.0, TimeSpan.FromMinutes(5)),
    offPeakStrategy: new ExponentialBackoffStrategy(
        TimeSpan.FromMilliseconds(500), 2.0, TimeSpan.FromMinutes(1)));
```

## Fallback Strategies

Implement cascading fallback when primary strategy exhausts retries:

```csharp
public class FallbackRetryStrategy : IRetryDelayStrategy
{
    private readonly IRetryDelayStrategy _primaryStrategy;
    private readonly IRetryDelayStrategy _fallbackStrategy;
    private readonly int _maxPrimaryAttempts;

    public FallbackRetryStrategy(
        IRetryDelayStrategy primary,
        IRetryDelayStrategy fallback,
        int maxPrimaryAttempts = 3)
    {
        _primaryStrategy = primary;
        _fallbackStrategy = fallback;
        _maxPrimaryAttempts = maxPrimaryAttempts;
    }

    public async ValueTask<TimeSpan> GetDelayAsync(
        int attemptNumber,
        CancellationToken cancellationToken = default)
    {
        var strategy = attemptNumber < _maxPrimaryAttempts
            ? _primaryStrategy
            : _fallbackStrategy;

        return await strategy.GetDelayAsync(attemptNumber, cancellationToken);
    }
}

// Usage - try fast exponential, then fall back to slow linear:
var fallbackStrategy = new FallbackRetryStrategy(
    primary: new ExponentialBackoffStrategy(
        TimeSpan.FromMilliseconds(100), 2.0, TimeSpan.FromSeconds(5)),
    fallback: new LinearBackoffStrategy(
        TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10)),
    maxPrimaryAttempts: 3);
```

## Circuit Breaker Pattern with Retries

Combine retry delays with circuit breaker to fail fast:

```csharp
public class CircuitBreakerRetryStrategy : IRetryDelayStrategy
{
    private readonly IRetryDelayStrategy _innerStrategy;
    private readonly CircuitBreaker _circuitBreaker;

    public CircuitBreakerRetryStrategy(
        IRetryDelayStrategy innerStrategy,
        CircuitBreaker circuitBreaker)
    {
        _innerStrategy = innerStrategy;
        _circuitBreaker = circuitBreaker;
    }

    public async ValueTask<TimeSpan> GetDelayAsync(
        int attemptNumber,
        CancellationToken cancellationToken = default)
    {
        // If circuit is open, fail immediately
        if (_circuitBreaker.IsOpen)
            throw new ServiceUnavailableException(
                "Circuit breaker is open - service unavailable");

        // Otherwise, use normal retry delay
        return await _innerStrategy.GetDelayAsync(attemptNumber, cancellationToken);
    }
}
```

## Load-Adaptive Strategy

Adjust retry delays based on system CPU/memory:

```csharp
public class LoadAdaptiveRetryStrategy : IRetryDelayStrategy
{
    private readonly IRetryDelayStrategy _lightLoadStrategy;
    private readonly IRetryDelayStrategy _heavyLoadStrategy;
    private readonly float _cpuThreshold;

    public LoadAdaptiveRetryStrategy(
        IRetryDelayStrategy lightLoadStrategy,
        IRetryDelayStrategy heavyLoadStrategy,
        float cpuThreshold = 0.7f)
    {
        _lightLoadStrategy = lightLoadStrategy;
        _heavyLoadStrategy = heavyLoadStrategy;
        _cpuThreshold = cpuThreshold;
    }

    public async ValueTask<TimeSpan> GetDelayAsync(
        int attemptNumber,
        CancellationToken cancellationToken = default)
    {
        var currentLoad = GetSystemLoad();
        var strategy = currentLoad > _cpuThreshold
            ? _heavyLoadStrategy
            : _lightLoadStrategy;

        return await strategy.GetDelayAsync(attemptNumber, cancellationToken);
    }

    private float GetSystemLoad()
    {
        // Get actual CPU/memory load from system
        // Implementation depends on platform and monitoring solution
        return 0.5f; // Placeholder
    }
}
```

## Dependency Injection with Custom Strategies

Register custom strategies in DI container:

```csharp
var services = new ServiceCollection();

// Register custom strategies
services.AddSingleton<IRetryDelayStrategy>(sp =>
    new FibonacciBackoffStrategy(TimeSpan.FromMinutes(1)));

services.AddSingleton<IRetryDelayStrategy>("polynomial", sp =>
    new PolynomialBackoffStrategy(2.0,
        TimeSpan.FromSeconds(1),
        TimeSpan.FromMinutes(1)));

services.AddSingleton<IRetryDelayStrategy>("timeaware", sp =>
    new TimeOfDayAwareBackoffStrategy(
        new ExponentialBackoffStrategy(TimeSpan.FromSeconds(2), 2.0, TimeSpan.FromMinutes(5)),
        new ExponentialBackoffStrategy(TimeSpan.FromMilliseconds(500), 2.0, TimeSpan.FromMinutes(1))));

var provider = services.BuildServiceProvider();

// Use in pipeline
var context = PipelineContext.Default;
var strategy = provider.GetRequiredService<IRetryDelayStrategy>();
```

## Best Practices for Custom Strategies

1. **Always implement ValueTask**: Use ValueTask for efficient async
2. **Respect CancellationToken**: Honor cancellation requests
3. **Cache expensive calculations**: Don't recalculate on each call
4. **Document behavior**: Clearly describe when to use each strategy
5. **Test thoroughly**: Include edge cases and boundary conditions
6. **Monitor performance**: Track actual delays in production
7. **Set reasonable caps**: Prevent indefinitely long delays

## Related Topics

- [Retry Configuration](retries.md) - Overall retry configuration
- [Retry Delays](retry-delays.md) - Overview of strategies
- [Exponential Backoff](retry-delay-exponential.md) - Exponential delays
- [Linear Backoff](retry-delay-linear.md) - Linear delays
- [Fixed Delay](retry-delay-fixed.md) - Fixed delays
- [Testing Retries](retry-delay-testing.md) - Testing strategies
- [Monitoring Retries](retry-delay-monitoring.md) - Observing metrics
