---
title: Testing Retry Behavior
description: Test retry delay strategies with predictable, deterministic behavior in NPipeline
sidebar_position: 8.5
---

# Testing Retry Behavior

Testing retry strategies requires predictable timing. Use fixed delays and controlled randomness to ensure your tests are deterministic.

## Testing Principles

**Key Rule:** Always use fixed delays (no jitter) in tests to ensure predictable timing.

```csharp
// Good - predictable for testing
context.UseFixedDelay(TimeSpan.FromMilliseconds(10));

// Bad - jitter makes timing unpredictable
context.UseFixedDelay(TimeSpan.FromSeconds(1), JitterStrategies.FullJitter());
```

## Basic Retry Testing

Test that retries actually occur with expected delays:

```csharp
[Fact]
public async Task Retry_Occurs_WithExpectedDelay()
{
    // Arrange
    var context = CreateTestContext();
    context.UseFixedDelay(TimeSpan.FromMilliseconds(10));
    
    var attemptCount = 0;
    var failingNode = new TestNode(async () =>
    {
        attemptCount++;
        if (attemptCount < 3)
            throw new TemporaryException("Try again");
        return Unit.Default;
    });

    // Act
    var stopwatch = Stopwatch.StartNew();
    await ExecuteWithRetry(context, failingNode, maxRetries: 3);
    stopwatch.Stop();

    // Assert
    Assert.Equal(3, attemptCount);
    // 2 retries × 10ms = 20ms minimum
    Assert.True(stopwatch.ElapsedMilliseconds >= 20);
}
```

## Testing Exponential Backoff

Verify exponential backoff produces correct delays:

```csharp
[Fact]
public async Task ExponentialBackoff_ProducesExpectedSequence()
{
    // Arrange
    var context = CreateTestContext();
    context.UseExponentialBackoffDelay(
        baseDelay: TimeSpan.FromMilliseconds(10),
        multiplier: 2.0,
        maxDelay: TimeSpan.FromMilliseconds(100));

    var delays = new List<TimeSpan>();
    var node = new InstrumentedNode(async (attempt) =>
    {
        if (attempt < 5)
        {
            delays.Add(context.GetRetryDelayStrategy().GetDelayAsync(attempt).Result);
            throw new TemporaryException();
        }
    });

    // Act
    await ExecuteWithRetry(context, node, maxRetries: 5);

    // Assert
    var expected = new[]
    {
        TimeSpan.FromMilliseconds(10),   // 10 × 2^0
        TimeSpan.FromMilliseconds(20),   // 10 × 2^1
        TimeSpan.FromMilliseconds(40),   // 10 × 2^2
        TimeSpan.FromMilliseconds(80),   // 10 × 2^3
        TimeSpan.FromMilliseconds(100),  // 10 × 2^4, capped at 100
    };

    Assert.Equal(expected, delays);
}
```

## Testing Linear Backoff

```csharp
[Fact]
public async Task LinearBackoff_ProducesExpectedSequence()
{
    // Arrange
    var context = CreateTestContext();
    context.UseLinearBackoffDelay(
        baseDelay: TimeSpan.FromMilliseconds(10),
        increment: TimeSpan.FromMilliseconds(5),
        maxDelay: TimeSpan.FromMilliseconds(50));

    var delays = new List<TimeSpan>();
    
    // Act - collect delays for first 6 attempts
    var strategy = context.GetRetryDelayStrategy();
    for (int i = 0; i < 6; i++)
    {
        delays.Add(await strategy.GetDelayAsync(i));
    }

    // Assert
    var expected = new[]
    {
        TimeSpan.FromMilliseconds(10),   // 10 + (0 × 5)
        TimeSpan.FromMilliseconds(15),   // 10 + (1 × 5)
        TimeSpan.FromMilliseconds(20),   // 10 + (2 × 5)
        TimeSpan.FromMilliseconds(25),   // 10 + (3 × 5)
        TimeSpan.FromMilliseconds(30),   // 10 + (4 × 5)
        TimeSpan.FromMilliseconds(35),   // 10 + (5 × 5), capped at 50
    };

    Assert.Equal(expected, delays);
}
```

## Testing Jitter Distribution

When testing jitter, use seeded Random for reproducibility:

```csharp
[Fact]
public async Task FullJitter_ProducesVariableDelays()
{
    // Arrange
    var context = CreateTestContext();
    var random = new Random(42); // Fixed seed
    context.UseFixedDelay(
        TimeSpan.FromMilliseconds(100),
        jitterStrategy: new SeededJitterStrategy(random));

    var delays = new List<TimeSpan>();

    // Act - collect multiple delays
    var strategy = context.GetRetryDelayStrategy();
    for (int i = 0; i < 10; i++)
    {
        delays.Add(await strategy.GetDelayAsync(i));
    }

    // Assert
    // With full jitter on 100ms:
    // - All delays should be between 0-100ms
    Assert.All(delays, d =>
    {
        Assert.True(d >= TimeSpan.Zero);
        Assert.True(d <= TimeSpan.FromMilliseconds(100));
    });

    // Delays should vary (not all identical)
    var distinctDelays = delays.Distinct().Count();
    Assert.True(distinctDelays > 1);
}

public class SeededJitterStrategy : IJitterStrategy
{
    private readonly Random _random;

    public SeededJitterStrategy(Random random)
    {
        _random = random;
    }

    public TimeSpan Apply(TimeSpan delay)
    {
        return TimeSpan.FromMilliseconds(
            _random.NextDouble() * delay.TotalMilliseconds);
    }
}
```

## Testing Retry Exhaustion

Verify behavior when max retries are exceeded:

```csharp
[Fact]
public async Task MaxRetries_ThrowsWhenExhausted()
{
    // Arrange
    var context = CreateTestContext();
    context.UseFixedDelay(TimeSpan.FromMilliseconds(1));

    var alwaysFailingNode = new TestNode(async () =>
    {
        throw new Exception("Always fails");
    });

    // Act & Assert
    await Assert.ThrowsAsync<MaxRetriesExceededException>(async () =>
    {
        await ExecuteWithRetry(context, alwaysFailingNode, maxRetries: 3);
    });
}
```

## Testing Mixed Scenarios

Test different retry strategies across multiple nodes:

```csharp
[Fact]
public async Task MultipleNodes_WithDifferentStrategies()
{
    // Arrange
    var context = CreateTestContext();

    // Different strategies per node
    var exponentialContext = CreateContextWithExponentialBackoff();
    var linearContext = CreateContextWithLinearBackoff();

    var node1 = BuildNode(exponentialContext, "exponential-node");
    var node2 = BuildNode(linearContext, "linear-node");

    // Act
    var result1 = await ExecuteNode(context, node1);
    var result2 = await ExecuteNode(context, node2);

    // Assert
    Assert.NotNull(result1);
    Assert.NotNull(result2);
}
```

## Testing Timeout with Retries

Combine timeout with retry testing:

```csharp
[Fact]
public async Task Timeout_CombinedWithRetry()
{
    // Arrange
    var context = CreateTestContext();
    context.UseFixedDelay(TimeSpan.FromMilliseconds(50));

    var slowNode = new TestNode(async () =>
    {
        await Task.Delay(100); // Slower than timeout
        return Unit.Default;
    });

    // Act & Assert - operation times out before completing
    using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
    
    await Assert.ThrowsAsync<OperationCanceledException>(async () =>
    {
        await ExecuteWithRetry(context, slowNode, maxRetries: 3, cts.Token);
    });
}
```

## Testing Custom Strategies

Test your custom retry delay implementations:

```csharp
[Fact]
public async Task CustomFibonacciStrategy_ProducesExpectedSequence()
{
    // Arrange
    var strategy = new FibonacciBackoffStrategy(TimeSpan.FromSeconds(10));
    var delays = new List<double>();

    // Act
    for (int i = 0; i < 6; i++)
    {
        var delay = await strategy.GetDelayAsync(i);
        delays.Add(delay.TotalSeconds);
    }

    // Assert - Fibonacci sequence: 1, 1, 2, 3, 5, 8
    var expected = new[] { 1, 1, 2, 3, 5, 8 };
    for (int i = 0; i < expected.Length; i++)
    {
        Assert.Equal(expected[i], delays[i], 0.1); // Allow small rounding
    }
}
```

## Testing Framework Helper

Create reusable test utilities:

```csharp
public static class RetryTestHelpers
{
    /// <summary>
    /// Execute action with retries and collect timing information.
    /// </summary>
    public static async Task<RetryTestResult> ExecuteWithRetryTimingAsync(
        Func<int, Task> action,
        IRetryDelayStrategy strategy,
        int maxRetries = 3)
    {
        var attempts = 0;
        var delays = new List<TimeSpan>();
        var stopwatch = Stopwatch.StartNew();

        for (int attempt = 0; attempt <= maxRetries; attempt++)
        {
            attempts++;
            try
            {
                await action(attempt);
                break; // Success
            }
            catch when (attempt < maxRetries)
            {
                var delay = await strategy.GetDelayAsync(attempt);
                delays.Add(delay);
                await Task.Delay(delay);
            }
        }

        stopwatch.Stop();

        return new RetryTestResult
        {
            AttemptCount = attempts,
            Delays = delays,
            TotalElapsed = stopwatch.Elapsed
        };
    }

    public class RetryTestResult
    {
        public int AttemptCount { get; set; }
        public List<TimeSpan> Delays { get; set; }
        public TimeSpan TotalElapsed { get; set; }
    }
}

// Usage:
[Fact]
public async Task Using_RetryTestHelper()
{
    var strategy = new ExponentialBackoffStrategy(
        TimeSpan.FromMilliseconds(10), 2.0, TimeSpan.FromSeconds(1));
    
    var result = await RetryTestHelpers.ExecuteWithRetryTimingAsync(
        action: async (attempt) =>
        {
            if (attempt < 2)
                throw new Exception("Retry");
        },
        strategy: strategy,
        maxRetries: 3);

    Assert.Equal(3, result.AttemptCount);
    Assert.Equal(2, result.Delays.Count);
}
```

## Best Practices

1. **Always use fixed delays in tests**: Ensures deterministic timing
2. **Test with seeded random**: Use fixed seeds for reproducibility
3. **Verify actual timing**: Use Stopwatch to validate delays
4. **Test edge cases**: Max retries, immediate success, timeout
5. **Test combinations**: Different strategies + error types
6. **Use test helpers**: Create utilities for reusable patterns
7. **Document assumptions**: Explain expected timing behavior

## Related Topics

- [Retry Configuration](retries.md) - Configuration options
- [Retry Delays](retry-delays.md) - Strategy overview
- [Exponential Backoff](retry-delay-exponential.md) - Exponential strategy
- [Linear Backoff](retry-delay-linear.md) - Linear strategy
- [Fixed Delay](retry-delay-fixed.md) - Fixed delay strategy
- [Advanced Patterns](retry-delay-advanced.md) - Custom strategies
- [Monitoring Retries](retry-delay-monitoring.md) - Production monitoring
