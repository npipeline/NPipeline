using AwesomeAssertions;
using NPipeline.Execution.RetryDelay;
using NPipeline.Execution.RetryDelay.Backoff;
using NPipeline.Execution.RetryDelay.Jitter;

namespace NPipeline.Tests.Execution.RetryDelay;

public sealed class CompositeRetryDelayStrategyTests
{
    [Fact]
    public void Constructor_WithValidParameters_ShouldCreateInstance()
    {
        // Arrange
        var backoffStrategy = new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration());
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);

        // Act
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Assert
        strategy.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullBackoffStrategy_ShouldThrowArgumentNullException()
    {
        // Arrange
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new CompositeRetryDelayStrategy(null!, jitterStrategy, random));
    }

    [Fact]
    public void Constructor_WithNullRandom_ShouldThrowArgumentNullException()
    {
        // Arrange
        var backoffStrategy = new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration());
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, null!));
    }

    [Fact]
    public void Constructor_WithNullJitterStrategy_ShouldCreateInstance()
    {
        // Arrange
        var backoffStrategy = new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration());
        var random = new Random(42);

        // Act
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, null, random);

        // Assert
        strategy.Should().NotBeNull();
    }

    [Fact]
    public async Task GetDelayAsync_WithCancellationRequested_ShouldReturnCancelledTask()
    {
        // Arrange
        var backoffStrategy = new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration());
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);
        var cancellationTokenSource = new CancellationTokenSource();
        var cancellationToken = cancellationTokenSource.Token;

        // Act
        cancellationTokenSource.Cancel();
        var result = strategy.GetDelayAsync(0, cancellationToken);

        // Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await result);
    }

    [Fact]
    public async Task GetDelayAsync_WithNegativeAttemptNumber_ShouldReturnZero()
    {
        // Arrange
        var backoffStrategy = new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration());
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var result = await strategy.GetDelayAsync(-1);

        // Assert
        result.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetDelayAsync_WithBothStrategies_ShouldApplyBoth()
    {
        // Arrange
        var backoffStrategy = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(1000) });
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var result = await strategy.GetDelayAsync(0);

        // Assert
        // Base delay should be 1000ms
        // Jitter should be between 0 and 1000ms
        result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        result.Should().BeLessThan(TimeSpan.FromMilliseconds(1000));
    }

    [Fact]
    public async Task GetDelayAsync_WithoutJitterStrategy_ShouldReturnBackoffDelay()
    {
        // Arrange
        var backoffStrategy = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(1000) });
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, null, random);

        // Act
        var result = await strategy.GetDelayAsync(0);

        // Assert
        result.Should().Be(TimeSpan.FromMilliseconds(1000));
    }

    [Fact]
    public async Task GetDelayAsync_WithExponentialBackoffAndFullJitter_ShouldApplyCorrectly()
    {
        // Arrange
        var backoffStrategy = new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromMilliseconds(100),
            Multiplier = 2.0,
        });

        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var result0 = await strategy.GetDelayAsync(0); // 100ms base
        var result1 = await strategy.GetDelayAsync(1); // 200ms base
        var result2 = await strategy.GetDelayAsync(2); // 400ms base

        // Assert
        result0.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        result0.Should().BeLessThan(TimeSpan.FromMilliseconds(100));

        result1.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        result1.Should().BeLessThan(TimeSpan.FromMilliseconds(200));

        result2.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        result2.Should().BeLessThan(TimeSpan.FromMilliseconds(400));
    }

    [Fact]
    public async Task GetDelayAsync_WithLinearBackoffAndEqualJitter_ShouldApplyCorrectly()
    {
        // Arrange
        var backoffStrategy = new LinearBackoffStrategy(new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromMilliseconds(100),
            Increment = TimeSpan.FromMilliseconds(50),
        });

        var jitterStrategy = new EqualJitterStrategy(new EqualJitterConfiguration());
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var result0 = await strategy.GetDelayAsync(0); // 100ms base
        var result1 = await strategy.GetDelayAsync(1); // 150ms base
        var result2 = await strategy.GetDelayAsync(2); // 200ms base

        // Assert
        // Each result should be between baseDelay/2 and baseDelay
        result0.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(50));
        result0.Should().BeLessThan(TimeSpan.FromMilliseconds(100));

        result1.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(75));
        result1.Should().BeLessThan(TimeSpan.FromMilliseconds(150));

        result2.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(100));
        result2.Should().BeLessThan(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public async Task GetDelayAsync_WithDecorrelatedJitter_ShouldTrackPreviousDelay()
    {
        // Arrange
        var backoffStrategy = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(1000) });

        var jitterStrategy = new DecorrelatedJitterStrategy(new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromMilliseconds(2000),
            Multiplier = 2.0,
        });

        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var result1 = await strategy.GetDelayAsync(0); // First call
        var result2 = await strategy.GetDelayAsync(0); // Second call with same base

        // Assert
        // First call should be between baseDelay and baseDelay (1000-1000ms)
        result1.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(1000));
        result1.Should().BeLessThan(TimeSpan.FromMilliseconds(1000));

        // Second call should be between baseDelay and min(baseDelay, result1 * 2.0)
        result2.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(1000));
        result2.Should().BeLessThan(TimeSpan.FromMilliseconds(2000));
    }

    [Fact]
    public async Task GetDelayAsync_WithNoJitterStrategy_ShouldIgnoreRandomParameter()
    {
        // Arrange
        var backoffStrategy = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(1000) });
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, null, random);

        // Act
        var result1 = await strategy.GetDelayAsync(0);
        var result2 = await strategy.GetDelayAsync(0);

        // Assert
        result1.Should().Be(TimeSpan.FromMilliseconds(1000));
        result2.Should().Be(TimeSpan.FromMilliseconds(1000));
    }

    [Fact]
    public Task GetDelayAsync_WithConcurrentCalls_ShouldBeThreadSafe()
    {
        // Arrange
        var backoffStrategy = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(1000) });
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);
        var results = new List<TimeSpan>();
        var lockObject = new object();

        // Act
        Parallel.For(0, 10, async i =>
        {
            var localResults = new List<TimeSpan>();

            for (var j = 0; j < 10; j++)
            {
                var valueTask = strategy.GetDelayAsync(0);

                if (valueTask.IsCompleted)
                {
                    var result = valueTask.GetAwaiter().GetResult();
                    localResults.Add(result);
                }
                else
                {
                    var result = await valueTask;
                    localResults.Add(result);
                }
            }

            lock (lockObject)
            {
                results.AddRange(localResults);
            }
        });

        // Assert
        results.Should().HaveCount(100);

        // All results should be in valid range
        foreach (var result in results)
        {
            result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
            result.Should().BeLessThan(TimeSpan.FromMilliseconds(1000));
        }

        return Task.CompletedTask;
    }

    [Fact]
    public async Task GetDelayAsync_WithDifferentStrategyCombinations_ShouldWorkCorrectly()
    {
        // Arrange
        var strategies = new object[]
        {
            new
            {
                Backoff = new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration()),
                Jitter = new FullJitterStrategy(new FullJitterConfiguration()),
                Name = "Exponential + Full",
            },
            new
            {
                Backoff = new LinearBackoffStrategy(new LinearBackoffConfiguration()),
                Jitter = new EqualJitterStrategy(new EqualJitterConfiguration()),
                Name = "Linear + Equal",
            },
            new
            {
                Backoff = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(1000) }),
                Jitter = new DecorrelatedJitterStrategy(new DecorrelatedJitterConfiguration()),
                Name = "Fixed + Decorrelated",
            },
        };

        var random = new Random(42);

        // Act & Assert
        foreach (dynamic strategyConfig in strategies)
        {
            var strategy = new CompositeRetryDelayStrategy(strategyConfig.Backoff, strategyConfig.Jitter, random);
            var result = await strategy.GetDelayAsync(0);

            // All should return valid delays
            result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        }
    }

    [Fact]
    public async Task GetDelayAsync_WithZeroAttemptNumber_ShouldApplyStrategies()
    {
        // Arrange
        var backoffStrategy = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(500) });
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var result = await strategy.GetDelayAsync(0);

        // Assert
        result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        result.Should().BeLessThan(TimeSpan.FromMilliseconds(500));
    }

    [Fact]
    public async Task GetDelayAsync_WithLargeAttemptNumber_ShouldHandleCorrectly()
    {
        // Arrange
        var backoffStrategy = new ExponentialBackoffStrategy(new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromMilliseconds(100),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromMilliseconds(10000),
        });

        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var result = await strategy.GetDelayAsync(10); // Large attempt number

        // Assert
        // Should be capped at max delay (10000ms) before jitter
        result.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        result.Should().BeLessThan(TimeSpan.FromMilliseconds(10000));
    }

    [Fact]
    public async Task GetDelayAsync_WithFixedSeed_ShouldProduceConsistentResults()
    {
        // Arrange
        var backoffStrategy = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(1000) });
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random1 = new Random(42);
        var random2 = new Random(42);
        var strategy1 = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random1);
        var strategy2 = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random2);

        // Act
        var result1 = await strategy1.GetDelayAsync(0);
        var result2 = await strategy2.GetDelayAsync(0);

        // Assert
        result1.Should().Be(result2);
    }

    [Fact]
    public async Task GetDelayAsync_WithDifferentRandomInstances_ShouldProduceDifferentResults()
    {
        // Arrange
        var backoffStrategy = new FixedDelayStrategy(new FixedDelayConfiguration { Delay = TimeSpan.FromMilliseconds(1000) });
        var jitterStrategy = new FullJitterStrategy(new FullJitterConfiguration());
        var random1 = new Random(42);
        var random2 = new Random(24); // Different seed
        var strategy1 = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random1);
        var strategy2 = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random2);

        // Act
        var result1 = await strategy1.GetDelayAsync(0);
        var result2 = await strategy2.GetDelayAsync(0);

        // Assert
        result1.Should().NotBe(result2);
    }

    private sealed record StrategyTest(string Name, IRetryDelayStrategy Strategy, TimeSpan[] ExpectedDelays);
}
