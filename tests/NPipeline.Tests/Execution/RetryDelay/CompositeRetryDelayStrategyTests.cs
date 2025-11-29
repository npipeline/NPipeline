using AwesomeAssertions;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Execution.RetryDelay;

public sealed class CompositeRetryDelayStrategyTests
{
    [Fact]
    public void Constructor_WithValidParameters_ShouldCreateInstance()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var jitterStrategy = JitterStrategies.FullJitter();
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
        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new CompositeRetryDelayStrategy(null!, jitterStrategy, random));
    }

    [Fact]
    public void Constructor_WithNullRandom_ShouldThrowArgumentNullException()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var jitterStrategy = JitterStrategies.FullJitter();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, null!));
    }

    [Fact]
    public async Task GetDelayAsync_WithJitterStrategy_ShouldApplyJitter()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delay = await strategy.GetDelayAsync(0);

        // Assert
        delay.Should().BeGreaterThan(TimeSpan.Zero);
        delay.Should().BeLessThan(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task GetDelayAsync_WithoutJitterStrategy_ShouldReturnBackoffDelay()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, null, random);

        // Act
        var delay = await strategy.GetDelayAsync(0);

        // Assert
        delay.Should().Be(TimeSpan.FromSeconds(1));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public async Task GetDelayAsync_WithVariousAttemptNumbers_ShouldReturnValidDelay(int attemptNumber)
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromMilliseconds(100),
            2.0,
            TimeSpan.FromSeconds(10));

        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delay = await strategy.GetDelayAsync(attemptNumber);

        // Assert
        delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task GetDelayAsync_WithCancellation_ShouldRespectCancellationToken()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1));
        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<TaskCanceledException>(() => strategy.GetDelayAsync(0, cts.Token).AsTask());
    }

    [Fact]
    public async Task GetDelayAsync_WithExponentialBackoffAndJitter_ShouldCalculateCorrectly()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromMilliseconds(100),
            2.0,
            TimeSpan.FromSeconds(5));

        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42); // Fixed seed for predictable results
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 5; i++)
        {
            delays.Add(await strategy.GetDelayAsync(i));
        }

        // Assert
        // Base delays would be: 100ms, 200ms, 400ms, 800ms, 1600ms
        // With full jitter, each should be between 0 and the base delay
        delays[0].Should().BeLessThan(TimeSpan.FromMilliseconds(100));
        delays[1].Should().BeLessThan(TimeSpan.FromMilliseconds(200));
        delays[2].Should().BeLessThan(TimeSpan.FromMilliseconds(400));
        delays[3].Should().BeLessThan(TimeSpan.FromMilliseconds(800));
        delays[4].Should().BeLessThan(TimeSpan.FromMilliseconds(1600));
    }

    [Fact]
    public async Task GetDelayAsync_WithLinearBackoffAndJitter_ShouldCalculateCorrectly()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromSeconds(5));

        var jitterStrategy = JitterStrategies.EqualJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 5; i++)
        {
            delays.Add(await strategy.GetDelayAsync(i));
        }

        // Assert
        // Base delays would be: 100ms, 150ms, 200ms, 250ms, 300ms
        // With equal jitter, each should be between base/2 and base
        delays[0].Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(50));
        delays[0].Should().BeLessThan(TimeSpan.FromMilliseconds(100));
        delays[1].Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(75));
        delays[1].Should().BeLessThan(TimeSpan.FromMilliseconds(150));
        delays[2].Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(100));
        delays[2].Should().BeLessThan(TimeSpan.FromMilliseconds(200));
        delays[3].Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(125));
        delays[3].Should().BeLessThan(TimeSpan.FromMilliseconds(250));
        delays[4].Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(150));
        delays[4].Should().BeLessThan(TimeSpan.FromMilliseconds(300));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task GetDelayAsync_WithFixedDelayAndJitter_ShouldCalculateCorrectly(int attemptNumber)
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(1000));
        var jitterStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMilliseconds(2000));
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delay = await strategy.GetDelayAsync(attemptNumber);

        // Assert
        // Fixed delay should always be 1000ms, jitter should be between 0 and 2000ms
        delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        delay.Should().BeLessThan(TimeSpan.FromMilliseconds(2000));
    }

    [Fact]
    public async Task GetDelayAsync_WithMultipleStrategies_ShouldWorkConsistently()
    {
        // Arrange
        var strategyConfigs = new[]
        {
            new
            {
                Backoff = BackoffStrategies.ExponentialBackoff(TimeSpan.FromMilliseconds(100), 2.0, TimeSpan.FromSeconds(5)),
                Jitter = JitterStrategies.FullJitter(),
            },
            new
            {
                Backoff = BackoffStrategies.LinearBackoff(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(50), TimeSpan.FromSeconds(5)),
                Jitter = JitterStrategies.EqualJitter(),
            },
            new
            {
                Backoff = BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(1000)),
                Jitter = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMilliseconds(2000)),
            },
        };

        // Act & Assert
        foreach (var config in strategyConfigs)
        {
            var strategy = new CompositeRetryDelayStrategy(config.Backoff, config.Jitter, new Random(42));

            // All strategies should return valid delays
            for (var i = 0; i < 10; i++)
            {
                var delay = await strategy.GetDelayAsync(i);
                delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
                delay.Should().BeLessThan(TimeSpan.FromSeconds(10)); // Reasonable upper bound
            }
        }
    }

    [Fact]
    public async Task GetDelayAsync_WithMaxDelayCapping_ShouldRespectMaxDelay()
    {
        // Arrange
        var backoffStrategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromMilliseconds(1),
            10.0,
            TimeSpan.FromMilliseconds(100));

        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42);
        var strategy = new CompositeRetryDelayStrategy(backoffStrategy, jitterStrategy, random);

        // Act
        var delays = new List<TimeSpan>();

        for (var i = 0; i < 10; i++)
        {
            delays.Add(await strategy.GetDelayAsync(i));
        }

        // Assert
        // All delays should be capped at maxDelay (100ms) even with exponential growth
        foreach (var delay in delays)
        {
            delay.Should().BeLessThan(TimeSpan.FromMilliseconds(100));
        }
    }
}
