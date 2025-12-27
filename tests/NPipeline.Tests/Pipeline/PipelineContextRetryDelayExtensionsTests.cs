using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

public sealed class PipelineContextRetryDelayExtensionsTests
{
    [Fact]
    public void GetRetryDelayStrategy_WithNullContext_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => PipelineContextRetryDelayExtensions.GetRetryDelayStrategy(null!));
    }

    [Fact]
    public void GetRetryDelayStrategy_WithNoConfiguration_ShouldReturnNoOpStrategy()
    {
        // Arrange
        var context = new PipelineContext();

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<NoOpRetryDelayStrategy>();
        _ = strategy.Should().BeSameAs(NoOpRetryDelayStrategy.Instance);
    }

    [Fact]
    public void GetRetryDelayStrategy_WithCachedStrategy_ShouldReturnCachedInstance()
    {
        // Arrange
        var context = new PipelineContext();
        var strategy1 = context.GetRetryDelayStrategy();

        // Act
        var strategy2 = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy1.Should().BeSameAs(strategy2);
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithExponentialBackoffConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.5,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(2);
        _ = delay.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithLinearBackoffConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromMilliseconds(25),
                TimeSpan.FromSeconds(10)),
            JitterStrategies.EqualJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that Strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(3);
        _ = delay.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithFixedDelayConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(200)),
            JitterStrategies.NoJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that Strategy produces expected delay
        var delay = await strategy.GetDelayAsync(5);
        _ = delay.Should().Be(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithDecorrelatedJitterConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1)));

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that Strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(2);
        _ = delay.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithEqualJitterConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.EqualJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that Strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(1);
        _ = delay.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithNoJitterConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.NoJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that Strategy produces deterministic delays (no jitter)
        var delay1 = await strategy.GetDelayAsync(1);
        var delay2 = await strategy.GetDelayAsync(1);
        _ = delay1.Should().Be(delay2); // Should be deterministic
    }

    [Fact]
    public void GetRetryDelayStrategy_WithInvalidConfiguration_ShouldCreateCompositeStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            TestBackoffStrategy(), // Valid test implementation
            JitterStrategies.FullJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void GetRetryDelayStrategy_WithUnknownBackoffStrategy_ShouldCreateCompositeStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            TestBackoffStrategy(), // Valid test implementation
            JitterStrategies.FullJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithCustomJitterStrategy_ShouldWorkCorrectly()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            TestJitterStrategy());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Should use custom jitter strategy (which returns TimeSpan.Zero)
        var delay = await strategy.GetDelayAsync(1);
        _ = delay.Should().Be(TimeSpan.Zero); // TestJitterStrategy returns TimeSpan.Zero
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithCancellation_ShouldRespectCancellationToken()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromSeconds(1),
                2.0,
                TimeSpan.FromSeconds(10)),
            JitterStrategies.FullJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);
        var strategy = context.GetRetryDelayStrategy();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        // TaskCanceledException is thrown which is a subclass of OperationCanceledException
        _ = await Assert.ThrowsAsync<TaskCanceledException>(() => strategy.GetDelayAsync(1, cts.Token).AsTask());
    }

    [Fact]
    public void GetRetryDelayStrategy_WithMultipleCalls_ShouldCacheCorrectly()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy1 = context.GetRetryDelayStrategy();
        var strategy2 = context.GetRetryDelayStrategy();
        var strategy3 = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy1.Should().BeSameAs(strategy2);
        _ = strategy2.Should().BeSameAs(strategy3);
    }

    [Fact]
    public void GetRetryDelayStrategy_WithDifferentContexts_ShouldCreateSeparateStrategies()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter());

        var context1 = CreateContextWithDelayStrategy(delayConfig);
        var context2 = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy1 = context1.GetRetryDelayStrategy();
        var strategy2 = context2.GetRetryDelayStrategy();

        // Assert
        _ = strategy1.Should().NotBeSameAs(strategy2); // Different contexts should have different instances
        _ = strategy1.Should().BeOfType<CompositeRetryDelayStrategy>();
        _ = strategy2.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithAllStrategyCombinations_ShouldWorkCorrectly()
    {
        // Arrange
        var combinations = new[]
        {
            new
            {
                Backoff = BackoffStrategies.ExponentialBackoff(
                    TimeSpan.FromMilliseconds(100),
                    2.0,
                    TimeSpan.FromSeconds(30)),
                Jitter = JitterStrategies.FullJitter(),
                ExpectedType = typeof(CompositeRetryDelayStrategy),
            },
            new
            {
                Backoff = BackoffStrategies.LinearBackoff(
                    TimeSpan.FromMilliseconds(100),
                    TimeSpan.FromMilliseconds(100),
                    TimeSpan.FromSeconds(10)),
                Jitter = JitterStrategies.EqualJitter(),
                ExpectedType = typeof(CompositeRetryDelayStrategy),
            },
            new
            {
                Backoff = BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(500)),
                Jitter = JitterStrategies.NoJitter(),
                ExpectedType = typeof(CompositeRetryDelayStrategy),
            },
        };

        // Act & Assert
        foreach (var combination in combinations)
        {
            var delayConfig = new RetryDelayStrategyConfiguration(
                combination.Backoff,
                combination.Jitter);

            var context = CreateContextWithDelayStrategy(delayConfig);

            var strategy = context.GetRetryDelayStrategy();

            _ = strategy.Should().BeOfType(combination.ExpectedType);

            // Test that Strategy produces reasonable delays
            var delay = await strategy.GetDelayAsync(1);
            _ = delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        }
    }

    private static PipelineContext CreateContextWithDelayStrategy(RetryDelayStrategyConfiguration delayConfig)
    {
        var retryOptions = new PipelineRetryOptions(
            3,
            DelayStrategyConfiguration: delayConfig,
            MaxNodeRestartAttempts: 5,
            MaxSequentialNodeAttempts: 10);

        var config = PipelineContextConfiguration.WithRetry(retryOptions);
        return new PipelineContext(config);
    }

    // Test helper methods for unknown strategy types
    private static BackoffStrategy TestBackoffStrategy()
    {
        return attempt => TimeSpan.FromMilliseconds(100); // Test implementation
    }

    private static JitterStrategy TestJitterStrategy()
    {
        return (baseDelay, random) => TimeSpan.Zero; // Test implementation
    }

    // Test helper record for combinations
    private sealed record StrategyCombination(
        BackoffStrategy Backoff,
        JitterStrategy Jitter,
        Type ExpectedType);
}
