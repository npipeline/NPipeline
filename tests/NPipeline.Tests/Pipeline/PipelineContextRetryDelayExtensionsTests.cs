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
            new ExponentialBackoffConfiguration(
                TimeSpan.FromMilliseconds(100),
                2.5,
                TimeSpan.FromSeconds(30)),
            new FullJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(2);
        _ = delay.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithLinearBackoffConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new LinearBackoffConfiguration(
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromMilliseconds(25),
                TimeSpan.FromSeconds(10)),
            new EqualJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(3);
        _ = delay.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithFixedDelayConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new FixedDelayConfiguration(TimeSpan.FromMilliseconds(200)),
            new NoJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy produces the expected delay
        var delay = await strategy.GetDelayAsync(5);
        _ = delay.Should().Be(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithDecorrelatedJitterConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            new DecorrelatedJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(2);
        _ = delay.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithEqualJitterConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(),
            new EqualJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(1);
        _ = delay.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithNoJitterConfiguration_ShouldCreateCorrectStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(),
            new NoJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy produces deterministic delays (no jitter)
        var delay1 = await strategy.GetDelayAsync(1);
        var delay2 = await strategy.GetDelayAsync(1);
        _ = delay1.Should().Be(delay2); // Should be deterministic
    }

    [Fact]
    public void GetRetryDelayStrategy_WithInvalidConfiguration_ShouldFallbackToNoOpStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(
                TimeSpan.Zero, // Invalid
                2.0,
                TimeSpan.FromSeconds(30)),
            new FullJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<NoOpRetryDelayStrategy>();
        _ = strategy.Should().BeSameAs(NoOpRetryDelayStrategy.Instance);
    }

    [Fact]
    public void GetRetryDelayStrategy_WithUnknownBackoffStrategy_ShouldFallbackToNoOpStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new TestBackoffConfiguration(),
            new FullJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<NoOpRetryDelayStrategy>();
        _ = strategy.Should().BeSameAs(NoOpRetryDelayStrategy.Instance);
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithUnknownJitterStrategy_ShouldFallbackToNoOpStrategy()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(),
            new TestJitterConfiguration());

        var context = CreateContextWithDelayStrategy(delayConfig);

        // Act
        var strategy = context.GetRetryDelayStrategy();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<NoOpRetryDelayStrategy>();

        // Should fall back to no-op strategy when jitter type is unknown
        var delay = await strategy.GetDelayAsync(1);
        _ = delay.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public async Task GetRetryDelayStrategy_WithCancellation_ShouldRespectCancellationToken()
    {
        // Arrange
        var delayConfig = new RetryDelayStrategyConfiguration(
            new ExponentialBackoffConfiguration(
                TimeSpan.FromSeconds(1),
                2.0,
                TimeSpan.FromSeconds(10)),
            new FullJitterConfiguration());

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
            new ExponentialBackoffConfiguration(),
            new FullJitterConfiguration());

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
            new ExponentialBackoffConfiguration(),
            new FullJitterConfiguration());

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
                Backoff = (BackoffStrategyConfiguration)new ExponentialBackoffConfiguration(),
                Jitter = (JitterStrategyConfiguration)new FullJitterConfiguration(),
                ExpectedType = typeof(CompositeRetryDelayStrategy),
            },
            new
            {
                Backoff = (BackoffStrategyConfiguration)new LinearBackoffConfiguration(),
                Jitter = (JitterStrategyConfiguration)new EqualJitterConfiguration(),
                ExpectedType = typeof(CompositeRetryDelayStrategy),
            },
            new
            {
                Backoff = (BackoffStrategyConfiguration)new FixedDelayConfiguration(),
                Jitter = (JitterStrategyConfiguration)new NoJitterConfiguration(),
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

            // Test that the strategy produces reasonable delays
            var delay = await strategy.GetDelayAsync(1);
            _ = delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        }
    }

    private static PipelineContext CreateContextWithDelayStrategy(RetryDelayStrategyConfiguration delayConfig)
    {
        var retryOptions = new PipelineRetryOptions(3, 5, 10, null, delayConfig);
        var config = PipelineContextConfiguration.WithRetry(retryOptions);
        return new PipelineContext(config);
    }

    // Test helper records for unknown strategy types
    private sealed record TestBackoffConfiguration : BackoffStrategyConfiguration
    {
        public override string StrategyType => "Unknown";

        public override void Validate()
        {
            // No validation for test
        }
    }

    private sealed record TestJitterConfiguration : JitterStrategyConfiguration
    {
        public override string StrategyType => "Unknown";

        public override void Validate()
        {
            // No validation for test
        }
    }
}
