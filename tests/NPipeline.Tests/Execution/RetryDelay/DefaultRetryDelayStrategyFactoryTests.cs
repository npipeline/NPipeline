using AwesomeAssertions;
using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Execution.RetryDelay;

public sealed class DefaultRetryDelayStrategyFactoryTests
{
    private readonly DefaultRetryDelayStrategyFactory _factory;

    public DefaultRetryDelayStrategyFactoryTests()
    {
        _factory = new DefaultRetryDelayStrategyFactory();
    }

    [Fact]
    public async Task CreateStrategy_WithExponentialBackoff_ShouldCreateStrategy()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromSeconds(1),
                2.0,
                TimeSpan.FromMinutes(1)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = _factory.CreateStrategy(configuration);

        // Assert
        var compositeStrategy = strategy as CompositeRetryDelayStrategy;
        _ = compositeStrategy.Should().NotBeNull();

        // Test that the strategy works correctly
        var delay = await strategy.GetDelayAsync(0);
        _ = delay.Should().Be(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task CreateStrategy_WithLinearBackoff_ShouldCreateStrategy()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(500),
                TimeSpan.FromMinutes(2)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = _factory.CreateStrategy(configuration);

        // Assert
        var compositeStrategy = strategy as CompositeRetryDelayStrategy;
        _ = compositeStrategy.Should().NotBeNull();

        // Test that the strategy works correctly
        var delay = await strategy.GetDelayAsync(0);
        _ = delay.Should().Be(TimeSpan.FromSeconds(1));
    }

    [Fact]
    public void CreateStrategy_WithJitterStrategy_ShouldReturnCompositeStrategy()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter());

        // Act
        var strategy = _factory.CreateStrategy(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void CreateStrategy_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => _factory.CreateStrategy(null!));
    }

    [Fact]
    public async Task CreateStrategy_WithFixedDelay_ShouldReturnStrategy()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(200)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = _factory.CreateStrategy(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy works correctly
        var delay = await strategy.GetDelayAsync(0);
        _ = delay.Should().Be(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void CreateNoOp_ShouldReturnSingletonInstance()
    {
        // Act
        var strategy1 = _factory.CreateNoOp();
        var strategy2 = _factory.CreateNoOp();

        // Assert
        _ = strategy1.Should().NotBeNull();
        _ = strategy2.Should().NotBeNull();
        _ = strategy1.Should().BeSameAs(strategy2);
        _ = strategy1.Should().BeOfType<NoOpRetryDelayStrategy>();
    }

    [Fact]
    public void CreateFullJitter_ShouldReturnStrategy()
    {
        // Act
        var strategy = _factory.CreateFullJitter();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<JitterStrategy>();
    }

    [Fact]
    public void CreateDecorrelatedJitter_WithValidParameters_ShouldReturnStrategy()
    {
        // Arrange
        var maxDelay = TimeSpan.FromSeconds(30);

        // Act
        var strategy = _factory.CreateDecorrelatedJitter(maxDelay);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<JitterStrategy>();
    }

    [Fact]
    public void CreateDecorrelatedJitter_WithInvalidMaxDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var invalidMaxDelay = TimeSpan.Zero; // Invalid

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => _factory.CreateDecorrelatedJitter(invalidMaxDelay));
    }

    [Fact]
    public void CreateDecorrelatedJitter_WithInvalidMultiplier_ShouldThrowArgumentException()
    {
        // Arrange
        var maxDelay = TimeSpan.FromSeconds(30);
        const double invalidMultiplier = 0.5; // Invalid - less than 1.0

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => _factory.CreateDecorrelatedJitter(maxDelay, invalidMultiplier));
    }

    [Fact]
    public void CreateEqualJitter_ShouldReturnStrategy()
    {
        // Act
        var strategy = _factory.CreateEqualJitter();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<JitterStrategy>();
    }

    [Fact]
    public void CreateNoJitter_ShouldReturnStrategy()
    {
        // Act
        var strategy = _factory.CreateNoJitter();

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<JitterStrategy>();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task CreateStrategy_WithExponentialBackoffDifferentMultipliers_ShouldCalculateCorrectly(double multiplier)
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                multiplier,
                TimeSpan.FromSeconds(10)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = _factory.CreateStrategy(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();

        // Test a few delay calculations
        var delay1 = await strategy.GetDelayAsync(1);
        var delay2 = await strategy.GetDelayAsync(2);

        _ = delay1.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay2.Should().BeGreaterThanOrEqualTo(delay1); // Should be non-decreasing
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task CreateStrategy_WithLinearBackoffDifferentIncrements_ShouldCalculateCorrectly(int incrementSeconds)
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromSeconds(incrementSeconds),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.NoJitter());

        // Act
        var strategy = _factory.CreateStrategy(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();

        // Test a few delay calculations
        var delay1 = await strategy.GetDelayAsync(1);
        var delay2 = await strategy.GetDelayAsync(2);

        _ = delay1.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay2.Should().BeGreaterThanOrEqualTo(delay1); // Should be non-decreasing
    }

    [Fact]
    public async Task Factory_WithDefaultRandom_ShouldUseSharedRandomForJitter()
    {
        // Arrange
        var factory = new DefaultRetryDelayStrategyFactory();

        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.FullJitter());

        // Act
        var strategy1 = factory.CreateStrategy(configuration);
        var strategy2 = factory.CreateStrategy(configuration);

        // Assert
        _ = strategy1.Should().NotBeNull();
        _ = strategy2.Should().NotBeNull();

        // Test that strategies work
        var delay1 = await strategy1.GetDelayAsync(1);
        var delay2 = await strategy2.GetDelayAsync(1);

        _ = delay1.Should().BeGreaterThan(TimeSpan.Zero);
        _ = delay2.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public async Task CreateStrategies_WithConcurrentAccess_ShouldBeThreadSafe()
    {
        // Arrange
        var tasks = new Task[10];

        // Act
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                var configuration = new RetryDelayStrategyConfiguration(
                    BackoffStrategies.ExponentialBackoff(
                        TimeSpan.FromMilliseconds(100),
                        2.0,
                        TimeSpan.FromSeconds(30)),
                    JitterStrategies.FullJitter());

                var strategy = _factory.CreateStrategy(configuration);
                _ = strategy.Should().NotBeNull();

                var delay = await strategy.GetDelayAsync(1);
                _ = delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
            });
        }

        // Assert
        await Task.WhenAll(tasks);
    }

    [Fact]
    public void CreateAllStrategyTypes_ShouldReturnValidInstances()
    {
        // Arrange
        var exponentialConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(30)),
            JitterStrategies.NoJitter());

        var linearConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromMilliseconds(100),
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromSeconds(30)),
            JitterStrategies.NoJitter());

        var fixedConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromMilliseconds(200)),
            JitterStrategies.NoJitter());

        // Act
        var exponentialStrategy = _factory.CreateStrategy(exponentialConfig);
        var linearStrategy = _factory.CreateStrategy(linearConfig);
        var fixedStrategy = _factory.CreateStrategy(fixedConfig);
        var noOpStrategy = _factory.CreateNoOp();
        var fullJitterStrategy = _factory.CreateFullJitter();
        var decorrelatedJitterStrategy = _factory.CreateDecorrelatedJitter(TimeSpan.FromSeconds(30));
        var equalJitterStrategy = _factory.CreateEqualJitter();
        var noJitterStrategy = _factory.CreateNoJitter();

        // Assert
        _ = exponentialStrategy.Should().NotBeNull();
        _ = linearStrategy.Should().NotBeNull();
        _ = fixedStrategy.Should().NotBeNull();
        _ = noOpStrategy.Should().NotBeNull();
        _ = fullJitterStrategy.Should().NotBeNull();
        _ = decorrelatedJitterStrategy.Should().NotBeNull();
        _ = equalJitterStrategy.Should().NotBeNull();
        _ = noJitterStrategy.Should().NotBeNull();
    }

    [Fact]
    public async Task CreateStrategies_WithCombinedJitter_ShouldWorkCorrectly()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromMilliseconds(100),
                2.0,
                TimeSpan.FromSeconds(5)),
            JitterStrategies.FullJitter());

        // Act
        var strategy = _factory.CreateStrategy(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(2);
        _ = delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(5)); // Should not exceed max delay
    }
}
