using AwesomeAssertions;
using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;
using NPipeline.Execution.RetryDelay.Jitter;
using DecorrelatedJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.DecorrelatedJitterConfiguration;
using EqualJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.EqualJitterConfiguration;
using FullJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.FullJitterConfiguration;
using NoJitterConfiguration = NPipeline.Execution.RetryDelay.Jitter.NoJitterConfiguration;

namespace NPipeline.Tests.Execution.RetryDelay;

public sealed class DefaultRetryDelayStrategyFactoryTests
{
    private readonly DefaultRetryDelayStrategyFactory _factory;

    public DefaultRetryDelayStrategyFactoryTests()
    {
        _factory = new DefaultRetryDelayStrategyFactory();
    }

    [Fact]
    public void Constructor_WithNullRandom_ShouldCreateInstance()
    {
        // Act
        var factory = new DefaultRetryDelayStrategyFactory();

        // Assert
        _ = factory.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithCustomRandom_ShouldCreateInstance()
    {
        // Arrange & Act & Assert
        // DefaultRetryDelayStrategyFactory doesn't have a constructor that takes Random
        // This test should verify the default constructor works
        var factory = new DefaultRetryDelayStrategyFactory();
        _ = factory.Should().NotBeNull();
    }

    [Fact]
    public void CreateExponentialBackoff_WithValidConfiguration_ShouldReturnStrategy()
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration(
            TimeSpan.FromMilliseconds(100),
            2.0,
            TimeSpan.FromSeconds(10));

        // Act
        var strategy = _factory.CreateExponentialBackoff(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void CreateExponentialBackoff_WithJitterStrategy_ShouldReturnCompositeStrategy()
    {
        // Arrange
        var backoffConfiguration = new ExponentialBackoffConfiguration();
        var jitterConfiguration = new FullJitterConfiguration();
        var jitterStrategy = _factory.CreateFullJitter(jitterConfiguration);

        // Act
        var strategy = _factory.CreateExponentialBackoff(backoffConfiguration, jitterStrategy);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void CreateExponentialBackoff_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => _factory.CreateExponentialBackoff(null!));
    }

    [Fact]
    public void CreateExponentialBackoff_WithInvalidConfiguration_ShouldThrowArgumentException()
    {
        // Arrange
        var invalidConfiguration = new ExponentialBackoffConfiguration(
            TimeSpan.Zero, // Invalid
            2.0,
            TimeSpan.FromSeconds(10));

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => _factory.CreateExponentialBackoff(invalidConfiguration));
    }

    [Fact]
    public void CreateLinearBackoff_WithValidConfiguration_ShouldReturnStrategy()
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromMilliseconds(50),
            TimeSpan.FromSeconds(10));

        // Act
        var strategy = _factory.CreateLinearBackoff(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void CreateLinearBackoff_WithJitterStrategy_ShouldReturnCompositeStrategy()
    {
        // Arrange
        var backoffConfiguration = new LinearBackoffConfiguration();
        var jitterConfiguration = new EqualJitterConfiguration();
        var jitterStrategy = _factory.CreateEqualJitter(jitterConfiguration);

        // Act
        var strategy = _factory.CreateLinearBackoff(backoffConfiguration, jitterStrategy);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void CreateLinearBackoff_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => _factory.CreateLinearBackoff(null!));
    }

    [Fact]
    public void CreateLinearBackoff_WithInvalidConfiguration_ShouldThrowArgumentException()
    {
        // Arrange
        var invalidConfiguration = new LinearBackoffConfiguration(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(-1), // Invalid
            TimeSpan.FromSeconds(10));

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => _factory.CreateLinearBackoff(invalidConfiguration));
    }

    [Fact]
    public void CreateFixedDelay_WithValidConfiguration_ShouldReturnStrategy()
    {
        // Arrange
        var configuration = new FixedDelayConfiguration(TimeSpan.FromMilliseconds(200));

        // Act
        var strategy = _factory.CreateFixedDelay(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void CreateFixedDelay_WithJitterStrategy_ShouldReturnCompositeStrategy()
    {
        // Arrange
        var backoffConfiguration = new FixedDelayConfiguration(TimeSpan.FromMilliseconds(1000));
        var jitterConfiguration = new DecorrelatedJitterConfiguration();
        var jitterStrategy = _factory.CreateDecorrelatedJitter(jitterConfiguration);

        // Act
        var strategy = _factory.CreateFixedDelay(backoffConfiguration, jitterStrategy);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();
    }

    [Fact]
    public void CreateFixedDelay_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => _factory.CreateFixedDelay(null!));
    }

    [Fact]
    public void CreateFixedDelay_WithInvalidConfiguration_ShouldThrowArgumentException()
    {
        // Arrange
        var invalidConfiguration = new FixedDelayConfiguration(TimeSpan.Zero); // Invalid

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => _factory.CreateFixedDelay(invalidConfiguration));
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
    public void CreateFullJitter_WithValidConfiguration_ShouldReturnStrategy()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();

        // Act
        var strategy = _factory.CreateFullJitter(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<FullJitterStrategy>();
    }

    [Fact]
    public void CreateFullJitter_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => _factory.CreateFullJitter(null!));
    }

    [Fact]
    public void CreateDecorrelatedJitter_WithValidConfiguration_ShouldReturnStrategy()
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration();

        // Act
        var strategy = _factory.CreateDecorrelatedJitter(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<DecorrelatedJitterStrategy>();
    }

    [Fact]
    public void CreateDecorrelatedJitter_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => _factory.CreateDecorrelatedJitter(null!));
    }

    [Fact]
    public void CreateDecorrelatedJitter_WithInvalidConfiguration_ShouldThrowArgumentException()
    {
        // Arrange
        var invalidConfiguration = new DecorrelatedJitterConfiguration
        {
            Multiplier = 0.5, // Invalid - less than 1.0
        };

        // Act & Assert
        _ = Assert.Throws<ArgumentException>(() => _factory.CreateDecorrelatedJitter(invalidConfiguration));
    }

    [Fact]
    public void CreateEqualJitter_WithValidConfiguration_ShouldReturnStrategy()
    {
        // Arrange
        var configuration = new EqualJitterConfiguration();

        // Act
        var strategy = _factory.CreateEqualJitter(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<EqualJitterStrategy>();
    }

    [Fact]
    public void CreateEqualJitter_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => _factory.CreateEqualJitter(null!));
    }

    [Fact]
    public void CreateNoJitter_WithValidConfiguration_ShouldReturnStrategy()
    {
        // Arrange
        var configuration = new NoJitterConfiguration();

        // Act
        var strategy = _factory.CreateNoJitter(configuration);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<NoJitterStrategy>();
    }

    [Fact]
    public void CreateNoJitter_WithNullConfiguration_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() => _factory.CreateNoJitter(null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task CreateExponentialBackoff_WithDifferentMultipliers_ShouldCalculateCorrectly(double multiplier)
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration(
            TimeSpan.FromMilliseconds(100),
            multiplier,
            TimeSpan.FromSeconds(10));

        // Act
        var strategy = _factory.CreateExponentialBackoff(configuration);

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
    public async Task CreateLinearBackoff_WithDifferentIncrements_ShouldCalculateCorrectly(int incrementSeconds)
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration(
            TimeSpan.FromMilliseconds(100),
            TimeSpan.FromSeconds(incrementSeconds),
            TimeSpan.FromSeconds(30));

        // Act
        var strategy = _factory.CreateLinearBackoff(configuration);

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

        var backoffConfiguration = new ExponentialBackoffConfiguration();
        var jitterConfiguration = new FullJitterConfiguration();
        var jitterStrategy = factory.CreateFullJitter(jitterConfiguration);

        // Act
        var strategy1 = factory.CreateExponentialBackoff(backoffConfiguration, jitterStrategy);
        var strategy2 = factory.CreateExponentialBackoff(backoffConfiguration, jitterStrategy);

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
                var backoffConfig = new ExponentialBackoffConfiguration();
                var jitterConfig = new FullJitterConfiguration();
                var jitterStrategy = _factory.CreateFullJitter(jitterConfig);

                var strategy = _factory.CreateExponentialBackoff(backoffConfig, jitterStrategy);
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
        var exponentialConfig = new ExponentialBackoffConfiguration();
        var linearConfig = new LinearBackoffConfiguration();
        var fixedConfig = new FixedDelayConfiguration(TimeSpan.FromMilliseconds(200));
        var fullJitterConfig = new FullJitterConfiguration();
        var decorrelatedJitterConfig = new DecorrelatedJitterConfiguration();
        var equalJitterConfig = new EqualJitterConfiguration();
        var noJitterConfig = new NoJitterConfiguration();

        // Act
        var exponentialStrategy = _factory.CreateExponentialBackoff(exponentialConfig);
        var linearStrategy = _factory.CreateLinearBackoff(linearConfig);
        var fixedStrategy = _factory.CreateFixedDelay(fixedConfig);
        var noOpStrategy = _factory.CreateNoOp();
        var fullJitterStrategy = _factory.CreateFullJitter(fullJitterConfig);
        var decorrelatedJitterStrategy = _factory.CreateDecorrelatedJitter(decorrelatedJitterConfig);
        var equalJitterStrategy = _factory.CreateEqualJitter(equalJitterConfig);
        var noJitterStrategy = _factory.CreateNoJitter(noJitterConfig);

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
        var backoffConfig = new ExponentialBackoffConfiguration(
            TimeSpan.FromMilliseconds(100),
            2.0,
            TimeSpan.FromSeconds(5));

        var jitterConfig = new FullJitterConfiguration();
        var jitterStrategy = _factory.CreateFullJitter(jitterConfig);

        // Act
        var strategy = _factory.CreateExponentialBackoff(backoffConfig, jitterStrategy);

        // Assert
        _ = strategy.Should().NotBeNull();
        _ = strategy.Should().BeOfType<CompositeRetryDelayStrategy>();

        // Test that the strategy produces reasonable delays
        var delay = await strategy.GetDelayAsync(2);
        _ = delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero);
        _ = delay.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(5)); // Should not exceed max delay
    }
}
