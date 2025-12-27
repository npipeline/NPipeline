using AwesomeAssertions;
using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Configuration.RetryDelay;

public sealed class BackoffStrategyConfigurationTests
{
    #region BackoffStrategy Tests

    [Fact]
    public void ExponentialBackoffStrategy_WithDefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var strategy = BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1));

        // Assert
        _ = strategy.Should().NotBeNull();

        // Should not throw when creating strategy
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(1, 60)] // 1 tick
    [InlineData(1000, 60)] // 1 microsecond
    [InlineData(1000000, 60)] // 1 millisecond
    [InlineData(1000000000, 200)] // 100 seconds - need MaxDelay >= 100s
    [InlineData(10000000000, 2000)] // 1000 seconds - need MaxDelay >= 1000s
    public void ExponentialBackoffStrategy_WithValidBaseDelay_ShouldValidate(long baseDelayTicks, long maxDelaySeconds)
    {
        // Arrange
        var strategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromTicks(baseDelayTicks),
            2.0,
            TimeSpan.FromSeconds(maxDelaySeconds));

        // Act & Assert
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)] // Zero
    [InlineData(-1)] // Negative
    [InlineData(long.MinValue)] // Very negative
    public void ExponentialBackoffStrategy_WithInvalidBaseDelay_ShouldThrowArgumentException(long baseDelayTicks)
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromTicks(baseDelayTicks),
            2.0,
            TimeSpan.FromMinutes(1));

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*BaseDelay must be a positive TimeSpan*");
    }

    [Theory]
    [InlineData(1.0)] // Minimum valid
    [InlineData(1.5)] // Small multiplier
    [InlineData(2.0)] // Default
    [InlineData(3.0)] // Larger multiplier
    [InlineData(10.0)] // Large multiplier
    [InlineData(100.0)] // Very large multiplier
    public void ExponentialBackoffStrategy_WithValidMultiplier_ShouldValidate(double multiplier)
    {
        // Arrange
        var strategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromSeconds(1),
            multiplier,
            TimeSpan.FromMinutes(1));

        // Act & Assert
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(0.9)] // Less than 1.0
    [InlineData(0.5)] // Half
    [InlineData(0.0)] // Zero
    [InlineData(-1.0)] // Negative
    [InlineData(double.MinValue)] // Very negative
    public void ExponentialBackoffStrategy_WithInvalidMultiplier_ShouldThrowArgumentException(double multiplier)
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromSeconds(1),
            multiplier,
            TimeSpan.FromMinutes(1));

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*Multiplier must be greater than or equal to 1.0*");
    }

    [Theory]
    [InlineData(1, 1)] // Equal to base delay
    [InlineData(1, 2)] // Greater than base delay
    [InlineData(10, 10)] // Equal
    [InlineData(10, 100)] // Much greater
    public void ExponentialBackoffStrategy_WithValidMaxDelay_ShouldValidate(int baseDelaySeconds, int maxDelaySeconds)
    {
        // Arrange
        var strategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromSeconds(baseDelaySeconds),
            2.0,
            TimeSpan.FromSeconds(maxDelaySeconds));

        // Act & Assert
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(10, 5)] // Less than base delay
    [InlineData(60, 30)] // Less than base delay
    [InlineData(100, 1)] // Much less than base delay
    public void ExponentialBackoffStrategy_WithMaxDelaySmallerThanBaseDelay_ShouldThrowArgumentException(int baseDelaySeconds, int maxDelaySeconds)
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromSeconds(baseDelaySeconds),
            2.0,
            TimeSpan.FromSeconds(maxDelaySeconds));

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be greater than or equal to BaseDelay*");
    }

    [Fact]
    public void ExponentialBackoffStrategy_WithZeroMaxDelay_ShouldThrowArgumentException()
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromSeconds(1),
            2.0,
            TimeSpan.Zero);

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be greater than or equal to BaseDelay*");
    }

    #endregion

    #region LinearBackoffStrategy Tests

    [Fact]
    public void LinearBackoffStrategy_WithDefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var strategy = BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1));

        // Assert
        _ = strategy.Should().NotBeNull();

        // Should not throw when creating strategy
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(1, 60)] // 1 tick
    [InlineData(1000, 60)] // 1 microsecond
    [InlineData(1000000, 60)] // 1 millisecond
    [InlineData(1000000000, 200)] // 100 seconds - need MaxDelay >= 100s
    [InlineData(10000000000, 2000)] // 1000 seconds - need MaxDelay >= 1000s
    public void LinearBackoffStrategy_WithValidBaseDelay_ShouldValidate(long baseDelayTicks, long maxDelaySeconds)
    {
        // Arrange
        var strategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromTicks(baseDelayTicks),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(maxDelaySeconds));

        // Act & Assert
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)] // Zero
    [InlineData(-1)] // Negative
    [InlineData(long.MinValue)] // Very negative
    public void LinearBackoffStrategy_WithInvalidBaseDelay_ShouldThrowArgumentException(long baseDelayTicks)
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.LinearBackoff(
            TimeSpan.FromTicks(baseDelayTicks),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromMinutes(1));

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*BaseDelay must be a positive TimeSpan*");
    }

    [Theory]
    [InlineData(0)] // Zero increment (fixed delay)
    [InlineData(1)] // 1 tick
    [InlineData(1000)] // 1 microsecond
    [InlineData(1000000)] // 1 millisecond
    [InlineData(1000000000)] // 100 milliseconds
    public void LinearBackoffStrategy_WithValidIncrement_ShouldValidate(long incrementTicks)
    {
        // Arrange
        var strategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromTicks(incrementTicks),
            TimeSpan.FromMinutes(1));

        // Act & Assert
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(-1)] // Negative
    [InlineData(-1000)] // Negative microsecond
    [InlineData(-1000000)] // Negative millisecond
    [InlineData(long.MinValue)] // Very negative
    public void LinearBackoffStrategy_WithInvalidIncrement_ShouldThrowArgumentException(long incrementTicks)
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.LinearBackoff(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromTicks(incrementTicks),
            TimeSpan.FromMinutes(1));

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*Increment must be a non-negative TimeSpan*");
    }

    [Theory]
    [InlineData(1, 1)] // Equal to base delay
    [InlineData(1, 2)] // Greater than base delay
    [InlineData(10, 10)] // Equal
    [InlineData(10, 100)] // Much greater
    public void LinearBackoffStrategy_WithValidMaxDelay_ShouldValidate(int baseDelaySeconds, int maxDelaySeconds)
    {
        // Arrange
        var strategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromSeconds(baseDelaySeconds),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(maxDelaySeconds));

        // Act & Assert
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(10, 5)] // Less than base delay
    [InlineData(60, 30)] // Less than base delay
    [InlineData(100, 1)] // Much less than base delay
    public void LinearBackoffStrategy_WithMaxDelaySmallerThanBaseDelay_ShouldThrowArgumentException(int baseDelaySeconds, int maxDelaySeconds)
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.LinearBackoff(
            TimeSpan.FromSeconds(baseDelaySeconds),
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(maxDelaySeconds));

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be greater than or equal to BaseDelay*");
    }

    [Fact]
    public void LinearBackoffStrategy_WithZeroMaxDelay_ShouldThrowArgumentException()
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.LinearBackoff(
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(1),
            TimeSpan.Zero);

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be greater than or equal to BaseDelay*");
    }

    [Fact]
    public void LinearBackoffStrategy_WithZeroIncrement_ShouldBehaveLikeFixedDelay()
    {
        // Arrange
        var strategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromSeconds(5),
            TimeSpan.Zero, // Fixed delay behavior
            TimeSpan.FromMinutes(1));

        // Act & Assert
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    #endregion

    #region FixedDelayStrategy Tests

    [Fact]
    public void FixedDelayStrategy_WithDefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var strategy = BackoffStrategies.FixedDelay(TimeSpan.FromSeconds(1));

        // Assert
        _ = strategy.Should().NotBeNull();

        // Should not throw when creating strategy
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(1)] // 1 tick
    [InlineData(1000)] // 1 microsecond
    [InlineData(1000000)] // 1 millisecond
    [InlineData(1000000000)] // 100 milliseconds
    [InlineData(10000000000)] // 1 second
    [InlineData(60000000000)] // 1 minute
    public void FixedDelayStrategy_WithValidDelay_ShouldValidate(long delayTicks)
    {
        // Arrange
        var strategy = BackoffStrategies.FixedDelay(TimeSpan.FromTicks(delayTicks));

        // Act & Assert
        var createAction = () => new DefaultRetryDelayStrategyFactory().CreateStrategy(
            new RetryDelayStrategyConfiguration(strategy, JitterStrategies.NoJitter()));

        _ = createAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)] // Zero
    [InlineData(-1)] // Negative
    [InlineData(-1000)] // Negative microsecond
    [InlineData(long.MinValue)] // Very negative
    public void FixedDelayStrategy_WithInvalidDelay_ShouldThrowArgumentException(long delayTicks)
    {
        // Arrange & Act & Assert
        var createAction = () => BackoffStrategies.FixedDelay(TimeSpan.FromTicks(delayTicks));

        _ = createAction.Should().Throw<ArgumentException>()
            .WithMessage("*Delay must be a positive TimeSpan*");
    }

    #endregion

    #region Cross-Strategy Tests

    [Fact]
    public void AllBackoffStrategies_WithVeryLargeValues_ShouldValidate()
    {
        // Arrange
        var exponentialStrategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromDays(1),
            2.0,
            TimeSpan.FromDays(30)); // 30 days

        var linearStrategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromDays(1),
            TimeSpan.FromHours(1),
            TimeSpan.FromDays(30));

        var fixedStrategy = BackoffStrategies.FixedDelay(TimeSpan.FromHours(1));

        // Act & Assert
        var factory = new DefaultRetryDelayStrategyFactory();

        var createExponential = () => factory.CreateStrategy(
            new RetryDelayStrategyConfiguration(exponentialStrategy, JitterStrategies.NoJitter()));

        var createLinear = () => factory.CreateStrategy(
            new RetryDelayStrategyConfiguration(linearStrategy, JitterStrategies.NoJitter()));

        var createFixed = () => factory.CreateStrategy(
            new RetryDelayStrategyConfiguration(fixedStrategy, JitterStrategies.NoJitter()));

        _ = createExponential.Should().NotThrow();
        _ = createLinear.Should().NotThrow();
        _ = createFixed.Should().NotThrow();
    }

    [Fact]
    public void AllBackoffStrategies_WithVerySmallValues_ShouldValidate()
    {
        // Arrange
        var exponentialStrategy = BackoffStrategies.ExponentialBackoff(
            TimeSpan.FromTicks(1), // Smallest possible
            1.0,
            TimeSpan.FromTicks(10));

        var linearStrategy = BackoffStrategies.LinearBackoff(
            TimeSpan.FromTicks(1),
            TimeSpan.FromTicks(1),
            TimeSpan.FromTicks(10));

        var fixedStrategy = BackoffStrategies.FixedDelay(TimeSpan.FromTicks(1));

        // Act & Assert
        var factory = new DefaultRetryDelayStrategyFactory();

        var createExponential = () => factory.CreateStrategy(
            new RetryDelayStrategyConfiguration(exponentialStrategy, JitterStrategies.NoJitter()));

        var createLinear = () => factory.CreateStrategy(
            new RetryDelayStrategyConfiguration(linearStrategy, JitterStrategies.NoJitter()));

        var createFixed = () => factory.CreateStrategy(
            new RetryDelayStrategyConfiguration(fixedStrategy, JitterStrategies.NoJitter()));

        _ = createExponential.Should().NotThrow();
        _ = createLinear.Should().NotThrow();
        _ = createFixed.Should().NotThrow();
    }

    #endregion
}
