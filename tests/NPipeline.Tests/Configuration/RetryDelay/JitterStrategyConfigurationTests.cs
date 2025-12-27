using AwesomeAssertions;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Configuration.RetryDelay;

public sealed class JitterStrategyConfigurationTests
{
    #region FullJitterStrategy Tests

    [Fact]
    public void FullJitterStrategy_ShouldBeValid()
    {
        // Arrange & Act
        var jitterStrategy = JitterStrategies.FullJitter();

        // Assert - Full jitter should be a valid delegate
        _ = jitterStrategy.Should().NotBeNull();

        // Should not throw when executing with valid inputs
        var action = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
        _ = action.Should().NotThrow();
    }

    [Fact]
    public void FullJitterStrategy_WithVariousInputs_ShouldReturnValidTimeSpan()
    {
        // Arrange
        var jitterStrategy = JitterStrategies.FullJitter();
        var random = new Random(42); // Fixed seed for reproducible tests

        // Act & Assert
        var result1 = jitterStrategy(TimeSpan.FromMilliseconds(100), random);
        var result2 = jitterStrategy(TimeSpan.FromSeconds(1), random);
        var result3 = jitterStrategy(TimeSpan.FromMinutes(1), random);

        // All results should be positive and less than or equal to the input
        _ = result1.Should().BeGreaterThan(TimeSpan.Zero);
        _ = result1.Should().BeLessThanOrEqualTo(TimeSpan.FromMilliseconds(100));

        _ = result2.Should().BeGreaterThan(TimeSpan.Zero);
        _ = result2.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(1));

        _ = result3.Should().BeGreaterThan(TimeSpan.Zero);
        _ = result3.Should().BeLessThanOrEqualTo(TimeSpan.FromMinutes(1));
    }

    #endregion

    #region DecorrelatedJitterStrategy Tests

    [Fact]
    public void DecorrelatedJitterStrategy_DefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var jitterStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1));

        // Assert
        _ = jitterStrategy.Should().NotBeNull();

        // Should not throw when executing with valid inputs
        var action = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
        _ = action.Should().NotThrow();
    }

    [Theory]
    [InlineData(1)] // 1 tick
    [InlineData(1000)] // 1 microsecond
    [InlineData(1000000)] // 1 millisecond
    [InlineData(1000000000)] // 100 milliseconds
    [InlineData(10000000000)] // 1 second
    [InlineData(60000000000)] // 1 minute
    public void DecorrelatedJitterStrategy_WithValidMaxDelay_ShouldValidate(long maxDelayTicks)
    {
        // Arrange
        var maxDelay = TimeSpan.FromTicks(maxDelayTicks);
        var jitterStrategy = JitterStrategies.DecorrelatedJitter(maxDelay);

        // Act & Assert
        _ = jitterStrategy.Should().NotBeNull();

        // Should not throw when executing with valid inputs
        var action = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
        _ = action.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)] // Zero
    [InlineData(-1)] // Negative
    [InlineData(-1000)] // Negative microsecond
    [InlineData(long.MinValue)] // Very negative
    public void DecorrelatedJitterStrategy_WithInvalidMaxDelay_ShouldThrowArgumentException(long maxDelayTicks)
    {
        // Arrange
        var maxDelay = TimeSpan.FromTicks(maxDelayTicks);

        // Act & Assert
        var action = () => JitterStrategies.DecorrelatedJitter(maxDelay);

        _ = action.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be a positive TimeSpan*")
            .And.ParamName.Should().Be("maxDelay");
    }

    [Theory]
    [InlineData(1.0)] // Minimum valid
    [InlineData(1.5)] // Small multiplier
    [InlineData(2.0)] // Small multiplier
    [InlineData(3.0)] // Default
    [InlineData(5.0)] // Larger multiplier
    [InlineData(10.0)] // Large multiplier
    [InlineData(100.0)] // Very large multiplier
    public void DecorrelatedJitterStrategy_WithValidMultiplier_ShouldValidate(double multiplier)
    {
        // Arrange
        var maxDelay = TimeSpan.FromMinutes(1);
        var jitterStrategy = JitterStrategies.DecorrelatedJitter(maxDelay, multiplier);

        // Act & Assert
        _ = jitterStrategy.Should().NotBeNull();

        // Should not throw when executing with valid inputs
        var action = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
        _ = action.Should().NotThrow();
    }

    [Theory]
    [InlineData(0.9)] // Less than 1.0
    [InlineData(0.5)] // Half
    [InlineData(0.0)] // Zero
    [InlineData(-1.0)] // Negative
    [InlineData(-10.0)] // More negative
    [InlineData(double.MinValue)] // Very negative
    public void DecorrelatedJitterStrategy_WithInvalidMultiplier_ShouldThrowArgumentException(double multiplier)
    {
        // Arrange
        var maxDelay = TimeSpan.FromMinutes(1);

        // Act & Assert
        var action = () => JitterStrategies.DecorrelatedJitter(maxDelay, multiplier);

        _ = action.Should().Throw<ArgumentException>()
            .WithMessage("*Multiplier must be greater than or equal to 1.0*")
            .And.ParamName.Should().Be("multiplier");
    }

    #endregion

    #region EqualJitterStrategy Tests

    [Fact]
    public void EqualJitterStrategy_ShouldBeValid()
    {
        // Arrange & Act
        var jitterStrategy = JitterStrategies.EqualJitter();

        // Assert - Equal jitter should be a valid delegate
        _ = jitterStrategy.Should().NotBeNull();

        // Should not throw when executing with valid inputs
        var action = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
        _ = action.Should().NotThrow();
    }

    [Fact]
    public void EqualJitterStrategy_WithVariousInputs_ShouldReturnValidTimeSpan()
    {
        // Arrange
        var jitterStrategy = JitterStrategies.EqualJitter();
        var random = new Random(42); // Fixed seed for reproducible tests

        // Act & Assert
        var result1 = jitterStrategy(TimeSpan.FromMilliseconds(100), random);
        var result2 = jitterStrategy(TimeSpan.FromSeconds(1), random);
        var result3 = jitterStrategy(TimeSpan.FromMinutes(1), random);

        // All results should be positive and less than or equal to the input
        _ = result1.Should().BeGreaterThan(TimeSpan.Zero);
        _ = result1.Should().BeLessThanOrEqualTo(TimeSpan.FromMilliseconds(100));

        _ = result2.Should().BeGreaterThan(TimeSpan.Zero);
        _ = result2.Should().BeLessThanOrEqualTo(TimeSpan.FromSeconds(1));

        _ = result3.Should().BeGreaterThan(TimeSpan.Zero);
        _ = result3.Should().BeLessThanOrEqualTo(TimeSpan.FromMinutes(1));
    }

    #endregion

    #region NoJitterStrategy Tests

    [Fact]
    public void NoJitterStrategy_ShouldBeValid()
    {
        // Arrange & Act
        var jitterStrategy = JitterStrategies.NoJitter();

        // Assert - No jitter should be a valid delegate
        _ = jitterStrategy.Should().NotBeNull();

        // Should not throw when executing with valid inputs
        var action = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
        _ = action.Should().NotThrow();
    }

    [Fact]
    public void NoJitterStrategy_WithVariousInputs_ShouldReturnInputTimeSpan()
    {
        // Arrange
        var jitterStrategy = JitterStrategies.NoJitter();
        var random = new Random(42); // Fixed seed for reproducible tests

        // Act & Assert
        var input1 = TimeSpan.FromMilliseconds(100);
        var input2 = TimeSpan.FromSeconds(1);
        var input3 = TimeSpan.FromMinutes(1);

        var result1 = jitterStrategy(input1, random);
        var result2 = jitterStrategy(input2, random);
        var result3 = jitterStrategy(input3, random);

        // No jitter should return the input TimeSpan exactly
        _ = result1.Should().Be(input1);
        _ = result2.Should().Be(input2);
        _ = result3.Should().Be(input3);
    }

    #endregion

    #region Cross-Strategy Tests

    [Fact]
    public void AllJitterStrategies_WithVeryLargeValues_ShouldValidate()
    {
        // Arrange
        var decorrelatedStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromDays(30), 100.0);
        var fullStrategy = JitterStrategies.FullJitter();
        var equalStrategy = JitterStrategies.EqualJitter();
        var noStrategy = JitterStrategies.NoJitter();

        // Act & Assert
        var validateDecorrelated = () => decorrelatedStrategy(TimeSpan.FromSeconds(1), new Random());
        var validateFull = () => fullStrategy(TimeSpan.FromSeconds(1), new Random());
        var validateEqual = () => equalStrategy(TimeSpan.FromSeconds(1), new Random());
        var validateNo = () => noStrategy(TimeSpan.FromSeconds(1), new Random());

        _ = validateDecorrelated.Should().NotThrow();
        _ = validateFull.Should().NotThrow();
        _ = validateEqual.Should().NotThrow();
        _ = validateNo.Should().NotThrow();
    }

    [Fact]
    public void AllJitterStrategies_WithVerySmallValues_ShouldValidate()
    {
        // Arrange
        var decorrelatedStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromTicks(1), 1.0);
        var fullStrategy = JitterStrategies.FullJitter();
        var equalStrategy = JitterStrategies.EqualJitter();
        var noStrategy = JitterStrategies.NoJitter();

        // Act & Assert
        var validateDecorrelated = () => decorrelatedStrategy(TimeSpan.FromTicks(1), new Random());
        var validateFull = () => fullStrategy(TimeSpan.FromTicks(1), new Random());
        var validateEqual = () => equalStrategy(TimeSpan.FromTicks(1), new Random());
        var validateNo = () => noStrategy(TimeSpan.FromTicks(1), new Random());

        _ = validateDecorrelated.Should().NotThrow();
        _ = validateFull.Should().NotThrow();
        _ = validateEqual.Should().NotThrow();
        _ = validateNo.Should().NotThrow();
    }

    [Fact]
    public void AllJitterStrategies_WithBoundaryMultiplierValues_ShouldValidate()
    {
        // Arrange
        var decorrelatedStrategy1 = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1), 1.0);
        var decorrelatedStrategy2 = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1), double.MaxValue);
        var fullStrategy = JitterStrategies.FullJitter();
        var equalStrategy = JitterStrategies.EqualJitter();
        var noStrategy = JitterStrategies.NoJitter();

        // Act & Assert
        var validate1 = () => decorrelatedStrategy1(TimeSpan.FromSeconds(1), new Random());
        var validate2 = () => decorrelatedStrategy2(TimeSpan.FromSeconds(1), new Random());
        var validateFull = () => fullStrategy(TimeSpan.FromSeconds(1), new Random());
        var validateEqual = () => equalStrategy(TimeSpan.FromSeconds(1), new Random());
        var validateNo = () => noStrategy(TimeSpan.FromSeconds(1), new Random());

        _ = validate1.Should().NotThrow();
        _ = validate2.Should().NotThrow();
        _ = validateFull.Should().NotThrow();
        _ = validateEqual.Should().NotThrow();
        _ = validateNo.Should().NotThrow();
    }

    [Theory]
    [InlineData(1.0)] // Equal to 1.0
    [InlineData(1.0000001)] // Just above 1.0
    [InlineData(1.5)] // Small fraction
    [InlineData(2.0)] // Integer
    [InlineData(2.5)] // Half fraction
    [InlineData(10.0)] // Round number
    [InlineData(10.5)] // Round with fraction
    public void DecorrelatedJitterStrategy_WithFractionalMultipliers_ShouldValidate(double multiplier)
    {
        // Arrange
        var jitterStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1), multiplier);

        // Act & Assert
        var validateAction = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
        _ = validateAction.Should().NotThrow();
    }

    [Fact]
    public void AllJitterStrategies_ShouldHaveConsistentBehavior()
    {
        // Arrange
        var fullStrategy = JitterStrategies.FullJitter();
        var equalStrategy = JitterStrategies.EqualJitter();
        var noStrategy = JitterStrategies.NoJitter();

        // Act & Assert - All should be valid delegates
        _ = fullStrategy.Should().NotBeNull();
        _ = equalStrategy.Should().NotBeNull();
        _ = noStrategy.Should().NotBeNull();

        // All should execute without throwing
        var validateFull = () => fullStrategy(TimeSpan.FromSeconds(1), new Random());
        var validateEqual = () => equalStrategy(TimeSpan.FromSeconds(1), new Random());
        var validateNo = () => noStrategy(TimeSpan.FromSeconds(1), new Random());

        _ = validateFull.Should().NotThrow();
        _ = validateEqual.Should().NotThrow();
        _ = validateNo.Should().NotThrow();
    }

    #endregion
}
