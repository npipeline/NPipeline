using AwesomeAssertions;
using NPipeline.Execution.RetryDelay.Jitter;

namespace NPipeline.Tests.Configuration.RetryDelay;

public sealed class JitterStrategyConfigurationTests
{
    #region FullJitterConfiguration Tests

    [Fact]
    public void FullJitterConfiguration_DefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var configuration = new FullJitterConfiguration();

        // Assert - Full jitter doesn't have any configurable properties
        // Should not throw when validating defaults
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Fact]
    public void FullJitterConfiguration_Validate_ShouldAlwaysSucceed()
    {
        // Arrange
        var configuration = new FullJitterConfiguration();

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    #endregion

    #region DecorrelatedJitterConfiguration Tests

    [Fact]
    public void DecorrelatedJitterConfiguration_DefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var configuration = new DecorrelatedJitterConfiguration();

        // Assert
        _ = configuration.MaxDelay.Should().Be(TimeSpan.FromMinutes(1));
        _ = configuration.Multiplier.Should().Be(3.0);

        // Should not throw when validating defaults
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(1)] // 1 tick
    [InlineData(1000)] // 1 microsecond
    [InlineData(1000000)] // 1 millisecond
    [InlineData(1000000000)] // 100 milliseconds
    [InlineData(10000000000)] // 1 second
    [InlineData(60000000000)] // 1 minute
    public void DecorrelatedJitterConfiguration_WithValidMaxDelay_ShouldValidate(long maxDelayTicks)
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromTicks(maxDelayTicks),
            Multiplier = 3.0,
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)] // Zero
    [InlineData(-1)] // Negative
    [InlineData(-1000)] // Negative microsecond
    [InlineData(long.MinValue)] // Very negative
    public void DecorrelatedJitterConfiguration_WithInvalidMaxDelay_ShouldThrowArgumentException(long maxDelayTicks)
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromTicks(maxDelayTicks),
            Multiplier = 3.0,
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be a positive TimeSpan*")
            .And.ParamName.Should().Be("MaxDelay");
    }

    [Theory]
    [InlineData(1.0)] // Minimum valid
    [InlineData(1.5)] // Small multiplier
    [InlineData(2.0)] // Small multiplier
    [InlineData(3.0)] // Default
    [InlineData(5.0)] // Larger multiplier
    [InlineData(10.0)] // Large multiplier
    [InlineData(100.0)] // Very large multiplier
    public void DecorrelatedJitterConfiguration_WithValidMultiplier_ShouldValidate(double multiplier)
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromMinutes(1),
            Multiplier = multiplier,
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(0.9)] // Less than 1.0
    [InlineData(0.5)] // Half
    [InlineData(0.0)] // Zero
    [InlineData(-1.0)] // Negative
    [InlineData(-10.0)] // More negative
    [InlineData(double.MinValue)] // Very negative
    public void DecorrelatedJitterConfiguration_WithInvalidMultiplier_ShouldThrowArgumentException(double multiplier)
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromMinutes(1),
            Multiplier = multiplier,
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*Multiplier must be greater than or equal to 1.0*")
            .And.ParamName.Should().Be("Multiplier");
    }

    #endregion

    #region EqualJitterConfiguration Tests

    [Fact]
    public void EqualJitterConfiguration_DefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var configuration = new EqualJitterConfiguration();

        // Assert - Equal jitter doesn't have any configurable properties
        // Should not throw when validating defaults
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Fact]
    public void EqualJitterConfiguration_Validate_ShouldAlwaysSucceed()
    {
        // Arrange
        var configuration = new EqualJitterConfiguration();

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    #endregion

    #region NoJitterConfiguration Tests

    [Fact]
    public void NoJitterConfiguration_DefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var configuration = new NoJitterConfiguration();

        // Assert - No jitter doesn't have any configurable properties
        // Should not throw when validating defaults
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Fact]
    public void NoJitterConfiguration_Validate_ShouldAlwaysSucceed()
    {
        // Arrange
        var configuration = new NoJitterConfiguration();

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    #endregion

    #region Cross-Configuration Tests

    [Fact]
    public void AllJitterConfigurations_WithVeryLargeValues_ShouldValidate()
    {
        // Arrange
        var decorrelatedConfig = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromDays(30), // 30 days
            Multiplier = 100.0,
        };

        var fullConfig = new FullJitterConfiguration();
        var equalConfig = new EqualJitterConfiguration();
        var noConfig = new NoJitterConfiguration();

        // Act & Assert
        var validateDecorrelated = () => decorrelatedConfig.Validate();
        var validateFull = () => fullConfig.Validate();
        var validateEqual = () => equalConfig.Validate();
        var validateNo = () => noConfig.Validate();

        _ = validateDecorrelated.Should().NotThrow();
        _ = validateFull.Should().NotThrow();
        _ = validateEqual.Should().NotThrow();
        _ = validateNo.Should().NotThrow();
    }

    [Fact]
    public void AllJitterConfigurations_WithVerySmallValues_ShouldValidate()
    {
        // Arrange
        var decorrelatedConfig = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromTicks(1), // Smallest possible
            Multiplier = 1.0,
        };

        var fullConfig = new FullJitterConfiguration();
        var equalConfig = new EqualJitterConfiguration();
        var noConfig = new NoJitterConfiguration();

        // Act & Assert
        var validateDecorrelated = () => decorrelatedConfig.Validate();
        var validateFull = () => fullConfig.Validate();
        var validateEqual = () => equalConfig.Validate();
        var validateNo = () => noConfig.Validate();

        _ = validateDecorrelated.Should().NotThrow();
        _ = validateFull.Should().NotThrow();
        _ = validateEqual.Should().NotThrow();
        _ = validateNo.Should().NotThrow();
    }

    [Fact]
    public void AllJitterConfigurations_WithBoundaryMultiplierValues_ShouldValidate()
    {
        // Arrange
        var decorrelatedConfig1 = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromMinutes(1),
            Multiplier = 1.0, // Minimum valid
        };

        var decorrelatedConfig2 = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromMinutes(1),
            Multiplier = double.MaxValue, // Maximum possible
        };

        // Act & Assert
        var validate1 = () => decorrelatedConfig1.Validate();
        var validate2 = () => decorrelatedConfig2.Validate();

        _ = validate1.Should().NotThrow();
        _ = validate2.Should().NotThrow();
    }

    [Theory]
    [InlineData(1.0)] // Equal to 1.0
    [InlineData(1.0000001)] // Just above 1.0
    [InlineData(1.5)] // Small fraction
    [InlineData(2.0)] // Integer
    [InlineData(2.5)] // Half fraction
    [InlineData(10.0)] // Round number
    [InlineData(10.5)] // Round with fraction
    public void DecorrelatedJitterConfiguration_WithFractionalMultipliers_ShouldValidate(double multiplier)
    {
        // Arrange
        var configuration = new DecorrelatedJitterConfiguration
        {
            MaxDelay = TimeSpan.FromMinutes(1),
            Multiplier = multiplier,
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Fact]
    public void AllJitterConfigurations_ShouldHaveConsistentValidationPattern()
    {
        // Arrange
        var fullConfig = new FullJitterConfiguration();
        var equalConfig = new EqualJitterConfiguration();
        var noConfig = new NoJitterConfiguration();

        // Act & Assert - All should validate without throwing
        var validateFull = () => fullConfig.Validate();
        var validateEqual = () => equalConfig.Validate();
        var validateNo = () => noConfig.Validate();

        _ = validateFull.Should().NotThrow();
        _ = validateEqual.Should().NotThrow();
        _ = validateNo.Should().NotThrow();

        // All non-parameterized configurations should have Validate methods for consistency
        _ = fullConfig.Should().BeOfType<FullJitterConfiguration>();
        _ = equalConfig.Should().BeOfType<EqualJitterConfiguration>();
        _ = noConfig.Should().BeOfType<NoJitterConfiguration>();
    }

    #endregion
}
