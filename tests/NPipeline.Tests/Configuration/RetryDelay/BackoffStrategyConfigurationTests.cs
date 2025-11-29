using AwesomeAssertions;
using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Tests.Configuration.RetryDelay;

public sealed class BackoffStrategyConfigurationTests
{
    #region ExponentialBackoffConfiguration Tests

    [Fact]
    public void ExponentialBackoffConfiguration_DefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var configuration = new ExponentialBackoffConfiguration();

        // Assert
        _ = configuration.BaseDelay.Should().Be(TimeSpan.FromSeconds(1));
        _ = configuration.Multiplier.Should().Be(2.0);
        _ = configuration.MaxDelay.Should().Be(TimeSpan.FromMinutes(1));

        // Should not throw when validating defaults
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(1, 60)] // 1 tick
    [InlineData(1000, 60)] // 1 microsecond
    [InlineData(1000000, 60)] // 1 millisecond
    [InlineData(1000000000, 200)] // 100 seconds - need MaxDelay >= 100s
    [InlineData(10000000000, 2000)] // 1000 seconds - need MaxDelay >= 1000s
    public void ExponentialBackoffConfiguration_WithValidBaseDelay_ShouldValidate(long baseDelayTicks, long maxDelaySeconds)
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromTicks(baseDelayTicks),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(maxDelaySeconds),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)] // Zero
    [InlineData(-1)] // Negative
    [InlineData(long.MinValue)] // Very negative
    public void ExponentialBackoffConfiguration_WithInvalidBaseDelay_ShouldThrowArgumentException(long baseDelayTicks)
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromTicks(baseDelayTicks),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromMinutes(1),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*BaseDelay must be a positive TimeSpan*")
            .And.ParamName.Should().Be("BaseDelay");
    }

    [Theory]
    [InlineData(1.0)] // Minimum valid
    [InlineData(1.5)] // Small multiplier
    [InlineData(2.0)] // Default
    [InlineData(3.0)] // Larger multiplier
    [InlineData(10.0)] // Large multiplier
    [InlineData(100.0)] // Very large multiplier
    public void ExponentialBackoffConfiguration_WithValidMultiplier_ShouldValidate(double multiplier)
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            Multiplier = multiplier,
            MaxDelay = TimeSpan.FromMinutes(1),
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
    [InlineData(double.MinValue)] // Very negative
    public void ExponentialBackoffConfiguration_WithInvalidMultiplier_ShouldThrowArgumentException(double multiplier)
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            Multiplier = multiplier,
            MaxDelay = TimeSpan.FromMinutes(1),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*Multiplier must be greater than or equal to 1.0*")
            .And.ParamName.Should().Be("Multiplier");
    }

    [Theory]
    [InlineData(1, 1)] // Equal to base delay
    [InlineData(1, 2)] // Greater than base delay
    [InlineData(10, 10)] // Equal
    [InlineData(10, 100)] // Much greater
    public void ExponentialBackoffConfiguration_WithValidMaxDelay_ShouldValidate(int baseDelaySeconds, int maxDelaySeconds)
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(baseDelaySeconds),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(maxDelaySeconds),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(10, 5)] // Less than base delay
    [InlineData(60, 30)] // Less than base delay
    [InlineData(100, 1)] // Much less than base delay
    public void ExponentialBackoffConfiguration_WithMaxDelaySmallerThanBaseDelay_ShouldThrowArgumentException(int baseDelaySeconds, int maxDelaySeconds)
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(baseDelaySeconds),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(maxDelaySeconds),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be greater than or equal to BaseDelay*")
            .And.ParamName.Should().Be("MaxDelay");
    }

    [Fact]
    public void ExponentialBackoffConfiguration_WithZeroMaxDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.Zero,
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be greater than or equal to BaseDelay*")
            .And.ParamName.Should().Be("MaxDelay");
    }

    #endregion

    #region LinearBackoffConfiguration Tests

    [Fact]
    public void LinearBackoffConfiguration_DefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var configuration = new LinearBackoffConfiguration();

        // Assert
        _ = configuration.BaseDelay.Should().Be(TimeSpan.FromSeconds(1));
        _ = configuration.Increment.Should().Be(TimeSpan.FromSeconds(1));
        _ = configuration.MaxDelay.Should().Be(TimeSpan.FromMinutes(1));

        // Should not throw when validating defaults
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(1, 60)] // 1 tick
    [InlineData(1000, 60)] // 1 microsecond
    [InlineData(1000000, 60)] // 1 millisecond
    [InlineData(1000000000, 200)] // 100 seconds - need MaxDelay >= 100s
    [InlineData(10000000000, 2000)] // 1000 seconds - need MaxDelay >= 1000s
    public void LinearBackoffConfiguration_WithValidBaseDelay_ShouldValidate(long baseDelayTicks, long maxDelaySeconds)
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromTicks(baseDelayTicks),
            Increment = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.FromSeconds(maxDelaySeconds),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)] // Zero
    [InlineData(-1)] // Negative
    [InlineData(long.MinValue)] // Very negative
    public void LinearBackoffConfiguration_WithInvalidBaseDelay_ShouldThrowArgumentException(long baseDelayTicks)
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromTicks(baseDelayTicks),
            Increment = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.FromMinutes(1),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*BaseDelay must be a positive TimeSpan*")
            .And.ParamName.Should().Be("BaseDelay");
    }

    [Theory]
    [InlineData(0)] // Zero increment (fixed delay)
    [InlineData(1)] // 1 tick
    [InlineData(1000)] // 1 microsecond
    [InlineData(1000000)] // 1 millisecond
    [InlineData(1000000000)] // 100 milliseconds
    public void LinearBackoffConfiguration_WithValidIncrement_ShouldValidate(long incrementTicks)
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            Increment = TimeSpan.FromTicks(incrementTicks),
            MaxDelay = TimeSpan.FromMinutes(1),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(-1)] // Negative
    [InlineData(-1000)] // Negative microsecond
    [InlineData(-1000000)] // Negative millisecond
    [InlineData(long.MinValue)] // Very negative
    public void LinearBackoffConfiguration_WithInvalidIncrement_ShouldThrowArgumentException(long incrementTicks)
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            Increment = TimeSpan.FromTicks(incrementTicks),
            MaxDelay = TimeSpan.FromMinutes(1),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*Increment must be a non-negative TimeSpan*")
            .And.ParamName.Should().Be("Increment");
    }

    [Theory]
    [InlineData(1, 1)] // Equal to base delay
    [InlineData(1, 2)] // Greater than base delay
    [InlineData(10, 10)] // Equal
    [InlineData(10, 100)] // Much greater
    public void LinearBackoffConfiguration_WithValidMaxDelay_ShouldValidate(int baseDelaySeconds, int maxDelaySeconds)
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(baseDelaySeconds),
            Increment = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.FromSeconds(maxDelaySeconds),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(10, 5)] // Less than base delay
    [InlineData(60, 30)] // Less than base delay
    [InlineData(100, 1)] // Much less than base delay
    public void LinearBackoffConfiguration_WithMaxDelaySmallerThanBaseDelay_ShouldThrowArgumentException(int baseDelaySeconds, int maxDelaySeconds)
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(baseDelaySeconds),
            Increment = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.FromSeconds(maxDelaySeconds),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be greater than or equal to BaseDelay*")
            .And.ParamName.Should().Be("MaxDelay");
    }

    [Fact]
    public void LinearBackoffConfiguration_WithZeroMaxDelay_ShouldThrowArgumentException()
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(1),
            Increment = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.Zero,
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be greater than or equal to BaseDelay*")
            .And.ParamName.Should().Be("MaxDelay");
    }

    [Fact]
    public void LinearBackoffConfiguration_WithZeroIncrement_ShouldBehaveLikeFixedDelay()
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(5),
            Increment = TimeSpan.Zero, // Fixed delay behavior
            MaxDelay = TimeSpan.FromMinutes(1),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    #endregion

    #region FixedDelayConfiguration Tests

    [Fact]
    public void FixedDelayConfiguration_DefaultValues_ShouldBeValid()
    {
        // Arrange & Act
        var configuration = new FixedDelayConfiguration();

        // Assert
        _ = configuration.Delay.Should().Be(TimeSpan.FromSeconds(1));

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
    public void FixedDelayConfiguration_WithValidDelay_ShouldValidate(long delayTicks)
    {
        // Arrange
        var configuration = new FixedDelayConfiguration
        {
            Delay = TimeSpan.FromTicks(delayTicks),
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
    public void FixedDelayConfiguration_WithInvalidDelay_ShouldThrowArgumentException(long delayTicks)
    {
        // Arrange
        var configuration = new FixedDelayConfiguration
        {
            Delay = TimeSpan.FromTicks(delayTicks),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();

        _ = validateAction.Should().Throw<ArgumentException>()
            .WithMessage("*Delay must be a positive TimeSpan*")
            .And.ParamName.Should().Be("Delay");
    }

    #endregion

    #region Cross-Configuration Tests

    [Fact]
    public void AllBackoffConfigurations_WithVeryLargeValues_ShouldValidate()
    {
        // Arrange
        var exponentialConfig = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromDays(1),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromDays(30), // 30 days
        };

        var linearConfig = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromDays(1),
            Increment = TimeSpan.FromHours(1),
            MaxDelay = TimeSpan.FromDays(30),
        };

        var fixedConfig = new FixedDelayConfiguration
        {
            Delay = TimeSpan.FromHours(1),
        };

        // Act & Assert
        var validateExponential = () => exponentialConfig.Validate();
        var validateLinear = () => linearConfig.Validate();
        var validateFixed = () => fixedConfig.Validate();

        _ = validateExponential.Should().NotThrow();
        _ = validateLinear.Should().NotThrow();
        _ = validateFixed.Should().NotThrow();
    }

    [Fact]
    public void AllBackoffConfigurations_WithVerySmallValues_ShouldValidate()
    {
        // Arrange
        var exponentialConfig = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromTicks(1), // Smallest possible
            Multiplier = 1.0,
            MaxDelay = TimeSpan.FromTicks(10),
        };

        var linearConfig = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromTicks(1),
            Increment = TimeSpan.FromTicks(1),
            MaxDelay = TimeSpan.FromTicks(10),
        };

        var fixedConfig = new FixedDelayConfiguration
        {
            Delay = TimeSpan.FromTicks(1),
        };

        // Act & Assert
        var validateExponential = () => exponentialConfig.Validate();
        var validateLinear = () => linearConfig.Validate();
        var validateFixed = () => fixedConfig.Validate();

        _ = validateExponential.Should().NotThrow();
        _ = validateLinear.Should().NotThrow();
        _ = validateFixed.Should().NotThrow();
    }

    #endregion
}
