using System.Diagnostics;
using AwesomeAssertions;
using NPipeline.Execution.RetryDelay;
using NPipeline.Execution.RetryDelay.Backoff;

namespace NPipeline.Tests.Configuration.RetryDelay;

public sealed class RetryDelayStrategyConfigurationTests
{
    #region Configuration Performance Tests

    [Fact]
    public void ConfigurationValidation_ShouldBePerformant()
    {
        // Arrange
        var configurations = new object[]
        {
            new ExponentialBackoffConfiguration(),
            new LinearBackoffConfiguration(),
            new FixedDelayConfiguration(),
            JitterStrategies.FullJitter(),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1)),
            JitterStrategies.EqualJitter(),
            JitterStrategies.NoJitter(),
        };

        // Act
        var stopwatch = Stopwatch.StartNew();

        for (var i = 0; i < 10000; i++)
        {
            foreach (var config in configurations)
            {
                // For jitter strategies, just execute them to simulate validation
                if (config is JitterStrategy jitterStrategy)
                    _ = jitterStrategy(TimeSpan.FromSeconds(1), new Random());
                else
                    ((dynamic)config).Validate();
            }
        }

        stopwatch.Stop();

        // Assert
        _ = stopwatch.ElapsedMilliseconds.Should().BeLessThan(1000); // Should complete in less than 1 second
    }

    #endregion

    #region Configuration Validation Integration Tests

    [Fact]
    public void AllConfigurations_WithDefaultValues_ShouldValidate()
    {
        // Arrange
        var exponentialConfig = new ExponentialBackoffConfiguration();
        var linearConfig = new LinearBackoffConfiguration();
        var fixedConfig = new FixedDelayConfiguration();
        var fullJitterStrategy = JitterStrategies.FullJitter();
        var decorrelatedJitterStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1));
        var equalJitterStrategy = JitterStrategies.EqualJitter();
        var noJitterStrategy = JitterStrategies.NoJitter();

        // Act & Assert
        _ = exponentialConfig.Should().NotBeNull();
        _ = linearConfig.Should().NotBeNull();
        _ = fixedConfig.Should().NotBeNull();
        _ = fullJitterStrategy.Should().NotBeNull();
        _ = decorrelatedJitterStrategy.Should().NotBeNull();
        _ = equalJitterStrategy.Should().NotBeNull();
        _ = noJitterStrategy.Should().NotBeNull();

        // Validate configurations
        var validateExponential = () => exponentialConfig.Validate();
        var validateLinear = () => linearConfig.Validate();
        var validateFixed = () => fixedConfig.Validate();

        _ = validateExponential.Should().NotThrow();
        _ = validateLinear.Should().NotThrow();
        _ = validateFixed.Should().NotThrow();

        // Test jitter strategies
        Action validateFullJitter = () => fullJitterStrategy(TimeSpan.FromSeconds(1), new Random());
        Action validateDecorrelatedJitter = () => decorrelatedJitterStrategy(TimeSpan.FromSeconds(1), new Random());
        Action validateEqualJitter = () => equalJitterStrategy(TimeSpan.FromSeconds(1), new Random());
        Action validateNoJitter = () => noJitterStrategy(TimeSpan.FromSeconds(1), new Random());

        _ = validateFullJitter.Should().NotThrow();
        _ = validateDecorrelatedJitter.Should().NotThrow();
        _ = validateEqualJitter.Should().NotThrow();
        _ = validateNoJitter.Should().NotThrow();
    }

    [Fact]
    public void ConfigurationValidation_ShouldProvideMeaningfulErrorMessages()
    {
        // Arrange & Act & Assert
        var exponentialConfig = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.Zero,
        };

        var validateExponential = () => exponentialConfig.Validate();

        _ = validateExponential.Should().Throw<ArgumentException>()
            .WithMessage("*BaseDelay must be a positive TimeSpan*")
            .And.ParamName.Should().Be("BaseDelay");

        var linearConfig = new LinearBackoffConfiguration
        {
            Increment = TimeSpan.FromSeconds(-1),
        };

        var validateLinear = () => linearConfig.Validate();

        _ = validateLinear.Should().Throw<ArgumentException>()
            .WithMessage("*Increment must be a non-negative TimeSpan*")
            .And.ParamName.Should().Be("Increment");

        var fixedConfig = new FixedDelayConfiguration
        {
            Delay = TimeSpan.Zero,
        };

        var validateFixed = () => fixedConfig.Validate();

        _ = validateFixed.Should().Throw<ArgumentException>()
            .WithMessage("*Delay must be a positive TimeSpan*")
            .And.ParamName.Should().Be("Delay");

        Action validateDecorrelated = () => JitterStrategies.DecorrelatedJitter(TimeSpan.Zero);

        _ = validateDecorrelated.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be a positive TimeSpan*")
            .And.ParamName.Should().Be("maxDelay");
    }

    #endregion

    #region Cross-Configuration Compatibility Tests

    [Fact]
    public void BackoffConfigurations_ShouldBeCompatibleWithAllJitterStrategies()
    {
        // Arrange
        var backoffConfigs = new object[]
        {
            new ExponentialBackoffConfiguration(),
            new LinearBackoffConfiguration(),
            new FixedDelayConfiguration(),
        };

        var jitterStrategies = new[]
        {
            JitterStrategies.FullJitter(),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1)),
            JitterStrategies.EqualJitter(),
            JitterStrategies.NoJitter(),
        };

        // Act & Assert
        foreach (var backoffConfig in backoffConfigs)
        {
            // Use explicit type casting to avoid dynamic call issues
            if (backoffConfig is ExponentialBackoffConfiguration expConfig)
            {
                var validateBackoff = () => expConfig.Validate();
                _ = validateBackoff.Should().NotThrow();
            }
            else if (backoffConfig is LinearBackoffConfiguration linConfig)
            {
                var validateBackoff = () => linConfig.Validate();
                _ = validateBackoff.Should().NotThrow();
            }
            else if (backoffConfig is FixedDelayConfiguration fixedConfig)
            {
                var validateBackoff = () => fixedConfig.Validate();
                _ = validateBackoff.Should().NotThrow();
            }
        }

        foreach (var jitterStrategy in jitterStrategies)
        {
            Action validateJitter = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
            _ = validateJitter.Should().NotThrow();
        }
    }

    [Fact]
    public void Configurations_WithExtremeValues_ShouldValidateWhenAppropriate()
    {
        // Arrange
        var extremeExponentialConfig = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromTicks(1), // Minimum
            Multiplier = double.MaxValue, // Maximum
            MaxDelay = TimeSpan.MaxValue, // Maximum
        };

        var extremeLinearConfig = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromTicks(1), // Minimum
            Increment = TimeSpan.MaxValue, // Maximum
            MaxDelay = TimeSpan.MaxValue, // Maximum
        };

        var extremeFixedConfig = new FixedDelayConfiguration
        {
            Delay = TimeSpan.MaxValue, // Maximum
        };

        var extremeDecorrelatedStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.MaxValue, double.MaxValue);

        // Act & Assert
        var validateExponential = () => extremeExponentialConfig.Validate();
        var validateLinear = () => extremeLinearConfig.Validate();
        var validateFixed = () => extremeFixedConfig.Validate();
        Action validateDecorrelated = () => extremeDecorrelatedStrategy(TimeSpan.FromSeconds(1), new Random());

        _ = validateExponential.Should().NotThrow();
        _ = validateLinear.Should().NotThrow();
        _ = validateFixed.Should().NotThrow();
        _ = validateDecorrelated.Should().NotThrow();
    }

    [Theory]
    [InlineData(1, 1, 1)] // All minimum
    [InlineData(1, 2, 10)] // Minimum base, normal multiplier, larger max
    [InlineData(10, 1.0, 10)] // Equal base and max, minimum multiplier
    [InlineData(5, 1.5, 100)] // Normal values
    [InlineData(60, 3.0, 3600)] // Larger values
    public void ExponentialBackoffConfiguration_WithVariousValidCombinations_ShouldValidate(
        int baseDelaySeconds, double multiplier, int maxDelaySeconds)
    {
        // Arrange
        var configuration = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(baseDelaySeconds),
            Multiplier = multiplier,
            MaxDelay = TimeSpan.FromSeconds(maxDelaySeconds),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(1, 0, 10)] // Zero increment
    [InlineData(1, 1, 10)] // Small increment
    [InlineData(10, 5, 100)] // Normal values
    [InlineData(60, 60, 3600)] // Larger values
    public void LinearBackoffConfiguration_WithVariousValidCombinations_ShouldValidate(
        int baseDelaySeconds, int incrementSeconds, int maxDelaySeconds)
    {
        // Arrange
        var configuration = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(baseDelaySeconds),
            Increment = TimeSpan.FromSeconds(incrementSeconds),
            MaxDelay = TimeSpan.FromSeconds(maxDelaySeconds),
        };

        // Act & Assert
        var validateAction = () => configuration.Validate();
        _ = validateAction.Should().NotThrow();
    }

    [Theory]
    [InlineData(1.0)] // Minimum multiplier
    [InlineData(1.5)] // Small fractional
    [InlineData(2.0)] // Small integer
    [InlineData(3.0)] // Default
    [InlineData(5.5)] // Fractional
    [InlineData(10.0)] // Larger integer
    [InlineData(100.25)] // Large fractional
    public void DecorrelatedJitterStrategy_WithVariousValidMultipliers_ShouldValidate(double multiplier)
    {
        // Arrange
        var jitterStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(5), multiplier);

        // Act & Assert
        Action validateAction = () => jitterStrategy(TimeSpan.FromSeconds(1), new Random());
        _ = validateAction.Should().NotThrow();
    }

    #endregion

    #region Configuration Edge Cases

    [Fact]
    public void Configurations_WithMinimumValidTimeSpans_ShouldValidate()
    {
        // Arrange
        var minimumTimeSpan = TimeSpan.FromTicks(1);

        var exponentialConfig = new ExponentialBackoffConfiguration
        {
            BaseDelay = minimumTimeSpan,
            MaxDelay = minimumTimeSpan,
        };

        var linearConfig = new LinearBackoffConfiguration
        {
            BaseDelay = minimumTimeSpan,
            Increment = minimumTimeSpan,
            MaxDelay = minimumTimeSpan,
        };

        var fixedConfig = new FixedDelayConfiguration
        {
            Delay = minimumTimeSpan,
        };

        var decorrelatedStrategy = JitterStrategies.DecorrelatedJitter(minimumTimeSpan);

        // Act & Assert
        var validateExponential = () => exponentialConfig.Validate();
        var validateLinear = () => linearConfig.Validate();
        var validateFixed = () => fixedConfig.Validate();
        Action validateDecorrelated = () => decorrelatedStrategy(minimumTimeSpan, new Random());

        _ = validateExponential.Should().NotThrow();
        _ = validateLinear.Should().NotThrow();
        _ = validateFixed.Should().NotThrow();
        _ = validateDecorrelated.Should().NotThrow();
    }

    [Fact]
    public void Configurations_WithZeroTimeSpans_ShouldThrowArgumentException()
    {
        // Arrange
        var zeroTimeSpan = TimeSpan.Zero;

        var exponentialConfig = new ExponentialBackoffConfiguration
        {
            BaseDelay = zeroTimeSpan,
        };

        var linearConfig = new LinearBackoffConfiguration
        {
            BaseDelay = zeroTimeSpan,
        };

        var fixedConfig = new FixedDelayConfiguration
        {
            Delay = zeroTimeSpan,
        };

        // Act & Assert
        var validateExponential = () => exponentialConfig.Validate();
        var validateLinear = () => linearConfig.Validate();
        var validateFixed = () => fixedConfig.Validate();
        Action validateDecorrelated = () => JitterStrategies.DecorrelatedJitter(zeroTimeSpan);

        _ = validateExponential.Should().Throw<ArgumentException>();
        _ = validateLinear.Should().Throw<ArgumentException>();
        _ = validateFixed.Should().Throw<ArgumentException>();
        _ = validateDecorrelated.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Configurations_WithNegativeTimeSpans_ShouldThrowArgumentException()
    {
        // Arrange
        var negativeTimeSpan = TimeSpan.FromSeconds(-1);

        var exponentialConfig = new ExponentialBackoffConfiguration
        {
            BaseDelay = negativeTimeSpan,
        };

        var linearConfig = new LinearBackoffConfiguration
        {
            BaseDelay = negativeTimeSpan,
        };

        var fixedConfig = new FixedDelayConfiguration
        {
            Delay = negativeTimeSpan,
        };

        // Act & Assert
        var validateExponential = () => exponentialConfig.Validate();
        var validateLinear = () => linearConfig.Validate();
        var validateFixed = () => fixedConfig.Validate();
        Action validateDecorrelated = () => JitterStrategies.DecorrelatedJitter(negativeTimeSpan);

        _ = validateExponential.Should().Throw<ArgumentException>();
        _ = validateLinear.Should().Throw<ArgumentException>();
        _ = validateFixed.Should().Throw<ArgumentException>();
        _ = validateDecorrelated.Should().Throw<ArgumentException>();
    }

    #endregion

    #region Configuration Inheritance and Default Behavior Tests

    [Fact]
    public void AllConfigurations_ShouldHaveReasonableDefaultValues()
    {
        // Arrange & Act
        var exponentialConfig = new ExponentialBackoffConfiguration();
        var linearConfig = new LinearBackoffConfiguration();
        var fixedConfig = new FixedDelayConfiguration();

        // Assert
        // Exponential defaults
        _ = exponentialConfig.BaseDelay.Should().Be(TimeSpan.FromSeconds(1));
        _ = exponentialConfig.Multiplier.Should().Be(2.0);
        _ = exponentialConfig.MaxDelay.Should().Be(TimeSpan.FromMinutes(1));

        // Linear defaults
        _ = linearConfig.BaseDelay.Should().Be(TimeSpan.FromSeconds(1));
        _ = linearConfig.Increment.Should().Be(TimeSpan.FromSeconds(1));
        _ = linearConfig.MaxDelay.Should().Be(TimeSpan.FromMinutes(1));

        // Fixed defaults
        _ = fixedConfig.Delay.Should().Be(TimeSpan.FromSeconds(1));

        // Decorrelated defaults - test with default parameters
        var decorrelatedStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1));
        _ = decorrelatedStrategy.Should().NotBeNull();
    }

    [Fact]
    public void Configurations_ShouldMaintainImmutabilityAfterValidation()
    {
        // Arrange
        var exponentialConfig = new ExponentialBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(5),
            Multiplier = 3.0,
            MaxDelay = TimeSpan.FromMinutes(2),
        };

        var linearConfig = new LinearBackoffConfiguration
        {
            BaseDelay = TimeSpan.FromSeconds(3),
            Increment = TimeSpan.FromSeconds(2),
            MaxDelay = TimeSpan.FromMinutes(5),
        };

        // Act
        exponentialConfig.Validate();
        linearConfig.Validate();

        // Assert - Values should remain unchanged after validation
        _ = exponentialConfig.BaseDelay.Should().Be(TimeSpan.FromSeconds(5));
        _ = exponentialConfig.Multiplier.Should().Be(3.0);
        _ = exponentialConfig.MaxDelay.Should().Be(TimeSpan.FromMinutes(2));

        _ = linearConfig.BaseDelay.Should().Be(TimeSpan.FromSeconds(3));
        _ = linearConfig.Increment.Should().Be(TimeSpan.FromSeconds(2));
        _ = linearConfig.MaxDelay.Should().Be(TimeSpan.FromMinutes(5));
    }

    #endregion
}
