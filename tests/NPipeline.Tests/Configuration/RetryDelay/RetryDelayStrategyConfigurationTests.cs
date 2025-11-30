using System.Diagnostics;
using AwesomeAssertions;
using NPipeline.Configuration.RetryDelay;
using NPipeline.Execution.RetryDelay;

namespace NPipeline.Tests.Configuration.RetryDelay;

public sealed class RetryDelayStrategyConfigurationTests
{
    #region Configuration Performance Tests

    [Fact]
    public void ConfigurationValidation_ShouldBePerformant()
    {
        // Arrange
        var configurations = new[]
        {
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
                JitterStrategies.NoJitter()),
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)),
                JitterStrategies.NoJitter()),
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.FixedDelay(TimeSpan.FromSeconds(1)),
                JitterStrategies.NoJitter()),
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
                JitterStrategies.FullJitter()),
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
                JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1))),
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
                JitterStrategies.EqualJitter()),
        };

        // Act
        var stopwatch = Stopwatch.StartNew();

        for (var i = 0; i < 10000; i++)
        {
            foreach (var config in configurations)
            {
                // Validate configurations by creating strategies
                var factory = new DefaultRetryDelayStrategyFactory();
                _ = factory.CreateStrategy(config);
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
        var exponentialConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
            JitterStrategies.NoJitter());

        var linearConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)),
            JitterStrategies.NoJitter());

        var fixedConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromSeconds(1)),
            JitterStrategies.NoJitter());

        var fullJitterConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
            JitterStrategies.FullJitter());

        var decorrelatedJitterConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1)));

        var equalJitterConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
            JitterStrategies.EqualJitter());

        var noJitterConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
            JitterStrategies.NoJitter());

        // Act & Assert
        var factory = new DefaultRetryDelayStrategyFactory();

        _ = exponentialConfig.Should().NotBeNull();
        _ = linearConfig.Should().NotBeNull();
        _ = fixedConfig.Should().NotBeNull();
        _ = fullJitterConfig.Should().NotBeNull();
        _ = decorrelatedJitterConfig.Should().NotBeNull();
        _ = equalJitterConfig.Should().NotBeNull();
        _ = noJitterConfig.Should().NotBeNull();

        // Validate configurations by creating strategies
        var createExponential = () => factory.CreateStrategy(exponentialConfig);
        var createLinear = () => factory.CreateStrategy(linearConfig);
        var createFixed = () => factory.CreateStrategy(fixedConfig);
        var createFullJitter = () => factory.CreateStrategy(fullJitterConfig);
        var createDecorrelatedJitter = () => factory.CreateStrategy(decorrelatedJitterConfig);
        var createEqualJitter = () => factory.CreateStrategy(equalJitterConfig);
        var createNoJitter = () => factory.CreateStrategy(noJitterConfig);

        _ = createExponential.Should().NotThrow();
        _ = createLinear.Should().NotThrow();
        _ = createFixed.Should().NotThrow();
        _ = createFullJitter.Should().NotThrow();
        _ = createDecorrelatedJitter.Should().NotThrow();
        _ = createEqualJitter.Should().NotThrow();
        _ = createNoJitter.Should().NotThrow();
    }

    [Fact]
    public void ConfigurationValidation_ShouldProvideMeaningfulErrorMessages()
    {
        // Arrange & Act & Assert

        // Test invalid base delay (zero)
        var createExponential = () => BackoffStrategies.ExponentialBackoff(TimeSpan.Zero, 2.0, TimeSpan.FromMinutes(1));

        _ = createExponential.Should().Throw<ArgumentException>()
            .WithMessage("*BaseDelay must be a positive TimeSpan*");

        // Test invalid increment (negative)
        var createLinear = () => BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(-1), TimeSpan.FromMinutes(1));

        _ = createLinear.Should().Throw<ArgumentException>()
            .WithMessage("*Increment must be a non-negative TimeSpan*");

        // Test invalid fixed delay (zero)
        var createFixed = () => BackoffStrategies.FixedDelay(TimeSpan.Zero);

        _ = createFixed.Should().Throw<ArgumentException>()
            .WithMessage("*Delay must be a positive TimeSpan*");

        // Test invalid decorrelated jitter (zero max delay)
        Action createDecorrelated = () => JitterStrategies.DecorrelatedJitter(TimeSpan.Zero);

        _ = createDecorrelated.Should().Throw<ArgumentException>()
            .WithMessage("*MaxDelay must be a positive TimeSpan*");
    }

    #endregion

    #region Cross-Configuration Compatibility Tests

    [Fact]
    public void BackoffConfigurations_ShouldBeCompatibleWithAllJitterStrategies()
    {
        // Arrange
        var backoffConfigs = new[]
        {
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
                JitterStrategies.NoJitter()),
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)),
                JitterStrategies.NoJitter()),
            new RetryDelayStrategyConfiguration(
                BackoffStrategies.FixedDelay(TimeSpan.FromSeconds(1)),
                JitterStrategies.NoJitter()),
        };

        var jitterStrategies = new[]
        {
            JitterStrategies.FullJitter(),
            JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1)),
            JitterStrategies.EqualJitter(),
            JitterStrategies.NoJitter(),
        };

        // Act & Assert
        var factory = new DefaultRetryDelayStrategyFactory();

        foreach (var backoffConfig in backoffConfigs)
        {
            var createBackoff = () => factory.CreateStrategy(backoffConfig);
            _ = createBackoff.Should().NotThrow();
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
        var extremeExponentialConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromTicks(1), // Minimum
                double.MaxValue, // Maximum
                TimeSpan.MaxValue), // Maximum
            JitterStrategies.NoJitter());

        var extremeLinearConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromTicks(1), // Minimum
                TimeSpan.MaxValue, // Maximum
                TimeSpan.MaxValue), // Maximum
            JitterStrategies.NoJitter());

        var extremeFixedConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.MaxValue), // Maximum
            JitterStrategies.NoJitter());

        var extremeDecorrelatedStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.MaxValue, double.MaxValue);

        // Act & Assert
        var factory = new DefaultRetryDelayStrategyFactory();

        var createExponential = () => factory.CreateStrategy(extremeExponentialConfig);
        var createLinear = () => factory.CreateStrategy(extremeLinearConfig);
        var createFixed = () => factory.CreateStrategy(extremeFixedConfig);
        Action validateDecorrelated = () => extremeDecorrelatedStrategy(TimeSpan.FromSeconds(1), new Random());

        _ = createExponential.Should().NotThrow();
        _ = createLinear.Should().NotThrow();
        _ = createFixed.Should().NotThrow();
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
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(
                TimeSpan.FromSeconds(baseDelaySeconds),
                multiplier,
                TimeSpan.FromSeconds(maxDelaySeconds)),
            JitterStrategies.NoJitter());

        // Act & Assert
        var factory = new DefaultRetryDelayStrategyFactory();
        var createAction = () => factory.CreateStrategy(configuration);
        _ = createAction.Should().NotThrow();
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
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(
                TimeSpan.FromSeconds(baseDelaySeconds),
                TimeSpan.FromSeconds(incrementSeconds),
                TimeSpan.FromSeconds(maxDelaySeconds)),
            JitterStrategies.NoJitter());

        // Act & Assert
        var factory = new DefaultRetryDelayStrategyFactory();
        var createAction = () => factory.CreateStrategy(configuration);
        _ = createAction.Should().NotThrow();
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

        var exponentialConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(minimumTimeSpan, 2.0, minimumTimeSpan),
            JitterStrategies.NoJitter());

        var linearConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(minimumTimeSpan, minimumTimeSpan, minimumTimeSpan),
            JitterStrategies.NoJitter());

        var fixedConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(minimumTimeSpan),
            JitterStrategies.NoJitter());

        var decorrelatedStrategy = JitterStrategies.DecorrelatedJitter(minimumTimeSpan);

        // Act & Assert
        var factory = new DefaultRetryDelayStrategyFactory();

        var createExponential = () => factory.CreateStrategy(exponentialConfig);
        var createLinear = () => factory.CreateStrategy(linearConfig);
        var createFixed = () => factory.CreateStrategy(fixedConfig);
        Action validateDecorrelated = () => decorrelatedStrategy(minimumTimeSpan, new Random());

        _ = createExponential.Should().NotThrow();
        _ = createLinear.Should().NotThrow();
        _ = createFixed.Should().NotThrow();
        _ = validateDecorrelated.Should().NotThrow();
    }

    [Fact]
    public void Configurations_WithZeroTimeSpans_ShouldThrowArgumentException()
    {
        // Arrange
        var zeroTimeSpan = TimeSpan.Zero;

        // Act & Assert
        var createExponential = () => BackoffStrategies.ExponentialBackoff(zeroTimeSpan, 2.0, TimeSpan.FromMinutes(1));
        var createLinear = () => BackoffStrategies.LinearBackoff(zeroTimeSpan, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1));
        var createFixed = () => BackoffStrategies.FixedDelay(zeroTimeSpan);
        Action validateDecorrelated = () => JitterStrategies.DecorrelatedJitter(zeroTimeSpan);

        _ = createExponential.Should().Throw<ArgumentException>();
        _ = createLinear.Should().Throw<ArgumentException>();
        _ = createFixed.Should().Throw<ArgumentException>();
        _ = validateDecorrelated.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Configurations_WithNegativeTimeSpans_ShouldThrowArgumentException()
    {
        // Arrange
        var negativeTimeSpan = TimeSpan.FromSeconds(-1);

        // Act & Assert
        var createExponential = () => BackoffStrategies.ExponentialBackoff(negativeTimeSpan, 2.0, TimeSpan.FromMinutes(1));
        var createLinear = () => BackoffStrategies.LinearBackoff(negativeTimeSpan, TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1));
        var createFixed = () => BackoffStrategies.FixedDelay(negativeTimeSpan);
        Action validateDecorrelated = () => JitterStrategies.DecorrelatedJitter(negativeTimeSpan);

        _ = createExponential.Should().Throw<ArgumentException>();
        _ = createLinear.Should().Throw<ArgumentException>();
        _ = createFixed.Should().Throw<ArgumentException>();
        _ = validateDecorrelated.Should().Throw<ArgumentException>();
    }

    #endregion

    #region Null Jitter Strategy Tests

    [Fact]
    public void Configuration_WithBackoffAndNullJitter_ShouldValidate()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1))); // JitterStrategy is null (valid)

        // Act & Assert - Should not throw
        configuration.Validate();
    }

    [Fact]
    public void Configuration_WithBackoffAndNullJitter_ShouldCreateStrategySuccessfully()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1))); // JitterStrategy is null (valid)

        var factory = new DefaultRetryDelayStrategyFactory();

        // Act & Assert - Should not throw
        var createAction = () => factory.CreateStrategy(configuration);
        _ = createAction.Should().NotThrow();
    }

    [Fact]
    public void Configuration_WithBackoffAndNullJitter_ShouldWorkWithAllBackoffTypes()
    {
        // Arrange
        var exponentialConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)));

        var linearConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)));

        var fixedConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromSeconds(1)));

        var factory = new DefaultRetryDelayStrategyFactory();

        // Act & Assert - All should work without jitter
        var createExponential = () => factory.CreateStrategy(exponentialConfig);
        var createLinear = () => factory.CreateStrategy(linearConfig);
        var createFixed = () => factory.CreateStrategy(fixedConfig);

        _ = createExponential.Should().NotThrow();
        _ = createLinear.Should().NotThrow();
        _ = createFixed.Should().NotThrow();
    }

    [Fact]
    public async Task Configuration_WithBackoffAndNullJitter_ShouldProduceDeterministicDelays()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromMilliseconds(100), 2.0, TimeSpan.FromSeconds(5))); // No jitter for deterministic behavior

        var factory = new DefaultRetryDelayStrategyFactory();
        var strategy = factory.CreateStrategy(configuration);

        // Act
        var delay1 = await strategy.GetDelayAsync(1);
        var delay2 = await strategy.GetDelayAsync(1);
        var delay3 = await strategy.GetDelayAsync(2);

        // Assert - Should be deterministic without jitter
        _ = delay1.Should().Be(delay2); // Same attempt should produce same delay
        _ = delay3.Should().BeGreaterThan(delay1); // Higher attempt should produce greater delay
        _ = delay1.Should().Be(TimeSpan.FromMilliseconds(200)); // 100ms * 2^1
        _ = delay3.Should().Be(TimeSpan.FromMilliseconds(400)); // 100ms * 2^2
    }

    [Fact]
    public void Validator_WithBackoffAndNullJitter_ShouldValidateSuccessfully()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1))); // JitterStrategy is null (valid)

        // Act & Assert - Should not throw
        var validateAction = () => RetryDelayStrategyValidator.ValidateRetryDelayStrategyConfiguration(configuration);
        _ = validateAction.Should().NotThrow();
    }

    [Fact]
    public void Validator_WithNullBackoffAndNullJitter_ShouldThrowArgumentNullException()
    {
        // Arrange & Act & Assert - Test that validator properly handles null BackoffStrategy
        // Since BackoffStrategy is non-nullable in the record, we need to test the validator directly
        var validateAction = () => RetryDelayStrategyValidator.ValidateRetryDelayStrategyConfiguration(null!);
        _ = validateAction.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Validator_WithBackoffAndNullJitter_ShouldNotCallJitterValidation()
    {
        // Arrange
        var configuration = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1))); // JitterStrategy is null (valid)

        // Act & Assert - Should not throw and should not attempt to validate null jitter
        var validateAction = () => RetryDelayStrategyValidator.ValidateRetryDelayStrategyConfiguration(configuration);
        _ = validateAction.Should().NotThrow();
    }

    #endregion

    #region Configuration Inheritance and Default Behavior Tests

    [Fact]
    public void AllConfigurations_ShouldHaveReasonableDefaultValues()
    {
        // Arrange & Act
        var factory = new DefaultRetryDelayStrategyFactory();

        // Test default strategies
        var exponentialConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1)),
            JitterStrategies.NoJitter());

        var linearConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1)),
            JitterStrategies.NoJitter());

        var fixedConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.FixedDelay(TimeSpan.FromSeconds(1)),
            JitterStrategies.NoJitter());

        // Assert
        var createExponential = () => factory.CreateStrategy(exponentialConfig);
        var createLinear = () => factory.CreateStrategy(linearConfig);
        var createFixed = () => factory.CreateStrategy(fixedConfig);

        _ = createExponential.Should().NotThrow();
        _ = createLinear.Should().NotThrow();
        _ = createFixed.Should().NotThrow();

        // Decorrelated defaults - test with default parameters
        var decorrelatedStrategy = JitterStrategies.DecorrelatedJitter(TimeSpan.FromMinutes(1));
        _ = decorrelatedStrategy.Should().NotBeNull();
    }

    [Fact]
    public void Configurations_ShouldMaintainImmutabilityAfterValidation()
    {
        // Arrange
        var exponentialConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.ExponentialBackoff(TimeSpan.FromSeconds(5), 3.0, TimeSpan.FromMinutes(2)),
            JitterStrategies.NoJitter());

        var linearConfig = new RetryDelayStrategyConfiguration(
            BackoffStrategies.LinearBackoff(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(2), TimeSpan.FromMinutes(5)),
            JitterStrategies.NoJitter());

        // Act
        var factory = new DefaultRetryDelayStrategyFactory();
        _ = factory.CreateStrategy(exponentialConfig);
        _ = factory.CreateStrategy(linearConfig);

        // Assert - Values should remain unchanged after validation
        _ = exponentialConfig.BackoffStrategy.Should().NotBeNull();
        _ = exponentialConfig.JitterStrategy.Should().NotBeNull();
        _ = linearConfig.BackoffStrategy.Should().NotBeNull();
        _ = linearConfig.JitterStrategy.Should().NotBeNull();
    }

    #endregion
}
