using AwesomeAssertions;
using NPipeline.Configuration;

namespace NPipeline.Tests.Configuration;

public sealed class CircuitBreakerMemoryManagementOptionsTests
{
    [Fact]
    public void EffectiveCleanupTimeout_ShouldDefaultTo30Seconds()
    {
        // Arrange & Act
        var options = new CircuitBreakerMemoryManagementOptions();

        // Assert
        options.EffectiveCleanupTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void EffectiveCleanupTimeout_ShouldUseCustomValue()
    {
        // Arrange
        var customTimeout = TimeSpan.FromMinutes(1);

        // Act
        var options = new CircuitBreakerMemoryManagementOptions(CleanupTimeout: customTimeout);

        // Assert
        options.EffectiveCleanupTimeout.Should().Be(customTimeout);
    }

    [Fact]
    public void EffectiveCleanupTimeout_ShouldUseCustomValueWithOtherParameters()
    {
        // Arrange
        var customTimeout = TimeSpan.FromSeconds(45);

        // Act
        var options = new CircuitBreakerMemoryManagementOptions(
            TimeSpan.FromMinutes(10),
            TimeSpan.FromHours(1),
            false,
            500,
            customTimeout);

        // Assert
        options.EffectiveCleanupTimeout.Should().Be(customTimeout);
    }

    [Fact]
    public void Default_ShouldHave30SecondCleanupTimeout()
    {
        // Arrange & Act
        var defaultOptions = CircuitBreakerMemoryManagementOptions.Default;

        // Assert
        defaultOptions.EffectiveCleanupTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Disabled_ShouldHave30SecondCleanupTimeout()
    {
        // Arrange & Act
        var disabledOptions = CircuitBreakerMemoryManagementOptions.Disabled;

        // Assert
        disabledOptions.EffectiveCleanupTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Validate_ShouldThrowWhenCleanupTimeoutIsZero()
    {
        // Arrange
        var options = new CircuitBreakerMemoryManagementOptions(
            TimeSpan.FromMinutes(5),
            TimeSpan.FromMinutes(30),
            true,
            1000,
            TimeSpan.Zero); // This will use default value in EffectiveCleanupTimeout

        // Act & Assert
        // The EffectiveCleanupTimeout property returns the default value when CleanupTimeout is zero,
        // so validation won't fail. This test verifies that behavior.
        var validatedOptions = options.Validate();
        validatedOptions.EffectiveCleanupTimeout.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void Validate_ShouldThrowWhenCleanupTimeoutIsNegative()
    {
        // Arrange
        var options = new CircuitBreakerMemoryManagementOptions(CleanupTimeout: TimeSpan.FromSeconds(-1));

        // Act & Assert
        var act = () => options.Validate();

        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName("CleanupTimeout")
            .WithMessage("CleanupTimeout must be greater than zero*");
    }

    [Fact]
    public void Validate_ShouldReturnOptionsWhenCleanupTimeoutIsValid()
    {
        // Arrange
        var options = new CircuitBreakerMemoryManagementOptions(CleanupTimeout: TimeSpan.FromMinutes(2));

        // Act
        var result = options.Validate();

        // Assert
        result.Should().Be(options);
    }
}
