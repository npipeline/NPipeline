using AwesomeAssertions;
using NPipeline.Connectors.Azure.Configuration;

namespace NPipeline.Connectors.Azure.Tests.Configuration;

public class AzureRetryConfigurationTests
{
    [Fact]
    public void MaxRetryAttempts_DefaultShouldBe9()
    {
        // Arrange
        var config = new AzureRetryConfiguration();

        // Act & Assert
        config.MaxRetryAttempts.Should().Be(9);
    }

    [Fact]
    public void MaxRetryWaitTime_DefaultShouldBe30Seconds()
    {
        // Arrange
        var config = new AzureRetryConfiguration();

        // Act & Assert
        config.MaxRetryWaitTime.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void InitialRetryDelay_DefaultShouldBe100Milliseconds()
    {
        // Arrange
        var config = new AzureRetryConfiguration();

        // Act & Assert
        config.InitialRetryDelay.Should().Be(TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public void RetryBackoffFactor_DefaultShouldBe2()
    {
        // Arrange
        var config = new AzureRetryConfiguration();

        // Act & Assert
        config.RetryBackoffFactor.Should().Be(2.0);
    }

    [Fact]
    public void UseJitter_DefaultShouldBeTrue()
    {
        // Arrange
        var config = new AzureRetryConfiguration();

        // Act & Assert
        config.UseJitter.Should().BeTrue();
    }

    [Fact]
    public void MaxRetryAttempts_CanBeModified()
    {
        // Arrange
        var config = new AzureRetryConfiguration();

        // Act
        config.MaxRetryAttempts = 5;

        // Assert
        config.MaxRetryAttempts.Should().Be(5);
    }

    [Fact]
    public void MaxRetryWaitTime_CanBeModified()
    {
        // Arrange
        var config = new AzureRetryConfiguration();
        var newTime = TimeSpan.FromMinutes(2);

        // Act
        config.MaxRetryWaitTime = newTime;

        // Assert
        config.MaxRetryWaitTime.Should().Be(newTime);
    }

    [Fact]
    public void InitialRetryDelay_CanBeModified()
    {
        // Arrange
        var config = new AzureRetryConfiguration();
        var newDelay = TimeSpan.FromMilliseconds(500);

        // Act
        config.InitialRetryDelay = newDelay;

        // Assert
        config.InitialRetryDelay.Should().Be(newDelay);
    }

    [Fact]
    public void RetryBackoffFactor_CanBeModified()
    {
        // Arrange
        var config = new AzureRetryConfiguration();

        // Act
        config.RetryBackoffFactor = 1.5;

        // Assert
        config.RetryBackoffFactor.Should().Be(1.5);
    }

    [Fact]
    public void UseJitter_CanBeModified()
    {
        // Arrange
        var config = new AzureRetryConfiguration();

        // Act
        config.UseJitter = false;

        // Assert
        config.UseJitter.Should().BeFalse();
    }
}
