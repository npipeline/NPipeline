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
}
