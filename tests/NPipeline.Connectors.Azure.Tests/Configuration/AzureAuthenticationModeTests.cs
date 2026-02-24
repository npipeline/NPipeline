using AwesomeAssertions;
using NPipeline.Connectors.Azure.Configuration;

namespace NPipeline.Connectors.Azure.Tests.Configuration;

public class AzureAuthenticationModeTests
{
    [Fact]
    public void ConnectionString_ShouldHaveCorrectValue()
    {
        // Act
        var mode = AzureAuthenticationMode.ConnectionString;

        // Assert
        _ = mode.Should().Be(AzureAuthenticationMode.ConnectionString);
        ((int)mode).Should().Be(0);
    }

    [Fact]
    public void EndpointWithKey_ShouldHaveCorrectValue()
    {
        // Act
        var mode = AzureAuthenticationMode.EndpointWithKey;

        // Assert
        _ = mode.Should().Be(AzureAuthenticationMode.EndpointWithKey);
        ((int)mode).Should().Be(1);
    }

    [Fact]
    public void AzureAdCredential_ShouldHaveCorrectValue()
    {
        // Act
        var mode = AzureAuthenticationMode.AzureAdCredential;

        // Assert
        _ = mode.Should().Be(AzureAuthenticationMode.AzureAdCredential);
        ((int)mode).Should().Be(2);
    }

    [Fact]
    public void Default_ShouldBeConnectionString()
    {
        // Act
        var defaultMode = default(AzureAuthenticationMode);

        // Assert
        _ = defaultMode.Should().Be(AzureAuthenticationMode.ConnectionString);
    }

    [Fact]
    public void AllEnumValues_ShouldBeDefined()
    {
        // Arrange
        var expectedValues = new[]
        {
            AzureAuthenticationMode.ConnectionString,
            AzureAuthenticationMode.EndpointWithKey,
            AzureAuthenticationMode.AzureAdCredential,
        };

        // Act
        var actualValues = Enum.GetValues<AzureAuthenticationMode>();

        // Assert
        actualValues.Should().BeEquivalentTo(expectedValues);
    }
}
