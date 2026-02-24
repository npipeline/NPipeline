using AwesomeAssertions;
using Azure.Core;
using FakeItEasy;
using NPipeline.Connectors.Azure.Configuration;

namespace NPipeline.Connectors.Azure.Tests.Configuration;

public class AzureEndpointOptionsTests
{
    [Fact]
    public void Endpoint_DefaultShouldBeNull()
    {
        // Arrange
        var options = new AzureEndpointOptions();

        // Act & Assert
        options.Endpoint.Should().BeNull();
    }

    [Fact]
    public void Credential_DefaultShouldBeNull()
    {
        // Arrange
        var options = new AzureEndpointOptions();

        // Act & Assert
        options.Credential.Should().BeNull();
    }

    [Fact]
    public void Endpoint_CanBeSet()
    {
        // Arrange
        var options = new AzureEndpointOptions();
        var endpoint = new Uri("https://myaccount.blob.core.windows.net/");

        // Act
        options.Endpoint = endpoint;

        // Assert
        options.Endpoint.Should().Be(endpoint);
    }

    [Fact]
    public void Credential_CanBeSet()
    {
        // Arrange
        var options = new AzureEndpointOptions();
        var credential = A.Fake<TokenCredential>();

        // Act
        options.Credential = credential;

        // Assert
        options.Credential.Should().Be(credential);
    }

    [Fact]
    public void Endpoint_WithValidUri_ShouldRetainValue()
    {
        // Arrange
        var endpoint = new Uri("https://myaccount.documents.azure.com:443/");

        var options = new AzureEndpointOptions
        {
            Endpoint = endpoint,
        };

        // Act & Assert
        options.Endpoint.Should().Be(endpoint);
        options.Endpoint!.Scheme.Should().Be("https");
        options.Endpoint.Port.Should().Be(443);
    }

    [Fact]
    public void Options_CanBeCreatedWithBothProperties()
    {
        // Arrange
        var endpoint = new Uri("https://myaccount.servicebus.windows.net/");
        var credential = A.Fake<TokenCredential>();

        // Act
        var options = new AzureEndpointOptions
        {
            Endpoint = endpoint,
            Credential = credential,
        };

        // Assert
        options.Endpoint.Should().Be(endpoint);
        options.Credential.Should().Be(credential);
    }
}
