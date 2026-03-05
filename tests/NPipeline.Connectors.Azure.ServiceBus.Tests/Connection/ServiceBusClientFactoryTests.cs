using Azure.Core;
using FakeItEasy;
using NPipeline.Connectors.Azure.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Connection;
using NPipeline.Connectors.Azure.ServiceBus.Exceptions;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Connection;

public class ServiceBusClientFactoryTests
{
    [Fact]
    public async Task Create_WithAzureAdCredentialAndNoCredential_UsesDefaultAzureCredential()
    {
        var config = new ServiceBusConfiguration
        {
            AuthenticationMode = AzureAuthenticationMode.AzureAdCredential,
            FullyQualifiedNamespace = "test.servicebus.windows.net",
            QueueName = "test-queue",
        };

        var client = ServiceBusClientFactory.Create(config);

        client.Should().NotBeNull();
        await client.DisposeAsync();
    }

    [Fact]
    public async Task Create_WithEndpointWithKeyAndValidFields_CreatesClient()
    {
        var config = new ServiceBusConfiguration
        {
            AuthenticationMode = AzureAuthenticationMode.EndpointWithKey,
            FullyQualifiedNamespace = "test.servicebus.windows.net",
            SharedAccessKeyName = "RootManageSharedAccessKey",
            SharedAccessKey = "abc123=",
            QueueName = "test-queue",
        };

        var client = ServiceBusClientFactory.Create(config);

        client.Should().NotBeNull();
        await client.DisposeAsync();
    }

    [Fact]
    public async Task Create_WithNamedConnectionEndpointAndNoCredential_UsesDefaultAzureCredential()
    {
        var azureOptions = new AzureConnectionOptions();

        azureOptions.AddOrUpdateEndpoint("primary", new AzureEndpointOptions
        {
            Endpoint = new Uri("https://test.servicebus.windows.net"),
            Credential = null,
        });

        var config = new ServiceBusConfiguration
        {
            NamedConnection = "primary",
            QueueName = "test-queue",
        };

        var client = ServiceBusClientFactory.Create(config, azureOptions);

        client.Should().NotBeNull();
        await client.DisposeAsync();
    }

    [Fact]
    public void Create_WithEndpointWithKeyMissingKey_ThrowsServiceBusConnectionException()
    {
        var config = new ServiceBusConfiguration
        {
            AuthenticationMode = AzureAuthenticationMode.EndpointWithKey,
            FullyQualifiedNamespace = "test.servicebus.windows.net",
            SharedAccessKeyName = "RootManageSharedAccessKey",
            QueueName = "test-queue",
        };

        Assert.Throws<ServiceBusConnectionException>(() => ServiceBusClientFactory.Create(config));
    }

    [Fact]
    public async Task Create_WithAzureAdAndExplicitCredential_CreatesClient()
    {
        var config = new ServiceBusConfiguration
        {
            AuthenticationMode = AzureAuthenticationMode.AzureAdCredential,
            FullyQualifiedNamespace = "test.servicebus.windows.net",
            Credential = A.Fake<TokenCredential>(),
            QueueName = "test-queue",
        };

        var client = ServiceBusClientFactory.Create(config);

        client.Should().NotBeNull();
        await client.DisposeAsync();
    }
}
