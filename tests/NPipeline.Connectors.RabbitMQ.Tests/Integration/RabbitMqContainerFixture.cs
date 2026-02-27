using Testcontainers.RabbitMq;

namespace NPipeline.Connectors.RabbitMQ.Tests.Integration;

/// <summary>
///     Shared RabbitMQ container fixture for integration tests.
/// </summary>
public sealed class RabbitMqContainerFixture : IAsyncLifetime
{
    // Use a non-guest user so RabbitMQ doesn't restrict the connection to localhost-only.
    public const string TestUsername = "testuser";
    public const string TestPassword = "testpass";

    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4-management-alpine")
        .WithUsername(TestUsername)
        .WithPassword(TestPassword)
        .Build();

    public string HostName => _container.Hostname;
    public int Port => _container.GetMappedPublicPort(5672);

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }
}

[CollectionDefinition("RabbitMQ")]
public class RabbitMqCollectionFixture : ICollectionFixture<RabbitMqContainerFixture>;
