using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Integration;

/// <summary>
///     Integration fixture for Azure Service Bus tests.
///     Supports either an externally provided namespace connection string or a local emulator container.
/// </summary>
public sealed class ServiceBusIntegrationFixture : IAsyncLifetime
{
    private const int AmqpPort = 5672;
    private IContainer? _emulatorContainer;

    public string? ConnectionString { get; private set; }

    public bool IsAvailable => !string.IsNullOrWhiteSpace(ConnectionString);

    public string UnavailableReason { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        // Preferred path: use a real namespace or preconfigured emulator connection string.
        var externalConnectionString = Environment.GetEnvironmentVariable("NP_SERVICEBUS_CONNECTION_STRING");

        if (!string.IsNullOrWhiteSpace(externalConnectionString))
        {
            ConnectionString = externalConnectionString;
            return;
        }

        // Optional local emulator path (disabled by default to avoid forcing Docker dependency for all test runs).
        var enableTestcontainers = string.Equals(
            Environment.GetEnvironmentVariable("NP_SERVICEBUS_TESTCONTAINERS_ENABLED"),
            "true",
            StringComparison.OrdinalIgnoreCase);

        if (!enableTestcontainers)
        {
            UnavailableReason =
                "Service Bus integration environment not configured. " +
                "Set NP_SERVICEBUS_CONNECTION_STRING or enable NP_SERVICEBUS_TESTCONTAINERS_ENABLED=true.";

            return;
        }

        var image = Environment.GetEnvironmentVariable("NP_SERVICEBUS_EMULATOR_IMAGE")
                    ?? "mcr.microsoft.com/azure-service-bus/emulator:latest";

        _emulatorContainer = new ContainerBuilder(image)
            .WithPortBinding(AmqpPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(AmqpPort))
            .WithLabel("npipeline-test", "servicebus-integration")
            .Build();

        await _emulatorContainer.StartAsync();

        var mappedPort = _emulatorContainer.GetMappedPublicPort(AmqpPort);

        var keyName = Environment.GetEnvironmentVariable("NP_SERVICEBUS_SHARED_ACCESS_KEY_NAME")
                      ?? "RootManageSharedAccessKey";

        var key = Environment.GetEnvironmentVariable("NP_SERVICEBUS_SHARED_ACCESS_KEY")
                  ?? "SAS_KEY_VALUE";

        ConnectionString =
            $"Endpoint=sb://127.0.0.1:{mappedPort}/;SharedAccessKeyName={keyName};SharedAccessKey={key};UseDevelopmentEmulator=true;";
    }

    public async Task DisposeAsync()
    {
        if (_emulatorContainer != null)
        {
            await _emulatorContainer.StopAsync();
            await _emulatorContainer.DisposeAsync();
        }
    }

    public async Task EnsureQueueExistsAsync(string queueName, CancellationToken cancellationToken = default)
    {
        if (ConnectionString == null)
            throw new InvalidOperationException("Integration fixture is not available.");

        var admin = new ServiceBusAdministrationClient(ConnectionString);

        if (!await admin.QueueExistsAsync(queueName, cancellationToken).ConfigureAwait(false))
            await admin.CreateQueueAsync(queueName, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteQueueIfExistsAsync(string queueName, CancellationToken cancellationToken = default)
    {
        if (ConnectionString == null)
            return;

        var admin = new ServiceBusAdministrationClient(ConnectionString);

        if (await admin.QueueExistsAsync(queueName, cancellationToken).ConfigureAwait(false))
            await admin.DeleteQueueAsync(queueName, cancellationToken).ConfigureAwait(false);
    }

    public ServiceBusClient CreateClient()
    {
        if (ConnectionString == null)
            throw new InvalidOperationException("Integration fixture is not available.");

        return new ServiceBusClient(ConnectionString);
    }
}
