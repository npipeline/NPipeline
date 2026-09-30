using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Testcontainers.ServiceBus;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Integration;

/// <summary>One Service Bus emulator for the whole run; each test creates queues of its own.</summary>
public sealed class ServiceBusFixture : IAsyncLifetime, IAsyncDisposable
{
    /// <summary>The lock duration of every test queue: short, so lock renewal and redelivery are quick to observe.</summary>
    public static readonly TimeSpan LockDuration = TimeSpan.FromSeconds(5);

    private readonly ServiceBusContainer _container = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
        .WithAcceptLicenseAgreement(true)
        .WithLabel("npipeline-test", "servicebus-connector")
        .Build();

    private ServiceBusAdministrationClient? _administration;
    private bool _disposed;
    private ServiceBusClient? _client;

    /// <summary>A client shared by the tests, as an application would share one.</summary>
    public ServiceBusClient Client => _client ?? throw new InvalidOperationException("The emulator has not started.");

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        _administration = new ServiceBusAdministrationClient(_container.GetHttpConnectionString());
        _client = new ServiceBusClient(ConnectionString);
    }

    public async Task DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_client is not null)
            await _client.DisposeAsync();

        await _container.DisposeAsync();
    }

    ValueTask IAsyncDisposable.DisposeAsync() => new(DisposeAsync());

    /// <summary>Creates a queue with a name of its own.</summary>
    public async Task<string> CreateQueueAsync(bool sessions = false)
    {
        var queue = $"sb-{Guid.NewGuid():N}";

        _ = await _administration!.CreateQueueAsync(new CreateQueueOptions(queue)
        {
            LockDuration = LockDuration,
            MaxDeliveryCount = 10,
            RequiresSession = sessions,
        });

        return queue;
    }
}

[CollectionDefinition(Name)]
public sealed class ServiceBusEmulator : ICollectionFixture<ServiceBusFixture>
{
    public const string Name = "Service Bus emulator";
}
