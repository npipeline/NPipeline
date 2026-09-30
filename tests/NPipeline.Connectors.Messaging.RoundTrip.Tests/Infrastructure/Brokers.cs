using Testcontainers.Kafka;
using Testcontainers.LocalStack;
using Testcontainers.RabbitMq;
using Testcontainers.ServiceBus;

namespace NPipeline.Connectors.Messaging.RoundTrip.Tests.Infrastructure;

/// <summary>One container per broker for the whole run; each test uses destinations of its own.</summary>
public sealed class KafkaBrokerFixture : IAsyncLifetime
{
    private readonly KafkaContainer _container = new KafkaBuilder("confluentinc/cp-kafka:7.5.0")
        .WithLabel("npipeline-test", "messaging-roundtrip-kafka")
        .Build();

    public string BootstrapServers => _container.GetBootstrapAddress();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

public sealed class RabbitMqBrokerFixture : IAsyncLifetime
{
    // A user other than guest, which RabbitMQ restricts to localhost.
    public const string UserName = "roundtrip";
    public const string Password = "roundtrip";

    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4-management-alpine")
        .WithUsername(UserName)
        .WithPassword(Password)
        .WithLabel("npipeline-test", "messaging-roundtrip-rabbitmq")
        .Build();

    public string HostName => _container.Hostname;

    public int Port => _container.GetMappedPublicPort(5672);

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

public sealed class SqsBrokerFixture : IAsyncLifetime
{
    private readonly LocalStackContainer _container = new LocalStackBuilder("localstack/localstack:4.14.0")
        .WithEnvironment("SERVICES", "sqs")
        .WithLabel("npipeline-test", "messaging-roundtrip-sqs")
        .Build();

    public string ServiceUrl => _container.GetConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

public sealed class ServiceBusBrokerFixture : IAsyncLifetime
{
    private readonly ServiceBusContainer _container = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
        .WithAcceptLicenseAgreement(true)
        .WithLabel("npipeline-test", "messaging-roundtrip-servicebus")
        .Build();

    public string ConnectionString => _container.GetConnectionString();

    public string AdministrationConnectionString => _container.GetHttpConnectionString();

    public Task InitializeAsync() => _container.StartAsync();

    public Task DisposeAsync() => _container.DisposeAsync().AsTask();
}

[CollectionDefinition(Name)]
public sealed class KafkaBrokers : ICollectionFixture<KafkaBrokerFixture>
{
    public const string Name = "Kafka round trips";
}

[CollectionDefinition(Name)]
public sealed class RabbitMqBrokers : ICollectionFixture<RabbitMqBrokerFixture>
{
    public const string Name = "RabbitMQ round trips";
}

[CollectionDefinition(Name)]
public sealed class SqsBrokers : ICollectionFixture<SqsBrokerFixture>
{
    public const string Name = "SQS round trips";
}

[CollectionDefinition(Name)]
public sealed class ServiceBusBrokers : ICollectionFixture<ServiceBusBrokerFixture>
{
    public const string Name = "Service Bus round trips";
}
