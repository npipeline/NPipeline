using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.Kafka;

namespace NPipeline.Connectors.Kafka.Tests.Fixtures;

/// <summary>
///     Test fixture that provides a Kafka container for integration tests.
///     Uses the Testcontainers.Kafka module for reliable Kafka startup.
/// </summary>
public class KafkaTestContainerFixture : IAsyncLifetime
{
    private readonly KafkaContainer _kafkaContainer;
    private readonly INetwork _network;
    private readonly IContainer _schemaRegistryContainer;
    private bool _isInitialized;
    private bool _schemaRegistryStarted;

    public KafkaTestContainerFixture()
    {
        _network = new NetworkBuilder()
            .WithName($"npipeline-kafka-{Guid.NewGuid():N}")
            .Build();

        // Use Testcontainers.Kafka module which handles all the KRaft complexity
        _kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.5.0")
            .WithLabel("npipeline-test", "kafka-integration")
            .WithNetwork(_network)
            .WithNetworkAliases("kafka")
            .WithListener("kafka:29092")
            .Build();

        _schemaRegistryContainer = new ContainerBuilder("confluentinc/cp-schema-registry:7.5.0")
            .WithLabel("npipeline-test", "schema-registry-integration")
            .WithNetwork(_network)
            .WithNetworkAliases("schema-registry")
            .WithPortBinding(8081, true)
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:29092")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(r => r.ForPath("/subjects").ForPort(8081)))
            .Build();
    }

    /// <summary>
    ///     Gets the Kafka bootstrap servers address for client connections.
    ///     Throws if accessed before initialization completes.
    /// </summary>
    public string BootstrapServers
    {
        get
        {
            if (!_isInitialized)
            {
                throw new InvalidOperationException(
                    "Cannot access BootstrapServers before fixture is initialized. Ensure tests are in the 'Kafka' collection.");
            }

            return _kafkaContainer.GetBootstrapAddress();
        }
    }

    /// <summary>
    ///     Gets the Schema Registry URL for Avro/Protobuf serialization.
    /// </summary>
    public string SchemaRegistryUrl
    {
        get
        {
            if (!_schemaRegistryStarted)
                throw new InvalidOperationException("Schema Registry is not started. Call EnsureSchemaRegistryAsync first.");

            var port = _schemaRegistryContainer.GetMappedPublicPort(8081);
            return $"http://localhost:{port}";
        }
    }

    public async Task InitializeAsync()
    {
        await _kafkaContainer.StartAsync();
        _isInitialized = true;
    }

    public async Task DisposeAsync()
    {
        _isInitialized = false;

        if (_schemaRegistryStarted)
        {
            await _schemaRegistryContainer.DisposeAsync();
            _schemaRegistryStarted = false;
        }

        await _kafkaContainer.DisposeAsync();
        await _network.DisposeAsync();
    }

    /// <summary>
    ///     Ensures Schema Registry is started.
    /// </summary>
    public async Task EnsureSchemaRegistryAsync()
    {
        if (_schemaRegistryStarted)
            return;

        if (!_isInitialized)
            throw new InvalidOperationException("Cannot start Schema Registry before Kafka is initialized.");

        await _schemaRegistryContainer.StartAsync();
        _schemaRegistryStarted = true;
    }
}
