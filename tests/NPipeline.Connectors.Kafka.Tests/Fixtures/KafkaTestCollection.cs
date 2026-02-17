namespace NPipeline.Connectors.Kafka.Tests.Fixtures;

/// <summary>
///     xUnit collection definition for Kafka integration tests.
///     Tests in this collection share a single Kafka + Schema Registry container instance.
/// </summary>
[CollectionDefinition("Kafka")]
public class KafkaTestCollectionDefinition : ICollectionFixture<KafkaTestContainerFixture>
{
    // This class has no code; it is just a marker for xUnit collection fixture
    // The ICollectionFixture attribute ensures KafkaTestContainerFixture is created
    // once per test run and shared across all tests in this collection.
}
