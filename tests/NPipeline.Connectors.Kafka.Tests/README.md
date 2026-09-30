# NPipeline Kafka Connector Tests

```bash
dotnet test tests/NPipeline.Connectors.Kafka.Tests/
```

The unit tests (options, message model, offset tracking, dead-letter sink, resilience classification) need nothing
external. The integration tests (`Integration/`) start Kafka and a Schema Registry in Docker with Testcontainers
(`Fixtures/KafkaTestContainerFixture.cs`), so Docker must be running; they cover producing and consuming, offset
commits, undeserializable messages, tombstones, failed writes, exactly-once transactions, and Avro and Protobuf through
the registry.

The messaging round-trip suite (`tests/NPipeline.Connectors.Messaging.RoundTrip.Tests`) runs the scenarios every
message-queue connector shares against Kafka as well.
