---
title: "Kafka Connector"
description: "Consume from and produce to Apache Kafka with consumer groups, exactly-once semantics, and Schema Registry."
order: 15
---

# Kafka Connector

The `NPipeline.Connectors.Kafka` package provides source and sink nodes for [Apache Kafka](https://kafka.apache.org/). Supports consumer groups, exactly-once transactional semantics, multiple serialization formats (JSON, Avro, Protobuf) with Schema Registry integration, configurable acknowledgment strategies, and parallel processing.

## Installation

```bash
dotnet add package NPipeline.Connectors.Kafka
```

**Dependencies:** [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka) 2.x, [Confluent.SchemaRegistry](https://www.nuget.org/packages/Confluent.SchemaRegistry) 2.x (optional: Avro and Protobuf serializers)

## Source Node - `KafkaSourceNode<T>`

### Constructors

```csharp
public KafkaSourceNode(KafkaConfiguration configuration)

public KafkaSourceNode(
    KafkaConfiguration configuration,
    IKafkaMetrics metrics)

// Bring your own consumer
public KafkaSourceNode(
    IConsumer<string, T> consumer,
    KafkaConfiguration configuration,
    IKafkaMetrics metrics)
```

### Example

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SourceTopic = "orders",
    ConsumerGroupId = "order-processor",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    SerializationFormat = SerializationFormat.Json
};

var source = new KafkaSourceNode<Order>(config);
```

## Sink Node - `KafkaSinkNode<T>`

### Constructors

```csharp
public KafkaSinkNode(KafkaConfiguration configuration)

public KafkaSinkNode(
    KafkaConfiguration configuration,
    IKafkaMetrics metrics,
    IPartitionKeyProvider<T>? partitionKeyProvider = null)
```

### Example

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SinkTopic = "processed-orders",
    EnableIdempotence = true,
    Acks = Acks.All,
    SerializationFormat = SerializationFormat.Json
};

var sink = new KafkaSinkNode<ProcessedOrder>(config);
```

## Configuration

### Connection & Security

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BootstrapServers` | `string` | - | Broker addresses (comma-separated) |
| `ClientId` | `string?` | `null` | Client identifier |
| `SecurityProtocol` | `SecurityProtocol` | `Plaintext` | `Plaintext`, `Ssl`, `SaslPlaintext`, `SaslSsl` |
| `SaslMechanism` | `SaslMechanism` | `Plain` | `Plain`, `ScramSha256`, `ScramSha512`, `OAuthBearer` |
| `SaslUsername` | `string?` | `null` | SASL username |
| `SaslPassword` | `string?` | `null` | SASL password |

### Consumer (Source)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `SourceTopic` | `string` | - | Topic to consume from |
| `ConsumerGroupId` | `string` | - | Consumer group ID |
| `GroupInstanceId` | `string?` | `null` | Static group membership ID |
| `AutoOffsetReset` | `AutoOffsetReset` | `Latest` | `Earliest`, `Latest`, or `Error` |
| `EnableAutoCommit` | `bool` | - | Enable auto-commit |
| `MaxPollRecords` | `int` | `500` | Max records per poll |
| `PollTimeoutMs` | `int` | `100` | Poll timeout (ms) |
| `FetchMinBytes` | `int` | `1` | Min bytes to fetch |
| `FetchMaxBytes` | `int` | `52428800` | Max bytes to fetch |
| `Resilience` | `Resilience` | `KafkaConnectorResilience.Default` | How a failed consume is retried (see [Resilience](#resilience)) |

### Producer (Sink)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `SinkTopic` | `string` | - | Topic to produce to |
| `EnableIdempotence` | `bool` | `true` | Idempotent producer |
| `Acks` | `Acks` | `All` | `None`, `Leader`, or `All` |
| `BatchSize` | `int` | `16384` | Producer batch size (bytes) |
| `LingerMs` | `int` | `5` | Time to wait before sending a batch |
| `CompressionType` | `CompressionType` | `None` | `None`, `Gzip`, `Snappy`, `Lz4`, `Zstd` |
| `MessageMaxBytes` | `int` | `1000000` | Max message size |
| `DeliveryTimeoutMs` | `int` | `300000` | How long librdkafka retries a produce before failing it (`delivery.timeout.ms`); `0` is unlimited |
| `RetryBackoffMs` | `int` | `100` | First delay between librdkafka's produce retries (`retry.backoff.ms`) |
| `RetryBackoffMaxMs` | `int` | `1000` | Longest delay between librdkafka's produce retries (`retry.backoff.max.ms`) |
| `MetadataTimeoutMs` | `int` | `10000` | How long the sink waits for topic metadata when it starts |
| `ContinueOnError` | `bool` | `false` | Skip a message whose produce fails instead of failing the node |

### Serialization

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `SerializationFormat` | `SerializationFormat` | `Json` | `Json`, `Avro`, or `Protobuf` |
| `SchemaRegistry` | `SchemaRegistryConfiguration?` | `null` | Schema Registry settings (required for Avro/Protobuf) |

### Delivery Semantics

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `DeliverySemantic` | `DeliverySemantic` | `AtLeastOnce` | `AtLeastOnce` or `ExactlyOnce` |
| `AcknowledgmentStrategy` | `AcknowledgmentStrategy` | `AutoOnSinkSuccess` | When to acknowledge messages |
| `EnableTransactions` | `bool` | - | Enable transactional producer |
| `TransactionalId` | `string?` | `null` | Transactional ID (required for exactly-once) |

### Schema Registry

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SerializationFormat = SerializationFormat.Avro,
    SchemaRegistry = new SchemaRegistryConfiguration
    {
        Url = "http://localhost:8081",
        AutoRegisterSchemas = true,
        SchemaCacheCapacity = 1000
    }
};
```

Schemas are registered and looked up under a subject derived from the topic being written or read. Under the
default subject name strategy (`Topic`) the value subject is `<topic>-value`, so sinks writing different types to
different topics each get their own subject. `SubjectNameStrategy` (`Topic`, `Record`, or `TopicRecord`) and
`AutoRegisterSchemas` apply to the Avro and Protobuf serializers.

A custom `ISerializerProvider` can override `Serialize<T>(T, SerializationContext)` and
`Deserialize<T>(byte[], SerializationContext)` to receive the topic and component (key or value). The connector calls
these overloads; their default implementations forward to `Serialize<T>(T)` and `Deserialize<T>(byte[])`.

### Exactly-Once Semantics

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    DeliverySemantic = DeliverySemantic.ExactlyOnce,
    EnableTransactions = true,
    TransactionalId = "order-processor-1",
    EnableIdempotence = true,
    Acks = Acks.All,
    IsolationLevel = IsolationLevel.ReadCommitted
};
```

## Serialization Formats

| Format | Dependency | Schema | Best For |
|--------|-----------|--------|----------|
| `Json` (default) | - | None | Simple messages, debugging |
| `Avro` | `Confluent.SchemaRegistry.Serdes.Avro` | Schema Registry | Schema evolution, compact encoding |
| `Protobuf` | `Confluent.SchemaRegistry.Serdes.Protobuf` | Schema Registry | Cross-language, compact encoding |

### Schema Registry

```csharp
var config = new KafkaConfiguration
{
    SerializationFormat = SerializationFormat.Avro,
    SchemaRegistry = new SchemaRegistryConfiguration
    {
        Url = "http://localhost:8081",
        AutoRegisterSchemas = true,
        SchemaCacheCapacity = 1000
    }
};
```

## Delivery Semantics

| Semantic | Description | Configuration |
|----------|-------------|--------------|
| `AtLeastOnce` (default) | No data loss, possible duplicates | Default - `AcknowledgeAsync()` commits offset |
| `ExactlyOnce` | No data loss, no duplicates | Requires transactional producer |

### At-Least-Once

```csharp
// Default: offset committed on AcknowledgeAsync()
await message.AcknowledgeAsync(ct);
```

### Exactly-Once (Transactional)

```csharp
var config = new KafkaConfiguration
{
    DeliverySemantic = DeliverySemantic.ExactlyOnce,
    EnableTransactions = true,
    TransactionalId = "order-processor-1",
    EnableIdempotence = true,
    Acks = Acks.All,
    IsolationLevel = IsolationLevel.ReadCommitted
};
```

With exactly-once, `AcknowledgeAsync()` is a no-op - offsets are committed as part of the transaction by the sink.

## Resilience

### Consume

The source runs each consume through [NResilience](https://github.com/nresilience/NResilience). The `Resilience`
property on `KafkaConfiguration` configures it. The default, `KafkaConnectorResilience.Default`, does the following:

- Makes up to four attempts per consume (three retries).
- Waits with exponential backoff and full jitter, from 100 milliseconds up to 30 seconds.
- Retries errors Kafka reports as retriable: a lost broker connection, a timeout, a leader or coordinator that moved,
  a rebalance, too few in-sync replicas, and an exceeded `max.poll.interval.ms`. Quota throttling takes the long
  backoff curve.
- Surfaces a fatal error (`Error.IsFatal`), a deserialization error, an authorization failure, and any other error
  on the first attempt. Retrying a consume moves past a message that failed to deserialize, so retrying it would
  drop the message silently.
- Has no attempt timeout and no overall deadline. `PollTimeoutMs` bounds each poll.

The count applies to one consume: an error that clears restarts it, and one that persists fails the stream once the
attempts are spent. Messages consumed before the failure are delivered first. Each failed consume, retried or not,
is recorded with `IKafkaMetrics.RecordConsumeError`.

To change a setting, derive a policy with a `with` expression:

```csharp
var config = new KafkaConfiguration
{
    // ...
    Resilience = KafkaConnectorResilience.Default with { Attempts = 6 },
};
```

To fail on the first consume error, use `Resilience.None`. `KafkaConnectorResilience.Classifier` and
`KafkaConnectorResilience.IsRetriable(Error)` expose the classification if you build your own policy.

Cancelling the pipeline ends the stream gracefully, even during a backoff. An `OperationCanceledException` the
pipeline did not request fails the stream.

### Produce

The sink produces each message once and does not retry. librdkafka retries every failed produce request,
waiting `RetryBackoffMs` (100 ms) before the first retry and doubling up to `RetryBackoffMaxMs` (1 s), until
`DeliveryTimeoutMs` (five minutes) has passed since the message was produced. The idempotent producer
(`EnableIdempotence`, on by default) removes the duplicates those retries would cause. A retry above librdkafka would
send a new record that the idempotent producer cannot recognize, so a message whose delivery timed out but still
reached the broker would be written twice. `Resilience` does not apply to the sink; tune the three settings above
instead. They default to librdkafka's own defaults.

A produce error therefore means librdkafka gave up or the error is not retriable. It fails the node, or skips the
message when `ContinueOnError` is set. This holds for batched production too: when `BatchSize` is greater than 1 the
sink produces the batch concurrently, records every failure with `RecordProduceError`, acknowledges every message
that was delivered, and then fails the node with the first failure unless `ContinueOnError` is set.

When the sink starts, it reads the topic's partition count. If the brokers can't be reached within
`MetadataTimeoutMs`, the sink fails with an `InvalidOperationException` that names the topic and the brokers.
Cancelling the pipeline stops that wait at once.

### Acknowledgable messages

A sink of an acknowledgable type, such as `KafkaSinkNode<KafkaMessage<Order>>` fed by a Kafka source, produces
each message's `Body` (the value serializer writes the body, not the wrapper, as the body's runtime type, so Avro
records and Protobuf messages serialize with their own schema), turns its `Metadata` into headers,
and acknowledges the message only after the broker has it. By default the record key is the body's `ToString()`;
pass an `IPartitionKeyProvider<T>` to choose another key. With transactions, the offsets of consumed
`KafkaMessage<T>` items are sent to the transaction.

With `EnableTransactions`, the sink initializes transactions when it starts, bounded by `TransactionInitTimeoutMs`.
Cancelling the pipeline stops that wait at once. Disposing the sink waits for an initialization still in progress
before it disposes the producer.

## Acknowledgment Strategies

| Strategy | Description |
|----------|-------------|
| `AutoOnSinkSuccess` (default) | Offset committed after successful sink processing |
| `Manual` | Call `message.AcknowledgeAsync()` explicitly |

## Message Metadata

`KafkaMessage<T>` exposes:

| Property | Type | Description |
|----------|------|-------------|
| `Body` | `T` | Deserialized message value |
| `Key` | `string?` | Message key |
| `Topic` | `string` | Source topic |
| `Partition` | `int` | Partition number |
| `Offset` | `long` | Message offset |
| `Timestamp` | `DateTimeOffset` | Message timestamp |
| `Headers` | `Headers` | Kafka headers |

## Partitioning

Implement `IPartitionKeyProvider<T>` for custom partition routing:

```csharp
public class OrderPartitionProvider : IPartitionKeyProvider<Order>
{
    public string GetPartitionKey(Order item) => item.CustomerId.ToString();
}
```

## Dead-Letter Handling

Failed messages can be routed to a dead-letter topic via NPipeline's dead-letter mechanism:

```csharp
var config = new KafkaConfiguration
{
    DeadLetterTopic = "orders-dlq",
    MaxDeliveryAttempts = 3
};
```

## Best Practices

1. **Use `Acks.All` + `EnableIdempotence`** for durability
2. **Set `ConsumerGroupId`** per logical consumer - enables parallel processing
3. **Use Avro/Protobuf** with Schema Registry for schema evolution
4. **Tune `MaxPollRecords`** to control batch sizes (default 500)
5. **Monitor via `IKafkaMetrics`** - tracks consume/produce rates, lag, and consume, produce, and commit errors
6. **Use `CompressionType.Lz4`** for high-throughput topics
7. **Set `LingerMs = 5–50`** to batch small messages for better throughput
8. **Use exactly-once semantics** only when needed - higher overhead

## Next Steps

- [RabbitMQ Connector](rabbitmq.md) - alternative message broker
- [Azure Service Bus Connector](azure-service-bus.md) - managed messaging
- [Error Handling](../error-handling/index.md) - resilience for message processing
