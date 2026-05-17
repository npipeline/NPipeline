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
    IKafkaMetrics metrics,
    IRetryStrategy retryStrategy)

// Bring your own consumer
public KafkaSourceNode(
    IConsumer<string, T> consumer,
    KafkaConfiguration configuration,
    IKafkaMetrics metrics,
    IRetryStrategy retryStrategy)
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
    IRetryStrategy retryStrategy,
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
5. **Monitor via `IKafkaMetrics`** - tracks consume/produce rates, lag, errors
6. **Use `CompressionType.Lz4`** for high-throughput topics
7. **Set `LingerMs = 5–50`** to batch small messages for better throughput
8. **Use exactly-once semantics** only when needed - higher overhead

## Next Steps

- [RabbitMQ Connector](rabbitmq.md) - alternative message broker
- [Azure Service Bus Connector](azure-service-bus.md) - managed messaging
- [Error Handling](../error-handling/index.md) - resilience for message processing
