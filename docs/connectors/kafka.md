---
title: "Kafka Connector"
description: "Consume from and produce to Apache Kafka with consumer groups, ordered offset commits, exactly-once transactions and Schema Registry."
order: 16
---

# Kafka Connector

The `NPipeline.Connectors.Kafka` package consumes Kafka topics as a member of a consumer group and produces to topics,
with at-least-once delivery by default and exactly-once transactions when asked. Serialization, settlement,
undeserializable messages and failed writes work as in every message-queue connector; see
[Message Queues: Shared Behaviour](message-queues.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.Kafka
```

**Dependencies:** [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka), and
[Confluent.SchemaRegistry](https://www.nuget.org/packages/Confluent.SchemaRegistry) for the Avro and Protobuf serializers.

## Consuming

```csharp
var orders = KafkaConnector.Source<Order>("kafka:9092", "orders", groupId: "billing",
    o => o with { AutoOffsetReset = AutoOffsetReset.Earliest });

builder.AddSource(orders, "orders");
```

Each message is a `KafkaMessage<T>` with its `Body`, `Key` (UTF-8 text), `Topic`, `Partition`, `Offset`, `Timestamp`,
`Headers` and `IsTombstone`. `MessageId` is `topic/partition/offset`, which is unique, and `Metadata` holds the same
values (`Topic`, `Partition`, `Offset`, `Timestamp`, `Key`) with each header as `Header.<name>`.

### Offsets

Acknowledging a message stores its offset, and the consumer commits stored offsets every `CommitInterval` (5 seconds)
and when it closes. Offsets commit in order: a partition's committed offset only moves past a message once every
earlier message of that partition is settled, so a message still being written, or one that failed, is never skipped
by a later acknowledgement. What is committed is the next offset to read, so a restart continues after the last
acknowledged message.

`RejectAsync(requeue: false)` moves past a message. `RejectAsync(requeue: true)` holds the partition's commits at it,
since Kafka cannot redeliver one message: a restart reads it, and what follows, again.

When the read ends, the consumer stays until the messages it handed on are settled (up to `SettleTimeout`), then
commits and leaves the group, so its partitions move to another member at once rather than after a session timeout.

### Source options

| Option | Default | Description |
| --- | --- | --- |
| `Topic`, `GroupId` | required | The topic, and the consumer group whose committed offsets a read starts from |
| `AutoOffsetReset` | `Latest` | Where a group with no committed offset starts: `Latest` (Kafka's default) or `Earliest` |
| `GroupInstanceId` | `null` | A static member id, so a restarted member keeps its partitions without a rebalance |
| `IsolationLevel` | librdkafka's, read-committed | Whether transactional messages are read only once committed |
| `CommitInterval` | 5 s | How often stored offsets are committed |
| `SkipTombstones` | `true` | Tombstones (null values) are passed over; with `false` they are handed on with a default body and `IsTombstone` set |
| `RowErrorHandler`, `RawExcerptLength` | fail, 256 | See [Messages that don't deserialize](message-queues.md#messages-that-dont-deserialize); `Skip` moves past the message |
| `PollTimeout` | 100 ms | How long a poll waits before polling again |
| `Resilience` | `KafkaConnectorResilience.Default` | How a failed poll is retried; see [Resilience](#resilience) |
| `SettleTimeout` | 30 s | How long the consumer waits for handed-on messages to be settled after the read ends |

## Producing

```csharp
var invoices = KafkaConnector.Sink<Invoice>("kafka:9092", "invoices", o => o with { KeySelector = invoice => invoice.CustomerId });
builder.AddSink(invoices.Acknowledging(), "invoices");
```

The key decides the partition, and so the order: messages with one key keep their order. With no `KeySelector`, a
message read from Kafka keeps its key, and any other message has none, so librdkafka spreads them over the partitions.

The sink produces each batch of `BatchSize` messages and waits for all their deliveries; written through
`Acknowledging()`, each source message is acknowledged once the broker has it.

### Sink options

| Option | Default | Description |
| --- | --- | --- |
| `Topic` | required | The topic |
| `KeySelector` | `null` | Chooses each message's key from its body |
| `CopyHeaders` | `false` | Whether a message read from Kafka keeps its headers |
| `Acks` | `All` | How many replicas must have a message before the broker acknowledges it |
| `EnableIdempotence` | `true` | The producer's own retries never duplicate a message |
| `Linger`, `Compression` | 5 ms, none | librdkafka's batching delay and codec; `CompressionType.Lz4` or `Zstd` suits busy topics |
| `DeliveryTimeout` | 2 min | How long librdkafka keeps retrying a message before it fails |
| `BatchSize`, `BatchLinger` | 1,000, 10 ms | Messages whose deliveries are awaited together, and the longest a batch waits to fill |
| `FailedMessages` | `Fail` | See [Failed writes](message-queues.md#failed-writes) |
| `TransactionalId`, `TransactionTimeout` | none, 30 s | Exactly-once; see below |

### Exactly-once

With a `TransactionalId`, each batch is one transaction that also commits the offsets of the Kafka messages it came
from, so reading, transforming and writing are exactly-once: a batch and its source offsets commit or abort together.

```csharp
var orders = KafkaConnector.Source<Order>("kafka:9092", "orders", "billing");
var invoices = KafkaConnector.Sink<Invoice>("kafka:9092", "invoices", o => o with { TransactionalId = "billing-1" });
```

Each producer instance needs its own id. A failed batch aborts its transaction and fails the write (so
`FailedMessages` must be `Fail`), and a restart reads the batch's messages again. Consumers of the output should use
read-committed isolation, librdkafka's default.

## Connections and security

Both options records share the client settings:

| Option | Description |
| --- | --- |
| `BootstrapServers` | The brokers, `host:port` separated by commas |
| `ClientId` | The client id the brokers see |
| `SecurityProtocol`, `SaslMechanism`, `SaslUsername`, `SaslPassword` | Security; SASL with `Plain` or SCRAM needs the user name and password |
| `ClientSettings` | Any other librdkafka setting by name, such as `ssl.ca.location`, applied last |

### Tuning with `ClientSettings`

The options cover what most pipelines change. For the rest, set the librdkafka property directly. These are the ones
earlier versions of the connector exposed as properties:

| librdkafka setting | Applies to | Default | Effect |
| --- | --- | --- | --- |
| `fetch.min.bytes`, `fetch.max.bytes` | Source | 1, 52428800 | The least and most data a fetch returns; raise the minimum to trade latency for throughput |
| `max.poll.interval.ms` | Source | 300000 | How long the consumer may go without polling before the group removes it |
| `session.timeout.ms` | Source | 45000 | How long the broker waits for a heartbeat before it moves the member's partitions |
| `batch.size` | Sink | 1000000 | The producer's batch size in bytes (this is separate from the sink's `BatchSize`, which counts messages) |
| `message.max.bytes` | Sink | 1000000 | The largest message the producer accepts |
| `retry.backoff.ms`, `retry.backoff.max.ms` | Sink | 100, 1000 | The first and longest delay between librdkafka's produce retries |

```csharp
var orders = KafkaConnector.Source<Order>("kafka:9092", "orders", "billing", o => o with
{
    ClientSettings = new Dictionary<string, string> { ["fetch.min.bytes"] = "65536", ["max.poll.interval.ms"] = "600000" },
});
```

## Serialization

JSON by default; see [Serialization](message-queues.md#serialization). Avro and Protobuf use a schema registry:

| Serializer | Format | Best for |
| --- | --- | --- |
| `JsonMessageSerializer` (default) | JSON, no schema | Simple messages, debugging |
| `AvroMessageSerializer` | Avro | Schema evolution, compact encoding |
| `ProtobufMessageSerializer` | Protobuf | Cross-language use, compact encoding |

```csharp
var registry = new SchemaRegistryConfiguration { Url = "http://registry:8081" };
var orders = KafkaConnector.Source<OrderRecord>("kafka:9092", "orders", "billing", o => o with { Serializer = new AvroMessageSerializer(registry) });
```

| `SchemaRegistryConfiguration` option | Default | Description |
| --- | --- | --- |
| `Url` | required | The registry's address |
| `BasicAuthUsername`, `BasicAuthPassword` | `null` | Basic authentication |
| `EnableSsl` | `false` | Use TLS to reach the registry |
| `RequestTimeoutMs` | 30000 | How long a registry call may take |
| `SchemaCacheCapacity` | 1000 | The most schemas cached locally |
| `AutoRegisterSchemas` | `true` | Register a schema the registry doesn't have yet |
| `SubjectNameStrategy` | `Topic` | `Topic`, `Record` or `TopicRecord` |

Schemas are registered and looked up under a subject derived from the topic. Under the default subject name strategy
the value subject is `<topic>-value`, so sinks writing different types to different topics each get their own subject.
`SubjectNameStrategy` and `AutoRegisterSchemas` apply to both serializers. To use another format or registry, implement
`IMessageSerializer`; it receives the topic as `MessageContext.Destination`, and `IsKey` tells a key from a body.

## Resilience

The source retries a failed poll with [NResilience](https://github.com/nresilience/NResilience).
`KafkaConnectorResilience.Default`:

- Makes up to four attempts per poll (three retries), with exponential backoff and full jitter from 100 ms up to 30 s.
- Retries errors Kafka reports as retriable: a lost broker connection, a timeout, a leader or coordinator that moved,
  a rebalance, too few in-sync replicas, an exceeded `max.poll.interval.ms`. Quota throttling takes the longer backoff.
- Doesn't retry a fatal error, an authorization failure, or any other error.

Derive a policy to change it (`KafkaConnectorResilience.Default with { Attempts = 6 }`), or turn retries off with
`Resilience.None`. `KafkaConnectorResilience.IsRetriable(Error)` exposes the classification.

The sink doesn't retry: librdkafka retries every failed produce until `DeliveryTimeout`, and the idempotent producer
removes the duplicates those retries would cause. A retry above librdkafka would be a new record the producer could not
recognize, so a message whose delivery timed out but still reached the broker would be written twice.

## Dead letters

`KafkaDeadLetterSink` is a pipeline dead-letter sink that produces failed items to a topic, with the error in
`x-dead-letter-*` headers. A `MessageFailure` (a message that didn't deserialize) is produced with its original value and
key, so it can be replayed:

```csharp
using var producer = new ProducerBuilder<byte[]?, byte[]>(new ProducerConfig { BootstrapServers = "kafka:9092" }).Build();
builder.AddDeadLetterSink(new KafkaDeadLetterSink(producer, "orders-dead-letters"));
```

## Best practices

1. **Keep `Acks.All` and `EnableIdempotence`** (the defaults) for durability.
2. **Give each logical consumer its own `GroupId`.** Members of one group share a topic's partitions, so run more
   members, up to the partition count, to read in parallel.
3. **Use Avro or Protobuf with a schema registry** when the schema will evolve.
4. **Use `CompressionType.Lz4` or `Zstd`** on busy topics, and keep `Linger` at 5 to 50 ms so small messages batch.
5. **Choose keys deliberately.** Messages with one key keep their order, and a hot key overloads one partition.
6. **Use exactly-once only where duplicates matter.** A transaction per batch costs more than at-least-once delivery.
7. **Watch the metrics** ([Metrics](message-queues.md#metrics)) for rows read and written, row errors and settlements.

## Next Steps

- [Message Queues: Shared Behaviour](message-queues.md)
- [RabbitMQ](rabbitmq.md), [Azure Service Bus](azure-service-bus.md), [AWS SQS](aws-sqs.md)
