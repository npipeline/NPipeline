---
title: "Message Queues: Shared Behaviour"
description: "Serialization, settlement, acknowledgement through any sink, undeserializable messages and failed writes, shared by the message-queue connectors."
order: 15
---

# Message Queues: Shared Behaviour

The [Kafka](kafka.md), [RabbitMQ](rabbitmq.md), [Azure Service Bus](azure-service-bus.md) and [AWS SQS](aws-sqs.md)
connectors are built on one messaging layer in `NPipeline.Connectors.Messaging`, so they serialize, settle and handle
errors the same way. This page describes that shared behaviour; each connector's page covers its connection, options and
what settling means for its broker.

## Creating nodes

Each connector has a factory and an options record per direction:

```csharp
var orders = KafkaConnector.Source<Order>("kafka:9092", "orders", groupId: "billing");
var invoices = RabbitMqConnector.Sink<Invoice>(connection, exchange: "", routingKey: "invoices");
```

The optional last argument adjusts the defaults with a `with` expression. Sources emit the connector's message type
(`KafkaMessage<T>`, `RabbitMqMessage<T>`, `ServiceBusMessage<T>`, `SqsMessage<T>`); sinks are typed on the body.

## Messages and settlement

Every received message is an `IAcknowledgableMessage<T>`: a `Body`, a `MessageId`, the broker's `Metadata`, and two
ways to settle it:

| Method | Means |
| --- | --- |
| `AcknowledgeAsync()` | The message was handled; the broker won't deliver it again |
| `RejectAsync(requeue: true)` | Not handled; deliver it again |
| `RejectAsync(requeue: false)` | Not handled and not to be retried: the broker's dead-letter queue if it has one, otherwise dropped |

A message is settled once: the first call wins, and later calls return its task. A message that is never settled comes
back: RabbitMQ and Service Bus redeliver it when the source's connection closes, SQS after its visibility timeout, and
Kafka reads it again after a restart.

A transform keeps the message around a new body with `WithBody`, so the message can still be settled downstream:

```csharp
IAcknowledgableMessage<Invoice> Bill(IAcknowledgableMessage<Order> message) => message.WithBody(Invoice.For(message.Body));
```

Settling the copy settles the original. A transform that drops a message (a filter) should acknowledge it, or it is
delivered again.

## Writing messages through any sink

`Acknowledging()` turns any sink into a sink of messages: the bodies go to the sink, and each message is acknowledged
once the sink has written it.

```csharp
builder.AddSink(SqlServerConnector.Sink<Order>(connectionString, "Orders").Acknowledging(), "orders-db");
builder.AddSink(KafkaConnector.Sink<Order>("kafka:9092", "orders-copy").Acknowledging(), "orders-copy");
```

When a message counts as written depends on the sink:

| Sink | Acknowledged |
| --- | --- |
| Message-queue sinks | Once the broker has the published message; a message from the same broker keeps its key, id or properties, as each connector's page says |
| SQL sinks | After each committed batch (with `SqlTransactionMode.WholeRun`, after the final commit) |
| HTTP sink | After each successful request, or once a failed request is skipped or dead-lettered |
| Any other sink | When it has written the whole stream |

Sinks that batch also flush on time: a partial batch is written `BatchLinger` after its first item (one second for SQL
and HTTP sinks, 10 ms for message-queue sinks). A trickle of messages is therefore written promptly, and a source
holding unacknowledged messages (RabbitMQ's prefetch, Service Bus's in-flight limit) is never waiting on a batch that
cannot fill.

A file sink writes its file at the end of the stream, so with an endless source its messages are never acknowledged.
Write file outputs from a bounded read.

## Serialization

Bodies are serialized by an `IMessageSerializer`, the `Serializer` option on every node. The default,
`JsonMessageSerializer.Default`, uses the JSON connector's options (`ConnectorJson.Default`): camelCase names,
case-insensitive reads, enums as names, and the `[Column]` and `[IgnoreColumn]` attributes. A message one connector
writes reads back in any other, and in the JSON connector.

For Native AOT and trimming, pass a source-generated context:

```csharp
[JsonSerializable(typeof(Order))]
public partial class OrdersContext : JsonSerializerContext;

var orders = KafkaConnector.Source<Order>("kafka:9092", "orders", "billing", o => o with { Serializer = new JsonMessageSerializer(OrdersContext.Default) });
```

`new JsonMessageSerializer(options)` takes your own `JsonSerializerOptions` (the column attributes are added). Kafka
also has Avro and Protobuf serializers backed by a schema registry.

## Messages that don't deserialize

A source's `RowErrorHandler` decides what happens to a message whose body is not a valid `T`, as it does for a file's
bad row:

| `RowErrorAction` | What happens |
| --- | --- |
| `Fail` (default, when the handler is `null`) | The read fails with a `RecordMappingException` naming the topic or queue, the message's position and the start of its body; the message stays unsettled, so it comes back |
| `Skip` | The message is settled so it isn't delivered again: rejected to the broker's dead-letter queue where there is one, otherwise deleted or passed |
| `DeadLetter` | The message goes to the pipeline's dead-letter sink as a `MessageFailure`, then is settled as for `Skip` |

A `MessageFailure` holds the source, the message id, the whole body and the broker's metadata, so the message can be
inspected and published again. The RabbitMQ and Kafka dead-letter sinks publish it with its original body.

```csharp
var orders = RabbitMqConnector.Source<Order>(connection, "orders", o => o with { RowErrorHandler = _ => RowErrorAction.DeadLetter });
builder.AddDeadLetterSink(new RabbitMqDeadLetterSink(connection, exchange: "orders-dlx"));
```

`RawExcerptLength` (256 by default) sets how much of the body the row error keeps.

## Failed writes

A message-queue sink writes each batch, then settles the messages it came from: every message the broker accepted is
acknowledged, and one it didn't is handled as `FailedMessages` says:

| `FailedMessageAction` | What happens |
| --- | --- |
| `Fail` (default) | The write fails; the failed message stays unsettled, so it comes back |
| `Requeue` | The source message is rejected with requeue, and the write goes on |
| `DeadLetter` | The body goes to the pipeline's dead-letter sink, the source message is acknowledged, and the write goes on |

Each connector retries a failed send first, through its client library or its own `Resilience` policy, as its page
describes.

## When a read ends

When the pipeline stops reading (a bounded read, or the run ending), a source stops receiving, puts back what it
received but never handed on, and keeps its connection until the messages it handed on are settled, so a sink still
writing can settle them. `SettleTimeout` (30 seconds) bounds the wait; disposing the source (as a pipeline does when
its run ends) closes it at once, and unsettled messages come back. Cancelling the pipeline ends the read with an
`OperationCanceledException`.

## Metrics

Sources and sinks report through `System.Diagnostics.Metrics` (`NPipeline.Connectors`): messages read and written
(`rows_read`, `rows_written`), undeserializable messages by action (`row_errors`), and settlements by outcome
(`messages_settled`: `acknowledged`, `requeued`, `rejected`, `dead_lettered`), tagged with the connector's name.

## Next Steps

- [Kafka](kafka.md), [RabbitMQ](rabbitmq.md), [Azure Service Bus](azure-service-bus.md), [AWS SQS](aws-sqs.md)
- [SQL Connectors: Shared Behaviour](sql-connectors.md): the sinks that acknowledge per committed batch
