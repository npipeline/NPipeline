---
title: "Azure Service Bus Connector"
description: "Receive from and send to Azure Service Bus queues, topics and sessions, with lock renewal, dead-lettering and batched sends."
order: 18
---

# Azure Service Bus Connector

The `NPipeline.Connectors.Azure.ServiceBus` package receives from Service Bus queues, topic subscriptions and
session-enabled entities in peek-lock mode, and sends to queues and topics. Serialization, settlement, undeserializable
messages and failed writes work as in every message-queue connector; see
[Message Queues: Shared Behaviour](message-queues.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.Azure.ServiceBus
```

**Dependencies:** [Azure.Messaging.ServiceBus](https://www.nuget.org/packages/Azure.Messaging.ServiceBus) 7.x,
[Azure.Identity](https://www.nuget.org/packages/Azure.Identity)

## Connection

Share one `ServiceBusClient` between nodes, preferably with Microsoft Entra ID:

```csharp
await using var client = new ServiceBusClient("mynamespace.servicebus.windows.net", new DefaultAzureCredential());
```

Instead of `Client`, a node's options can take a `ConnectionString`, or a `FullyQualifiedNamespace` with a `Credential`
(`DefaultAzureCredential` when `null`); the node then creates its own client and disposes it. The SDK retries each
operation itself, as `Retry` (a `ServiceBusRetryOptions`, for a client the node creates) says; the connector adds no
retries of its own, and a failure the SDK gives up on reaches the pipeline.

## Receiving

```csharp
var orders = ServiceBusConnector.Source<Order>(client, "orders");
var billing = ServiceBusConnector.SubscriptionSource<Order>(client, "order-events", "billing");
var deadLetters = ServiceBusConnector.Source<Order>(client, "orders", o => o with { SubQueue = SubQueue.DeadLetter });
```

The source receives in batches while fewer than `MaxInFlight` (100) messages are unsettled, and renews each held
message's lock while it waits, for up to `MaxLockRenewal` (5 minutes). A sink can therefore hold and batch up to
`MaxInFlight` messages before it settles any, and a slow transform doesn't lose its locks. When the read ends, messages
received but not handed on are abandoned at once, and the source keeps its receivers until the messages handed on are
settled (up to `SettleTimeout`), abandoning any left.

Each message is a `ServiceBusMessage<T>` with its `Body`, `MessageId`, `SessionId`, `CorrelationId`, `Subject`,
`ContentType`, `DeliveryCount`, `EnqueuedTime`, `ApplicationProperties`, and the SDK's `Received` message. Besides
`AcknowledgeAsync` (complete) and `RejectAsync` (abandon, or dead-letter without requeue), it can be settled with
`DeadLetterAsync(reason, description)` or `DeferAsync()`.

### Sessions

For a session-enabled queue or subscription, `SessionSource` receives up to `MaxConcurrentSessions` (8) sessions at
once, each in order, moving to the next session once one has had no messages for `SessionIdleTimeout` (5 seconds):

```csharp
var orders = ServiceBusConnector.SessionSource<Order>(client, "orders-by-customer");
```

### Source options

| Option | Default | Description |
| --- | --- | --- |
| `Entity`, `Subscription` | required, `null` | The queue, or the topic and its subscription |
| `SubQueue` | `None` | Read the `DeadLetter` or `TransferDeadLetter` sub-queue instead |
| `MaxInFlight` | 100 | The most messages received and not settled |
| `PrefetchCount` | 0 | Messages the SDK fetches ahead; keep it 0 for slow processing, so prefetched locks don't expire |
| `MaxLockRenewal` | 5 min | How long a held message's lock is renewed |
| `RowErrorHandler`, `RawExcerptLength` | fail, 256 | See [Messages that don't deserialize](message-queues.md#messages-that-dont-deserialize); `Skip` dead-letters with reason `DeserializationFailed`, and `Fail` abandons the message, so the broker dead-letters it after its maximum delivery count |
| `MaxConcurrentSessions`, `SessionIdleTimeout` | 8, 5 s | Session receiving |
| `SettleTimeout` | 30 s | How long receivers stay open after the read ends |

## Sending

```csharp
var invoices = ServiceBusConnector.Sink<Invoice>(client, "invoices", o => o with { SessionId = invoice => invoice.CustomerId });
builder.AddSink(invoices.Acknowledging(), "invoices");
```

The sink sends each batch in as few Service Bus message batches as the size limit allows, and settles each source
message once its body is sent. A message received from Service Bus keeps its message id (so duplicate detection works
across the hop), session, correlation id, subject, time to live and application properties (`CopyMessageProperties`).
The sink creates its own sender, so disposing it never closes one another node uses.

### Sink options

| Option | Default | Description |
| --- | --- | --- |
| `Entity` | required | The queue or topic |
| `BatchSize`, `BatchLinger` | 100, 10 ms | Messages per send, and the longest a batch waits to fill |
| `CopyMessageProperties` | `true` | Carry a received message's properties to the one sent |
| `MessageId`, `SessionId`, `Subject` | `null` | Choose each message's id, session (required by a session-enabled entity) and subject from its body |
| `TimeToLive` | entity's | How long each message lives unless received |
| `FailedMessages` | `Fail` | See [Failed writes](message-queues.md#failed-writes) |

## Dependency Injection

```csharp
services.AddServiceBusConnector("mynamespace.servicebus.windows.net", credential: null);   // DefaultAzureCredential
```

Registers one shared `ServiceBusClient` and `ServiceBusNodeFactory` (`CreateSource`, `CreateSubscriptionSource`,
`CreateSessionSource`, `CreateSink`). Overloads take a connection string, or a factory for the client.

## Next Steps

- [Message Queues: Shared Behaviour](message-queues.md)
- [Kafka](kafka.md), [RabbitMQ](rabbitmq.md), [AWS SQS](aws-sqs.md)
