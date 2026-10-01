---
title: "RabbitMQ Connector"
description: "Consume from and publish to RabbitMQ with prefetch, publisher confirms, batched publishing, topology declaration and dead-letter exchanges."
order: 17
---

# RabbitMQ Connector

The `NPipeline.Connectors.RabbitMQ` package consumes RabbitMQ queues and publishes to exchanges, over one shared
connection, with publisher confirms by default. Serialization, settlement, undeserializable messages and failed writes
work as in every message-queue connector; see [Message Queues: Shared Behaviour](message-queues.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.RabbitMQ
```

**Dependencies:** [RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client) 7.x

## Connection

Sources and sinks share one connection, which connects on first use and recovers automatically:

```csharp
await using var connection = RabbitMqConnector.Connect(new RabbitMqConnectionOptions
{
    HostName = "rabbit.example.com",
    UserName = "pipeline",
    Password = secret,
});
```

| Option | Default | Description |
| --- | --- | --- |
| `HostName`, `Port`, `VirtualHost` | `localhost`, 5672, `/` | The broker |
| `UserName`, `Password` | `guest` | Credentials; RabbitMQ allows `guest` only from localhost |
| `Uri` | `null` | A full `amqp://` or `amqps://` URI, instead of the settings above |
| `Tls` | `null` | TLS: `Enabled`, `ServerName`, `CertificatePath`, `CertificatePassphrase`, `SslProtocols` |
| `AutomaticRecoveryEnabled`, `NetworkRecoveryInterval` | `true`, 5 s | Reconnects after a failure (it doesn't replay a failed publish; the sink retries that) |
| `TopologyRecoveryEnabled` | `true` | Redeclares the client's exchanges, queues and bindings after a reconnect |
| `RequestedHeartbeat` | 60 s | The heartbeat interval |
| `MaxChannelPoolSize` | 4 | Publishing channels kept for reuse |
| `ClientProvidedName` | `null` | The connection name shown in the management UI |

For production, use TLS (port 5671):

```csharp
var connection = RabbitMqConnector.Connect(new RabbitMqConnectionOptions
{
    HostName = "rabbit.example.com",
    Port = 5671,
    Tls = new RabbitMqTlsOptions
    {
        Enabled = true,
        ServerName = "rabbit.example.com",
        CertificatePath = "/path/to/client.pfx",
        SslProtocols = SslProtocols.Tls12,
    },
});
```

## Consuming

```csharp
var orders = RabbitMqConnector.Source<Order>(connection, "orders", o => o with { PrefetchCount = 200 });
```

Each message is a `RabbitMqMessage<T>` with its `Body`, `Exchange`, `RoutingKey`, `DeliveryTag`, `Redelivered`,
`CorrelationId`, `Headers` and all its `Properties`. The source has its own channel, and the broker delivers at most
`PrefetchCount` unsettled messages ahead: once that many are neither acknowledged nor rejected, it waits, which is the
source's backpressure. When the read ends, the consumer is cancelled, messages delivered but not handed on go back on
the queue, and the channel stays open until the messages handed on are settled (up to `SettleTimeout`).

`RejectAsync(requeue: false)` sends a message to the queue's dead-letter exchange, if it has one.

### Source options

| Option | Default | Description |
| --- | --- | --- |
| `Queue` | required | The queue |
| `PrefetchCount` | 100 | Unsettled messages the broker delivers ahead |
| `ConsumerTag`, `Exclusive` | broker's, `false` | The consumer's tag, and whether it is the queue's only consumer |
| `Topology` | `null` | Declares the queue and its bindings first; see [Topology](#topology) |
| `RowErrorHandler`, `RawExcerptLength` | fail, 256 | See [Messages that don't deserialize](message-queues.md#messages-that-dont-deserialize); `Skip` rejects without requeue |
| `MaxDeliveryAttempts` | `null` | Rejects, without requeue, a message delivered more than this many times |
| `SettleTimeout` | 30 s | How long the channel stays open after the read ends |

`MaxDeliveryAttempts` counts from the `x-delivery-count` header that quorum queues set, or `x-death` after dead-letter
cycles; classic queues count neither. Quorum queues also enforce a delivery limit of their own (20 by default in
RabbitMQ 4), so a message that keeps failing is dead-lettered by the broker without it.

To route poison messages away, give the queue a dead-letter exchange through [Topology](#topology) and set
`MaxDeliveryAttempts`: a message past the limit is rejected without requeue and the broker moves it to that exchange.
A message rejected with `requeue: true` goes back on the queue and is delivered again.

## Publishing

```csharp
var sink = RabbitMqConnector.Sink<Invoice>(connection, exchange: "billing", routingKey: "invoices");
var toQueue = RabbitMqConnector.Sink<Invoice>(connection, exchange: "", routingKey: "invoices");   // the default exchange routes to the queue
var byRegion = RabbitMqConnector.Sink<Order>(connection, "orders", configure: o => o with { RoutingKeySelector = order => $"orders.{order.Region}" });
```

The sink publishes each batch's messages together on one channel and awaits their confirms together, so a batch costs
about one round trip. A message received from RabbitMQ keeps its message id, correlation id, headers, type and
priority (`CopyMessageProperties`); others get a new message id, which stays the same across retries so a consumer can
drop a duplicate.

### Sink options

| Option | Default | Description |
| --- | --- | --- |
| `Exchange`, `RoutingKey` | required, `""` | Where messages go; `""` is the default exchange |
| `RoutingKeySelector` | `null` | Chooses each message's routing key from its body |
| `PublisherConfirms`, `ConfirmTimeout` | `true`, 5 s | Wait for the broker's confirm before a message counts as published |
| `Persistent` | `true` | Delivery mode 2 |
| `Mandatory` | `false` | The broker returns a message no queue is bound for, which fails its publish |
| `ContentType`, `AppId` | the serializer's, none | Set on every message |
| `BatchSize`, `BatchLinger` | 100, 10 ms | Messages published together, and the longest a batch waits to fill |
| `FailedMessages` | `Fail` | See [Failed writes](message-queues.md#failed-writes) |
| `CopyMessageProperties` | `true` | Carry a received message's properties to the one published |
| `Topology` | `null` | Declares the exchange first |
| `Resilience` | `RabbitMqConnectorResilience.Default` | How a failed publish is retried |

With `PublisherConfirms = false`, a publish completes once the message is written to the connection, so a message lost
after that goes undetected and its source message is still acknowledged: delivery is at-most-once.

## Resilience

A batch's publish is each message's first attempt. A message whose publish failed with an error the classifier calls
transient is retried on its own with the rest of `Resilience`'s attempts, on a fresh channel if the old one closed;
messages already published are never published again. `RabbitMqConnectorResilience.Default`:

- Makes up to four attempts in all, with exponential backoff and full jitter from 100 ms up to 30 s.
- Retries a lost connection, a closed channel, an unreachable broker, a forced close (320), an internal error (541), a
  publish the broker nacked, and a confirm that didn't arrive within `ConfirmTimeout`.
- Doesn't retry access refused (403), not found (404), resource locked (405), precondition failed (406), a message
  returned as unroutable (312, with `Mandatory`), failed authentication, or a close the application asked for.

A confirm that times out may still have reached the broker, so its retry can publish the message twice. To publish at
most once per message, use `Resilience.None`.

Once a batch's retries are done, the source messages whose publish succeeded are acknowledged first, then each failed
message is handled as `FailedMessages` says. With `Fail`, the write fails and the failed messages stay unacknowledged,
so the broker delivers them again; a published message is never left unacknowledged and published again.
The client's automatic recovery only reconnects; a failed publish is retried by the sink alone. To change the policy,
derive one: `RabbitMqConnectorResilience.Default with { Attempts = 6 }`.

## Topology

`RabbitMqTopologyOptions` declares what a node needs before it starts:

```csharp
var orders = RabbitMqConnector.Source<Order>(connection, "orders", o => o with
{
    Topology = new RabbitMqTopologyOptions
    {
        QueueType = QueueType.Quorum,
        DeadLetterExchange = "orders-dlx",
        ExchangeType = "topic",
        Bindings = [new BindingOptions("orders-exchange", "orders.#")],
    },
});
```

| Option | Default | Description |
| --- | --- | --- |
| `AutoDeclare` | `true` | Declare when the topology is set |
| `QueueType` | `Quorum` | `Classic`, `Quorum` (replicated, recommended) or `Stream` |
| `Durable`, `AutoDelete`, `Exclusive` | `true`, `false`, `false` | Queue and exchange flags |
| `ExchangeType` | `null` | Declares the bindings' exchanges (source) or the sink's exchange, of this type |
| `DeadLetterExchange`, `DeadLetterRoutingKey` | `null` | Where rejected messages go |
| `MessageTtlMs`, `MaxLength`, `MaxLengthBytes` | `null` | Queue limits |
| `Bindings` | none | Bindings from exchanges to the source's queue |
| `PassiveDeclare` | `false` | Check that the queue or exchange exists instead of declaring it |
| `ExtraArguments` | `null` | Other queue or exchange arguments by name, such as `x-max-priority` |

## Dead letters

`RabbitMqDeadLetterSink` is a pipeline dead-letter sink that publishes failed items to an exchange, with the error in
`x-death-*` headers. A `MessageFailure` is published with its original body and message id, so it can be replayed.

```csharp
builder.AddDeadLetterSink(new RabbitMqDeadLetterSink(connection, exchange: "orders-dlx", routingKey: "orders"));
```

## Dependency Injection

```csharp
services.AddRabbitMq(o => o with { HostName = "rabbit.example.com", UserName = "pipeline", Password = secret });
```

Registers one shared `IRabbitMqConnectionManager` and `RabbitMqNodeFactory`, whose `CreateSource<T>(queue, configure)`
and `CreateSink<T>(exchange, routingKey, configure)` build nodes on it.

## Best practices

1. **Use quorum queues** (the default): they are replicated and fault tolerant, which production needs.
2. **Keep publisher confirms on.** Without them a lost message goes undetected.
3. **Declare a dead-letter exchange** for the queues you consume, so poison messages are set aside.
4. **Size `PrefetchCount` to the consumer's throughput.** Too low starves the pipeline; too high holds messages that
   another consumer could process.
5. **Use TLS** in production.
6. **Raise `BatchSize`** on high-throughput sinks; a batch costs about one round trip.

## Next Steps

- [Message Queues: Shared Behaviour](message-queues.md)
- [Kafka](kafka.md), [Azure Service Bus](azure-service-bus.md), [AWS SQS](aws-sqs.md)
