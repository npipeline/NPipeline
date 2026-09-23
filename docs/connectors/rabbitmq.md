---
title: "RabbitMQ Connector"
description: "Consume from and publish to RabbitMQ with quorum queues, publisher confirms, and dead-letter handling."
order: 16
---

# RabbitMQ Connector

The `NPipeline.Connectors.RabbitMQ` package provides source and sink nodes for [RabbitMQ](https://www.rabbitmq.com/). Supports quorum queues, QoS prefetch, publisher confirms, batch publishing, TLS, automatic topology declaration, dead-letter exchanges, and poison message detection.

## Installation

```bash
dotnet add package NPipeline.Connectors.RabbitMQ
```

**Dependencies:** [RabbitMQ.Client](https://www.nuget.org/packages/RabbitMQ.Client) 7.x

## Source Node - `RabbitMqSourceNode<T>`

### Constructor

```csharp
public RabbitMqSourceNode(
    RabbitMqSourceOptions options,
    IRabbitMqConnectionManager connectionManager,
    IMessageSerializer serializer,
    IRabbitMqMetrics? metrics = null,
    ILogger<RabbitMqSourceNode<T>>? logger = null)
```

### Example

```csharp
var sourceOptions = new RabbitMqSourceOptions("order-queue")
{
    PrefetchCount = 100,
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
    MaxDeliveryAttempts = 5,
    Topology = new RabbitMqTopologyOptions
    {
        AutoDeclare = true,
        QueueType = QueueType.Quorum,
        DeadLetterExchange = "dlx"
    }
};
```

## Sink Node - `RabbitMqSinkNode<T>`

### Constructor

```csharp
public RabbitMqSinkNode(
    RabbitMqSinkOptions options,
    IRabbitMqConnectionManager connectionManager,
    IMessageSerializer serializer,
    IRabbitMqMetrics? metrics = null,
    ILogger<RabbitMqSinkNode<T>>? logger = null)
```

### Example

```csharp
var sinkOptions = new RabbitMqSinkOptions("order-exchange")
{
    RoutingKey = "processed",
    EnablePublisherConfirms = true,
    Persistent = true,
    Batching = new BatchPublishOptions
    {
        BatchSize = 100,
        LingerTime = TimeSpan.FromMilliseconds(50)
    }
};
```

## Configuration

### Connection - `RabbitMqConnectionOptions`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `HostName` | `string` | `"localhost"` | RabbitMQ server hostname |
| `Port` | `int` | `5672` | AMQP port |
| `VirtualHost` | `string` | `"/"` | Virtual host |
| `UserName` | `string` | `"guest"` | Username |
| `Password` | `string` | `"guest"` | Password |
| `Uri` | `Uri?` | `null` | Full AMQP URI (overrides individual settings) |
| `AutomaticRecoveryEnabled` | `bool` | `true` | Auto-reconnect on failure |
| `RequestedHeartbeat` | `TimeSpan` | `60s` | Heartbeat interval |
| `MaxChannelPoolSize` | `int` | `4` | Max pooled channels |

### TLS - `RabbitMqTlsOptions`

```csharp
var connection = new RabbitMqConnectionOptions
{
    HostName = "rabbitmq.example.com",
    Port = 5671,
    Tls = new RabbitMqTlsOptions
    {
        Enabled = true,
        ServerName = "rabbitmq.example.com",
        CertificatePath = "/path/to/client.pfx",
        SslProtocols = SslProtocols.Tls12
    }
};
```

### Source - `RabbitMqSourceOptions`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `QueueName` | `string` | (required) | Queue to consume from |
| `PrefetchCount` | `ushort` | `100` | QoS prefetch count |
| `AcknowledgmentStrategy` | `AcknowledgmentStrategy` | `AutoOnSinkSuccess` | When to ACK messages |
| `RequeueOnNack` | `bool` | `true` | Requeue rejected messages |
| `MaxDeliveryAttempts` | `int?` | `5` | Poison message threshold |
| `RejectOnMaxDeliveryAttempts` | `bool` | `true` | Reject after max attempts |
| `ConsumerDispatchConcurrency` | `int` | `1` | Concurrent dispatch |
| `InternalBufferCapacity` | `int` | `1000` | Internal buffer size |

### Sink - `RabbitMqSinkOptions`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ExchangeName` | `string` | (required) | Exchange to publish to |
| `RoutingKey` | `string` | `""` | Default routing key |
| `RoutingKeySelector` | `Func<object, string>?` | `null` | Per-message routing key |
| `EnablePublisherConfirms` | `bool` | `true` | Wait for broker confirmation (off: at-most-once, see [Resilience](#resilience)) |
| `Persistent` | `bool` | `true` | Mark messages as persistent |
| `Mandatory` | `bool` | `false` | Require at least one queue binding |
| `ContinueOnError` | `bool` | `false` | Skip a message whose publish fails instead of failing the node |
| `ConfirmTimeout` | `TimeSpan` | `5s` | How long one publish attempt waits for the broker's confirm |
| `ShutdownFlushTimeout` | `TimeSpan` | `30s` | How long the batched sink keeps publishing after the pipeline is cancelled |
| `Resilience` | `Resilience` | `RabbitMqConnectorResilience.Default` | How each publish is retried (see [Resilience](#resilience)) |

### Batch Publishing - `BatchPublishOptions`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BatchSize` | `int` | `100` | Messages per batch |
| `LingerTime` | `TimeSpan` | `50ms` | Time to wait before sending partial batch |

### Topology - `RabbitMqTopologyOptions`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `AutoDeclare` | `bool` | `true` | Auto-declare exchanges, queues, and bindings |
| `QueueType` | `QueueType` | `Quorum` | `Classic`, `Quorum` (recommended), or `Stream` |
| `Durable` | `bool` | `true` | Durable queue/exchange |
| `DeadLetterExchange` | `string?` | `null` | Dead-letter exchange name |
| `DeadLetterRoutingKey` | `string?` | `null` | Dead-letter routing key |
| `MessageTtlMs` | `int?` | `null` | Message TTL in milliseconds |
| `MaxLength` | `int?` | `null` | Max queue length (messages) |
| `MaxLengthBytes` | `int?` | `null` | Max queue size (bytes) |

## Dependency Injection

```csharp
services.AddRabbitMq(connection =>
{
    connection.HostName = "rabbitmq.example.com";
    connection.UserName = "app";
    connection.Password = "secret";
});

services.AddRabbitMqSource<Order>(new RabbitMqSourceOptions("order-queue")
{
    PrefetchCount = 200,
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess
});

services.AddRabbitMqSink<ProcessedOrder>(new RabbitMqSinkOptions("processed-exchange")
{
    RoutingKey = "orders.processed",
    EnablePublisherConfirms = true
});
```

## Next Steps

- [Kafka Connector](kafka.md) - distributed streaming platform
- [Azure Service Bus Connector](azure-service-bus.md) - managed cloud messaging
- [AWS SQS Connector](aws-sqs.md) - managed cloud queuing

## Topology Auto-Declaration

When `AutoDeclare = true` (default), the connector creates exchanges, queues, and bindings on startup:

```csharp
var topology = new RabbitMqTopologyOptions
{
    AutoDeclare = true,
    QueueType = QueueType.Quorum,
    Durable = true,
    DeadLetterExchange = "dlx",
    DeadLetterRoutingKey = "dead-letter"
};
```

### Queue Types

| Type | Description |
|------|-------------|
| `Classic` | Traditional RabbitMQ queues |
| `Quorum` (default) | Replicated, fault-tolerant - recommended for production |
| `Stream` | Append-only log - for replay scenarios |

## Dynamic Routing Keys

Route messages per-item using `RoutingKeySelector`:

```csharp
var sink = new RabbitMqSinkNode<Order>(new RabbitMqSinkOptions("order-exchange")
{
    RoutingKeySelector = order => $"orders.{order.Region.ToLower()}"
});
```

## Resilience

The sink retries each publish through [NResilience](https://github.com/nresilience/NResilience). The
`Resilience` property on `RabbitMqSinkOptions` configures it. The default, `RabbitMqConnectorResilience.Default`,
does the following:

- Makes up to four attempts (three retries).
- Waits with exponential backoff and full jitter, from 100 milliseconds up to 30 seconds.
- Retries a lost connection, a closed channel, an unreachable broker, a forced close (320), a broker internal error
  (541), and a publish the broker nacked.
- Does not retry access refused (403), not found (404), resource locked (405), precondition failed (406), a message
  returned as unroutable (312, with `Mandatory`), failed authentication, or a close the application asked for.
- Has no attempt timeout and no overall deadline, so the attempt count bounds the call.

A closed channel never reopens, so a retry that finds its channel closed takes a fresh one from the pool. Every
attempt carries the same `MessageId`, so a consumer can discard a duplicate.

Each attempt waits up to `ConfirmTimeout` (5 seconds) for the broker's publisher confirm. A confirm that doesn't
arrive in time fails the attempt with a `TimeoutException` and the policy retries it. The
unconfirmed message may still have reached the broker, so the retry can publish it twice.

With `EnablePublisherConfirms = false`, the sink publishes on channels that don't track confirms, and a publish
completes once the message is written to the connection; `ConfirmTimeout` doesn't apply. A message lost after that
(the connection drops, or the broker fails before routing it) goes undetected, and the source message is still
acknowledged, so delivery is **at-most-once**. Channels with and without confirms are pooled apart, so sinks with
either setting can share one `IRabbitMqConnectionManager`.

With batching on, messages are published in order and each one is retried on its own; the messages published
before it are not published again. If a message still fails, the source messages published before it are
acknowledged, and the failed message and those after it are not. The node then fails, or, with
`ContinueOnError`, drops the rest of the batch and carries on. Only one flush runs at a time: the linger timer and
a full batch never publish together.

When the pipeline is cancelled, the batched sink publishes and acknowledges the messages it has already taken from
the input, for up to `ShutdownFlushTimeout` (30 seconds), and then reports the cancellation. Messages it can't publish
in that time stay unacknowledged, so the broker redelivers them. A message already published is never published again
by the shutdown flush.

To change a setting, derive a policy with a `with` expression:

```csharp
var options = new RabbitMqSinkOptions
{
    ExchangeName = "orders",
    Resilience = RabbitMqConnectorResilience.Default with
    {
        Attempts = 6,
        AttemptTimeout = TimeSpan.FromSeconds(10),
    },
};
```

A publish that times out while it waits for a publisher confirm may still have reached the broker, so its retry can
publish the message twice. To publish once, use `Resilience.None`.

The sink is the only layer that retries a publish. The client's automatic connection recovery
(`AutomaticRecoveryEnabled`) still reconnects in the background, but it does not replay a failed publish.

The source message is acknowledged after its publish succeeds, outside the retried call. A failed acknowledgement
never publishes the message again, and a failed publish never acknowledges it.

## Connection Management

- **Lazy connection**: Connections are created on first use
- **Automatic recovery**: The underlying RabbitMQ client reconnects on failure
- **Channel pooling**: Channels are pooled and reused across operations

## Push-to-Pull Bridge

`RabbitMqSourceNode<T>` internally bridges RabbitMQ's push-based consumer to NPipeline's pull-based model using a bounded `Channel<T>`:

```csharp
var source = new RabbitMqSourceNode<Order>(new RabbitMqSourceOptions("order-queue")
{
    InternalBufferCapacity = 1000,  // bounded channel capacity
    PrefetchCount = 200             // QoS prefetch
});
```

If the buffer fills, RabbitMQ backpressure kicks in (broker stops delivering until space is available).

## Acknowledgment Strategies

| Strategy | Description |
|----------|-------------|
| `AutoOnSinkSuccess` (default) | ACK after sink processing completes |
| `Manual` | Call `message.AcknowledgeAsync()` explicitly |

### Poison Message Handling

```csharp
var source = new RabbitMqSourceNode<Order>(new RabbitMqSourceOptions("order-queue")
{
    MaxDeliveryAttempts = 5,
    RejectOnMaxDeliveryAttempts = true,  // NACK without requeue → goes to DLX
    RequeueOnNack = true                 // requeue on failure (before max attempts)
});
```

## Observability

Implement `IRabbitMqMetrics` to collect connection, channel, publish, and consume metrics:

```csharp
services.AddSingleton<IRabbitMqMetrics, MyRabbitMqMetrics>();
```

## Best Practices

1. **Use quorum queues** - fault-tolerant and recommended for production
2. **Enable publisher confirms** - ensures messages reach the broker
3. **Configure DLX** for poison message handling
4. **Set `PrefetchCount`** proportional to consumer throughput
5. **Use TLS** in production (`Tls.Enabled = true`)
6. **Tune `InternalBufferCapacity`** - too small causes backpressure, too large wastes memory
7. **Use batch publishing** for high-throughput sinks (`BatchSize`, `LingerTime`)
