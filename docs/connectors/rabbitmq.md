---
title: RabbitMQ Connector
description: Consume from and publish to RabbitMQ with NPipeline using the RabbitMQ connector with support for quorum queues, publisher confirms, dead-letter handling, and topology auto-declaration.
sidebar_position: 7
---

## RabbitMQ Connector

The `NPipeline.Connectors.RabbitMQ` package provides specialized source and sink nodes for working with RabbitMQ. This allows you to integrate RabbitMQ message queues into your pipelines as an input source, an output destination, or both.

This connector uses the official [RabbitMQ.Client 7.x](https://github.com/rabbitmq/rabbitmq-dotnet-client) library, providing fully asynchronous operations with `IChannel`, push-based consumers with backpressure, publisher confirms, quorum queue support, automatic topology declaration, and comprehensive dead-letter handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.RabbitMQ
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Quick Start

Here's a minimal pipeline that consumes messages from a RabbitMQ queue, processes them, and publishes results to another exchange:

```csharp
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

services.AddNPipeline(Assembly.GetExecutingAssembly());

// Register RabbitMQ connection
services.AddRabbitMq(o =>
{
    o.HostName = "localhost";
    o.Port = 5672;
    o.UserName = "guest";
    o.Password = "guest";
});

// Register source and sink
services.AddRabbitMqSource<OrderEvent>(new RabbitMqSourceOptions
{
    QueueName = "orders",
    PrefetchCount = 100,
});
services.AddRabbitMqSink<EnrichedOrder>(new RabbitMqSinkOptions
{
    ExchangeName = "enriched-orders",
    RoutingKey = "order.enriched",
});
```

## Configuration

### Connection Options

The `RabbitMqConnectionOptions` class configures the connection to the RabbitMQ broker:

```csharp
services.AddRabbitMq(o =>
{
    o.HostName = "rabbitmq.example.com";
    o.Port = 5672;
    o.VirtualHost = "/production";
    o.UserName = "app-user";
    o.Password = "secret";
    o.ClientProvidedName = "my-service"; // Visible in RabbitMQ Management UI
    o.AutomaticRecoveryEnabled = true;   // Default: true
    o.TopologyRecoveryEnabled = true;    // Default: true
    o.RequestedHeartbeat = TimeSpan.FromSeconds(60);
    o.MaxChannelPoolSize = 4;            // Default: 4
});
```

#### AMQP URI

You can use an AMQP URI instead of individual connection properties:

```csharp
services.AddRabbitMq(o =>
{
    o.Uri = new Uri("amqp://user:pass@host:5672/vhost");
});
```

#### TLS

```csharp
services.AddRabbitMq(o =>
{
    o.HostName = "rabbitmq.example.com";
    o.Port = 5671;
    o.Tls = new RabbitMqTlsOptions
    {
        Enabled = true,
        ServerName = "rabbitmq.example.com",
        CertificatePath = "/path/to/client.pfx",
        CertificatePassphrase = "cert-password",
    };
});
```

### Source Options

The `RabbitMqSourceOptions` configures the consumer node:

```csharp
services.AddRabbitMqSource<MyMessage>(new RabbitMqSourceOptions
{
    QueueName = "my-queue",                    // Required
    PrefetchCount = 100,                       // QoS prefetch (default: 100)
    InternalBufferCapacity = 1000,             // Backpressure buffer (default: 1000)
    ConsumerDispatchConcurrency = 1,           // Preserves ordering (default: 1)
    MaxDeliveryAttempts = 5,                   // Poison message rejection (default: 5)
    RejectOnMaxDeliveryAttempts = true,        // Reject without requeue (default: true)
    ContinueOnDeserializationError = false,    // Reject bad messages (default: false)
    RequeueOnNack = true,                      // Requeue nack'd messages (default: true)
});
```

### Sink Options

The `RabbitMqSinkOptions` configures the publisher node:

```csharp
services.AddRabbitMqSink<MyMessage>(new RabbitMqSinkOptions
{
    ExchangeName = "my-exchange",              // Required (use "" for default exchange)
    RoutingKey = "my.routing.key",             // Default routing key
    EnablePublisherConfirms = true,            // Wait for broker confirms (default: true)
    Persistent = true,                         // Durable messages (default: true)
    Mandatory = false,                         // Return unroutable messages (default: false)
    MaxRetries = 3,                            // Retry on transient failure (default: 3)
    RetryBaseDelayMs = 100,                    // Exponential backoff base (default: 100)
    ContinueOnError = false,                   // Stop pipeline on publish error (default: false)
});
```

#### Dynamic Routing Keys

```csharp
services.AddRabbitMqSink<MyMessage>(new RabbitMqSinkOptions
{
    ExchangeName = "events",
    RoutingKeySelector = obj =>
    {
        if (obj is MyMessage msg)
            return $"events.{msg.Type.ToLowerInvariant()}";
        return "events.unknown";
    },
});
```

#### Batch Publishing

```csharp
services.AddRabbitMqSink<MyMessage>(new RabbitMqSinkOptions
{
    ExchangeName = "my-exchange",
    Batching = new BatchPublishOptions
    {
        BatchSize = 100,
        LingerTime = TimeSpan.FromMilliseconds(50),
    },
});
```

## Topology Auto-Declaration

Both source and sink nodes can automatically declare exchanges, queues, and bindings at startup. This is useful for development and testing but can be disabled in production where topology is managed externally.

### Source Topology

```csharp
services.AddRabbitMqSource<MyMessage>(new RabbitMqSourceOptions
{
    QueueName = "orders",
    Topology = new RabbitMqTopologyOptions
    {
        AutoDeclare = true,
        Durable = true,
        QueueType = QueueType.Quorum,          // Classic, Quorum, or Stream
        DeadLetterExchange = "orders-dlx",     // Broker-level dead-letter exchange
        DeadLetterRoutingKey = "orders.dead",
        MessageTtlMs = 60_000,                 // Per-queue message TTL
        MaxLength = 100_000,                   // Max queue length
        Bindings =
        [
            new BindingOptions("orders-exchange", "order.created"),
            new BindingOptions("orders-exchange", "order.updated"),
        ],
        ExchangeType = "topic",
    },
});
```

### Sink Topology

```csharp
services.AddRabbitMqSink<MyMessage>(new RabbitMqSinkOptions
{
    ExchangeName = "enriched-orders",
    Topology = new RabbitMqTopologyOptions
    {
        AutoDeclare = true,
        Durable = true,
        ExchangeType = "topic",
    },
});
```

## Message Model

The `RabbitMqMessage<T>` class wraps consumed messages with RabbitMQ-specific metadata and ack/nack capabilities:

```csharp
public class OrderProcessor : TransformNode<RabbitMqMessage<OrderEvent>, EnrichedOrder>
{
    public override async Task<EnrichedOrder> ExecuteAsync(
        RabbitMqMessage<OrderEvent> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Access the deserialized body
        var order = input.Body;

        // Access RabbitMQ metadata
        Console.WriteLine($"Exchange: {input.Exchange}");
        Console.WriteLine($"Routing Key: {input.RoutingKey}");
        Console.WriteLine($"Delivery Tag: {input.DeliveryTag}");
        Console.WriteLine($"Redelivered: {input.Redelivered}");
        Console.WriteLine($"Correlation ID: {input.CorrelationId}");

        // Manual acknowledgment (if not using AutoOnSinkSuccess)
        await input.AcknowledgeAsync(cancellationToken);

        // Or negative acknowledgment with requeue
        // await input.NegativeAcknowledgeAsync(requeue: true, cancellationToken);

        return new EnrichedOrder(order, DateTime.UtcNow);
    }
}
```

### Acknowledgment Strategies

| Strategy | Description |
|----------|-------------|
| `AutoOnSinkSuccess` | Messages are acknowledged after the sink node successfully publishes them. This is the default and provides at-least-once delivery. |
| `Manual` | Your code must call `AcknowledgeAsync()` or `NegativeAcknowledgeAsync()` explicitly. |

### Thread-Safe Ack State Machine

The `RabbitMqMessage<T>` uses atomic `Interlocked.CompareExchange` for ack state transitions:

- **Pending** → **Acknowledged** (via `AcknowledgeAsync`)
- **Pending** → **Nacked** (via `NegativeAcknowledgeAsync`)
- Double-ack is idempotent (no-op)
- Ack after nack (or vice versa) throws `InvalidOperationException`

## Dead-Letter Handling

### Broker-Level Dead Lettering

Configure DLX via topology options — the broker automatically routes rejected/expired messages:

```csharp
Topology = new RabbitMqTopologyOptions
{
    DeadLetterExchange = "my-dlx",
    DeadLetterRoutingKey = "dead-letter",
}
```

### Pipeline-Level Dead Lettering

For transform/processing failures, use `RabbitMqDeadLetterSink`:

```csharp
var deadLetterSink = new RabbitMqDeadLetterSink(
    connectionManager,
    deadLetterExchange: "pipeline-dlx",
    routingKey: "pipeline.errors");
```

Dead-lettered messages include enriched headers:

| Header | Description |
|--------|-------------|
| `x-death-reason` | The exception message |
| `x-death-node` | The pipeline node ID that failed |
| `x-death-timestamp` | ISO 8601 timestamp of the failure |
| `x-death-exception-type` | The .NET exception type name |
| `x-death-stack-trace` | Truncated stack trace (max 2048 chars) |
| `x-original-exchange` | Original exchange (if from RabbitMQ source) |
| `x-original-routing-key` | Original routing key |
| `x-original-message-id` | Original message ID |

## Connection Management

The `RabbitMqConnectionManager` provides:

- **Lazy connection creation** — connects on first use
- **Automatic recovery** — reconnects on connection loss
- **Channel pooling** — bounded pool with configurable size for publisher channels
- **Publisher confirms** — pooled channels have confirms enabled automatically

Consumer channels are created separately (not pooled) since they are long-lived.

## Metrics

Implement `IRabbitMqMetrics` to capture connector metrics. The default `NullRabbitMqMetrics` is a no-op:

```csharp
public class PrometheusRabbitMqMetrics : IRabbitMqMetrics
{
    // Source metrics
    public void RecordConsumed(string queue, int count) { /* ... */ }
    public void RecordConsumeLatency(string queue, double milliseconds) { /* ... */ }
    public void RecordDeserializationError(string queue) { /* ... */ }
    public void RecordAck(string queue, int count) { /* ... */ }
    public void RecordNack(string queue, int count, bool requeued) { /* ... */ }

    // Sink metrics
    public void RecordPublished(string exchange, string routingKey, int count) { /* ... */ }
    public void RecordPublishLatency(string exchange, double milliseconds) { /* ... */ }
    public void RecordConfirmLatency(string exchange, double milliseconds) { /* ... */ }
    // ... and more
}

// Register before AddRabbitMq to override the default no-op
services.AddSingleton<IRabbitMqMetrics, PrometheusRabbitMqMetrics>();
```

## Serialization

The connector uses `IMessageSerializer` for message serialization. The default `RabbitMqJsonSerializer` uses `System.Text.Json` with camelCase naming.

Override with a custom serializer:

```csharp
services.AddSingleton<IMessageSerializer, MyCustomSerializer>();
services.AddRabbitMq(o => { /* ... */ });
```

## Architecture

```
                    ┌──────────────────────────────────┐
                    │    RabbitMQ Broker                │
                    │                                  │
                    │  ┌──────────┐   ┌─────────────┐ │
                    │  │ Exchange │──▶│    Queue     │ │
                    │  └──────────┘   └──────┬──────┘ │
                    └────────────────────────┼────────┘
                                             │ Push (BasicDeliver)
                    ┌────────────────────────▼────────┐
                    │  AsyncEventingBasicConsumer      │
                    │  (prefetch QoS controls rate)    │
                    └────────────────────────┬────────┘
                                             │ Write
                    ┌────────────────────────▼────────┐
                    │  Bounded Channel<T>              │
                    │  (backpressure buffer)           │
                    └────────────────────────┬────────┘
                                             │ ReadAllAsync
                    ┌────────────────────────▼────────┐
                    │  IAsyncEnumerable<T> / DataPipe  │
                    │  (NPipeline streaming surface)   │
                    └────────────────────────┬────────┘
                                             │
                    ┌────────────────────────▼────────┐
                    │  Transform Nodes                 │
                    │  (enrichment, filtering, etc.)   │
                    └────────────────────────┬────────┘
                                             │
                    ┌────────────────────────▼────────┐
                    │  RabbitMqSinkNode<T>             │
                    │  (BasicPublishAsync + confirms)  │
                    └────────────────────────┬────────┘
                                             │ Publish
                    ┌────────────────────────▼────────┐
                    │  RabbitMQ Broker (output)        │
                    └─────────────────────────────────┘
```

## Sample

See the [Sample_RabbitMqConnector](https://github.com/your-org/NPipeline/tree/main/samples/Sample_RabbitMqConnector) for a complete working example with docker-compose.

## Troubleshooting

### Consumer stops receiving messages

- Check that `PrefetchCount` is not set to 0 (minimum is 1)
- Verify the queue exists and has messages using `rabbitmqctl list_queues`
- Check consumer tag in RabbitMQ Management UI under Connections/Channels

### Publisher confirms timeout

- Ensure the channel was created with `PublisherConfirmationsEnabled` (automatic with DI)
- Check broker health and disk space (alarms block publishing)
- Increase `ConfirmTimeout` if the broker is under heavy load

### Messages not routed

- Verify exchange type matches the routing key pattern
- Check bindings in RabbitMQ Management UI
- Enable `Mandatory = true` on the sink to get unroutable message notifications
