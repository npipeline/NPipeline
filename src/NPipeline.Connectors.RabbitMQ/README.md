# NPipeline.Connectors.RabbitMQ

RabbitMQ connector for [NPipeline](https://github.com/your-org/NPipeline) — consume from and publish to RabbitMQ queues with integrated backpressure, publisher
confirms, and dead-letter handling.

## Features

- **Source node** — Push-based `AsyncEventingBasicConsumer` with bounded channel backpressure
- **Sink node** — Sequential and batched publishing with publisher confirms
- **Quorum queues** — First-class support for Classic, Quorum, and Stream queue types
- **Topology auto-declaration** — Exchanges, queues, and bindings declared at startup
- **Dead-letter handling** — Both broker-level (DLX) and pipeline-level with enriched headers
- **Thread-safe acknowledgment** — Atomic ack/nack state machine
- **Pluggable metrics** — Implement `IRabbitMqMetrics` for observability
- **Pluggable serialization** — Default `System.Text.Json`, override with `IMessageSerializer`

## Quick Start

```csharp
services.AddRabbitMq(o =>
{
    o.HostName = "localhost";
    o.UserName = "guest";
    o.Password = "guest";
});

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

## Requirements

- .NET 8.0, 9.0, or 10.0
- RabbitMQ 3.12+ (4.x recommended for quorum queues)

## Documentation

See the [full documentation](https://your-org.github.io/NPipeline/connectors/rabbitmq) for configuration, topology, dead-letter handling, and more.
