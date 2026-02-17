# NPipeline.Connectors.Kafka

Apache Kafka connector for NPipeline - integrate with Kafka for high-throughput streaming with multiple serialization formats and delivery semantics.

## Features

- **Source & Sink Nodes**: Read from and write to Kafka topics with type-safe message handling
- **Multiple Serialization Formats**: JSON (default), Apache Avro, and Protocol Buffers with Schema Registry support
- **Flexible Delivery Semantics**: At-least-once (default) and exactly-once delivery guarantees
- **Idempotent Production**: Prevent duplicate messages with configurable acknowledgment modes
- **Partition Management**: Custom partition key providers for sophisticated message routing
- **Consumer Groups**: Offset management and parallel processing across partitions
- **Message Acknowledgment**: Manual control over offset commits with acknowledgment callbacks
- **Error Handling**: Exponential backoff retry strategies for transient errors
- **Kafka Authentication**: Support for SASL/PLAIN and SASL/SSL security protocols
- **Message Metadata**: Access to Kafka-specific properties (topic, partition, offset, timestamp, headers)
- **Dead-Letter Envelope**: Optional `DeadLetterEnvelope` model for custom routing
- **Monitoring**: Built-in metrics collection for production observability

## Installation

```bash
dotnet add package NPipeline.Connectors.Kafka
```

## Quick Start

### Reading from Kafka

```csharp
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Pipeline;

public record Order(string OrderId, string CustomerId, decimal Amount);

var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SourceTopic = "orders",
    ConsumerGroupId = "order-processor",
    AutoOffsetReset = AutoOffsetReset.Latest,
};

var source = new KafkaSourceNode<Order>(config);

var sourceHandle = builder.AddSource(source, "kafka-source");
var sinkHandle = builder.AddSink(async (KafkaMessage<Order> message, CancellationToken ct) =>
{
    Console.WriteLine($"Processing: {message.Body.OrderId}");
    await message.AcknowledgeAsync(ct);
}, "process-order");

builder.Connect(sourceHandle, sinkHandle);
```

### Writing to Kafka

```csharp
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Pipeline;

public record OrderEvent(string OrderId, string EventType, DateTime Timestamp);

var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SinkTopic = "order-events",
    Acks = Acks.All,
};

var sink = new KafkaSinkNode<OrderEvent>(config);

var sourceHandle = builder.AddSource(() => new[]
{
    new OrderEvent("ORD-001", "Created", DateTime.UtcNow),
    new OrderEvent("ORD-002", "Shipped", DateTime.UtcNow),
}, "orders-source");

var sinkHandle = builder.AddSink(sink, "kafka-sink");

builder.Connect(sourceHandle, sinkHandle);
```

## Serialization Formats

```csharp
// JSON (default, no Schema Registry needed)
var config = new KafkaConfiguration
{
    SerializationFormat = SerializationFormat.Json,
};

// Avro with Schema Registry
var config = new KafkaConfiguration
{
    SerializationFormat = SerializationFormat.Avro,
    SchemaRegistry = new SchemaRegistryConfiguration
    {
        Url = "http://localhost:8081",
        AutoRegisterSchemas = true,
    },
};

// Protocol Buffers with Schema Registry
var config = new KafkaConfiguration
{
    SerializationFormat = SerializationFormat.Protobuf,
    SchemaRegistry = new SchemaRegistryConfiguration
    {
        Url = "http://localhost:8081",
    },
};
```

## Delivery Semantics

```csharp
// At-least-once (default)
var config = new KafkaConfiguration
{
    DeliverySemantic = DeliverySemantic.AtLeastOnce,
};

// Exactly-once
var config = new KafkaConfiguration
{
    DeliverySemantic = DeliverySemantic.ExactlyOnce,
    EnableTransactions = true,
    TransactionalId = "order-processor-1",
    EnableIdempotence = true,
    Acks = Acks.All,
};
```

## Tuning

```csharp
var config = new KafkaConfiguration
{
    PollTimeoutMs = 100,            // Consumer poll timeout
    TransactionInitTimeoutMs = 30000, // Transaction init timeout
};
```

## Authentication

```csharp
// SASL/Plain over TLS
var config = new KafkaConfiguration
{
    BootstrapServers = "kafka.example.com:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "username",
    SaslPassword = "password",
};
```

## Documentation

For comprehensive documentation, including advanced topics, partitioning, dead letter handling, and best practices, see
the [Kafka Connector Documentation](https://www.npipeline.dev/docs/connectors/kafka).

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Base abstractions for connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration
- **[NPipeline.Extensions.Observability.OpenTelemetry](https://www.nuget.org/packages/NPipeline.Extensions.Observability.OpenTelemetry)** - Observability and
  tracing

## Requirements

- .NET 8.0, 9.0, or 10.0
- Confluent.Kafka 2.6.1+ (automatically included)
- Confluent.SchemaRegistry 2.6.1+ for Avro/Protobuf support

## License

MIT - see LICENSE file in the repository
