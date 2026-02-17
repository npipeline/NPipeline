---
title: Kafka Connector
description: Read from and write to Apache Kafka with NPipeline using the Kafka connector with support for multiple serialization formats and delivery semantics.
sidebar_position: 5
---

## Kafka Connector

The `NPipeline.Connectors.Kafka` package provides specialized source and sink nodes for working with Apache Kafka. This allows you to easily integrate Kafka message streams into your pipelines as an input source or an output destination.

This connector uses the robust [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) library, providing high-throughput streaming with support for Avro and Protocol Buffer serialization formats, Schema Registry integration, flexible delivery semantics, and comprehensive error handling.

## Installation

To use the Kafka connector, install the `NPipeline.Connectors.Kafka` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Kafka
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## Configuration

The Kafka connector uses a `KafkaConfiguration` object to configure both source and sink nodes. This centralized configuration simplifies setup and ensures consistency across your pipeline.

### Connection Settings

Every Kafka configuration requires a **bootstrap server** to establish the initial connection to your Kafka cluster:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",  // or comma-separated list: "broker1:9092,broker2:9092"
    ClientId = "my-app-client",           // Optional: identifies your client to the broker
};
```

### Authentication

The connector supports SASL authentication for secure connections to managed Kafka services:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "kafka.example.com:9092",
    SecurityProtocol = SecurityProtocol.SaslSsl,
    SaslMechanism = SaslMechanism.Plain,
    SaslUsername = "username",
    SaslPassword = "password",
};
```

**Supported authentication methods:**

- **PLAINTEXT**: No authentication (development only)
- **SASL_PLAINTEXT**: SASL with plain text (not recommended for production)
- **SASL_SSL**: SASL with TLS encryption (recommended for production)
- **SSL**: TLS encryption without SASL

## `KafkaSourceNode<T>`

The `KafkaSourceNode<T>` continuously consumes messages from a Kafka topic and emits each deserialized message as a `KafkaMessage<T>`.

### Source Configuration

A source requires a topic to consume from and a consumer group ID:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SourceTopic = "orders",
    ConsumerGroupId = "order-processing-group",
    AutoOffsetReset = AutoOffsetReset.Latest,  // Start from latest for new consumers
    MaxPollRecords = 500,                       // Batch size
};
```

### Configuration Options for Consumption

- **`SourceTopic`**: The Kafka topic to consume from (required)
- **`ConsumerGroupId`**: Consumer group for offset management and parallel processing (required)
- **`AutoOffsetReset`**: Behavior when no committed offset exists:
  - `Latest`: Start from the most recent message (default)
  - `Earliest`: Start from the beginning of the topic
- **`EnableAutoCommit`**: Automatically commit offsets (default: `false` - use manual acknowledgment)
- **`MaxPollRecords`**: Maximum messages to fetch per poll (default: 500)
- **`PollTimeoutMs`**: Consumer poll timeout in milliseconds (default: 100)
- **`FetchMinBytes`**: Minimum bytes to accumulate before responding (default: 1)
- **`FetchMaxBytes`**: Maximum bytes to fetch per request (default: 50MB)
- **`MaxPartitionFetchBytes`**: Maximum bytes per partition (default: 1MB)
- **`GroupInstanceId`**: Enable static group membership (optional, for development)

### Message Acknowledgment

For **at-least-once** delivery, calling `AcknowledgeAsync()` commits the offset. For **exactly-once**, `AcknowledgeAsync()` is a no-op (offsets are sent to the transaction by the sink).

You can acknowledge manually in a sink, or let `KafkaSinkNode<T>` acknowledge automatically when `AcknowledgmentStrategy` is `AutoOnSinkSuccess` (the default).

```csharp
var source = builder.AddSource(new KafkaSourceNode<Order>(config), "kafka-source");

var sink = builder.AddSink(async (KafkaMessage<Order> message, CancellationToken ct) =>
{
    await ProcessOrderAsync(message.Body, ct);
    await message.AcknowledgeAsync(ct); // At-least-once: commits offset
}, "process-order");

builder.Connect(source, sink);
```

### Example: Reading from a Kafka Topic

```csharp
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record Order(string OrderId, string CustomerId, decimal Amount, DateTime CreatedAt);

public sealed class KafkaConsumerPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "orders",
            ConsumerGroupId = "order-processing-group",
            AutoOffsetReset = AutoOffsetReset.Latest,
        };

        var source = new KafkaSourceNode<Order>(config);

        var sourceHandle = builder.AddSource(source, "kafka-source");
        var sinkHandle = builder.AddSink(async (KafkaMessage<Order> message, CancellationToken ct) =>
        {
            Console.WriteLine($"Processing order: {message.Body.OrderId} for {message.Body.CustomerId}");
            await message.AcknowledgeAsync(ct);
        }, "process-order");

        builder.Connect(sourceHandle, sinkHandle);
    }
}
```

## `KafkaSinkNode<T>`

The `KafkaSinkNode<T>` produces messages to a Kafka topic. It supports batching, idempotent production, and transactional delivery for exactly-once semantics.

### Sink Configuration

A sink requires a topic to produce to:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SinkTopic = "order-events",
    EnableIdempotence = true,      // Prevent duplicate messages
    Acks = Acks.All,               // Wait for all replicas to acknowledge
    BatchSize = 100,               // Messages per batch
    LingerMs = 5,                  // Time to wait for batching (ms)
    BatchLingerMs = 100,           // Pipeline batching delay (ms)
};
```

### Configuration Options for Production

- **`SinkTopic`**: The Kafka topic to produce to (required)
- **`EnableIdempotence`**: Ensure messages aren't duplicated (default: `true`)
- **`Acks`**: Acknowledgment mode:
  - `None`: No acknowledgment (fastest, least safe)
  - `Leader`: Leader acknowledged (faster, good durability)
  - `All`: All in-sync replicas acknowledged (slowest, most durable)
- **`BatchSize`**: Maximum messages per batch (default: 16384)
- **`LingerMs`**: Time to wait for batch accumulation (default: 5ms)
- **`BatchLingerMs`**: Pipeline batch linger time (default: 100ms)
- **`CompressionType`**: Message compression (`None`, `Gzip`, `Snappy`, `Lz4`, `Zstd`)
- **`MessageMaxBytes`**: Maximum message size (default: 1MB)
- **`AcknowledgmentStrategy`**: Controls auto-acknowledgment for `IAcknowledgableMessage` items
- **`EnableTransactions`**: Enables transactional production (required for exactly-once)
- **`TransactionalId`**: Transactional ID required when `EnableTransactions` is `true`
- **`TransactionInitTimeoutMs`**: Transaction initialization timeout in milliseconds (default: 30000)

### Example: Writing to a Kafka Topic

```csharp
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record OrderEvent(string OrderId, string EventType, DateTime Timestamp);

public sealed class KafkaProducerPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var orders = new[]
        {
            new OrderEvent("ORD-001", "Created", DateTime.UtcNow),
            new OrderEvent("ORD-002", "Shipped", DateTime.UtcNow),
        };

        var config = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SinkTopic = "order-events",
            Acks = Acks.All,
        };

        var sink = new KafkaSinkNode<OrderEvent>(config);

        var sourceHandle = builder.AddSource(() => orders, "orders-source");
        var sinkHandle = builder.AddSink(sink, "kafka-sink");

        builder.Connect(sourceHandle, sinkHandle);
    }
}
```

## Serialization Formats

The Kafka connector supports multiple serialization formats to match your data models and infrastructure:

### JSON Serialization (Default)

JSON is the default format, requiring no Schema Registry configuration:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SerializationFormat = SerializationFormat.Json,
    // No Schema Registry configuration needed
};
```

JSON is ideal for:

- Development and testing
- Simple data structures
- When schema evolution is not critical

### Apache Avro

Avro provides schema evolution and compact serialization. Requires Schema Registry:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SerializationFormat = SerializationFormat.Avro,
    SchemaRegistry = new SchemaRegistryConfiguration
    {
        Url = "http://localhost:8081",
        BasicAuthUsername = "username",      // Optional
        BasicAuthPassword = "password",      // Optional
        RequestTimeoutMs = 30000,
        SchemaCacheCapacity = 1000,
        AutoRegisterSchemas = true,
    },
};
```

Avro is ideal for:

- Enterprise environments with Schema Registry
- Strong schema validation requirements
- Maximum binary efficiency

### Protocol Buffers

Protocol Buffers offer efficient serialization with Schema Registry support:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SerializationFormat = SerializationFormat.Protobuf,
    SchemaRegistry = new SchemaRegistryConfiguration
    {
        Url = "http://localhost:8081",
    },
};
```

Protocol Buffers are ideal for:

- Polyglot systems (Python, Java, Go, etc.)
- Performance-critical applications
- Schema versioning with backward compatibility

## Delivery Semantics

The connector supports different delivery guarantees to balance consistency and performance:

### At-Least-Once (Default)

Messages are delivered at least once; duplicates are possible:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    DeliverySemantic = DeliverySemantic.AtLeastOnce,
};
```

**When to use:**

- Most applications where idempotency can be handled downstream
- Higher throughput is important
- Duplicate processing is acceptable or idempotent

### Exactly-Once

Guaranteed exactly-once delivery using Kafka transactions:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    DeliverySemantic = DeliverySemantic.ExactlyOnce,
    EnableTransactions = true,
    TransactionalId = "order-processor-1",
    EnableIdempotence = true,  // Required for exactly-once
    Acks = Acks.All,           // Required for exactly-once
};
```

**When to use:**

- Financial transactions
- Inventory management
- Any scenario where duplicates are unacceptable
- **Note:** Exactly-once semantics require `EnableTransactions = true` and a `TransactionalId`. Offsets are sent to the transaction only when the sink processes `KafkaMessage<T>` items from the Kafka source.

## Partitioning

For sink nodes, messages can be distributed across partitions using a partition key provider. By default, the key provider uses `ToString()` on the message and lets the Kafka client choose the partition based on the key.

```csharp
public record Order(string OrderId, string CustomerId, decimal Amount);

// Custom partition key provider based on CustomerId
public sealed class CustomerPartitionKeyProvider : IPartitionKeyProvider<Order>
{
    public string GetPartitionKey(Order item)
    {
        return item.CustomerId;  // Orders from same customer go to same partition
    }
}

var config = new KafkaConfiguration { /* ... */ };
var sink = new KafkaSinkNode<Order>(
    config,
    metrics: NullKafkaMetrics.Instance,
    retryStrategy: new ExponentialBackoffRetryStrategy(),
    partitionKeyProvider: new CustomerPartitionKeyProvider()
);

// To target a specific partition directly, implement IPartitionKeyProvider.GetPartition.
```

## Metadata and Message Properties

Both source and sink nodes track Kafka-specific metadata through `KafkaMessage<T>`:

```csharp
var sourceHandle = builder.AddSource(source, "kafka-source");
var sinkHandle = builder.AddSink(async (KafkaMessage<Order> message, CancellationToken ct) =>
{
    var topic = message.Topic;           // Topic name
    var partition = message.Partition;   // Partition number
    var offset = message.Offset;         // Message offset
    var key = message.Key;               // Message key
    var timestamp = message.Timestamp;   // Message timestamp
    var headers = message.Headers;       // Message headers

    Console.WriteLine($"Message: {topic}[{partition}] offset={offset}");

    await message.AcknowledgeAsync(ct);
}, "inspect-message");

builder.Connect(sourceHandle, sinkHandle);
```

## Dead Letter Handling

You can model dead-letter messages with `DeadLetterEnvelope` and send them to a dedicated topic using `KafkaSinkNode<T>`:

```csharp
var deadLetterEnvelope = new DeadLetterEnvelope
{
    OriginalTopic = message.Topic,
    Partition = message.Partition,
    Offset = message.Offset,
    OriginalItem = message.Body,
    ExceptionType = exception.GetType().FullName,
    ExceptionMessage = exception.Message,
    StackTrace = exception.StackTrace,
    Timestamp = DateTime.UtcNow,
    CorrelationId = context.CorrelationId,
};

// Produce to dead letter topic
var deadLetterSink = new KafkaSinkNode<DeadLetterEnvelope>(
    new KafkaConfiguration
    {
        BootstrapServers = "localhost:9092",
        SinkTopic = "order-processing-dead-letters",
    }
);
```

## Error Handling and Retries

The connector includes built-in retry strategies for transient errors:

```csharp
var retryStrategy = new ExponentialBackoffRetryStrategy
{
    BaseDelayMs = 100,
    MaxDelayMs = 5000,
    MaxRetries = 5,
    JitterFactor = 0.2,
};

var source = new KafkaSourceNode<Order>(
    config,
    metrics: NullKafkaMetrics.Instance,
    retryStrategy: retryStrategy
);
```

Common transient errors (broker temporarily unavailable, network issues) are automatically retried with exponential backoff.

## Best Practices

### Consumer Groups

Always specify a unique `ConsumerGroupId` for parallel processing:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SourceTopic = "orders",
    ConsumerGroupId = "order-processing-v1",  // Increment version on breaking changes
};
```

### Offset Management

Prefer manual acknowledgment for precise control:

```csharp
config.EnableAutoCommit = false;  // Default: manual acknowledgment

// Commit only after successful processing
await message.AcknowledgeAsync();
```

### Batch Tuning

Adjust batch settings based on your throughput requirements:

```csharp
// High-throughput scenario
var config = new KafkaConfiguration
{
    MaxPollRecords = 1000,
    BatchLingerMs = 100,         // Wait up to 100ms to accumulate messages
};

// Low-latency scenario
var config = new KafkaConfiguration
{
    MaxPollRecords = 10,
    BatchLingerMs = 10,          // Process quickly
};
```

### Monitoring and Metrics

Implement `IKafkaMetrics` to track performance:

```csharp
public sealed class PrometheusKafkaMetrics : IKafkaMetrics
{
    public void RecordConsumed(string topic, int count) { /* ... */ }
    public void RecordProduced(string topic, int count) { /* ... */ }
    public void RecordProduceError(string topic, Exception ex) { /* ... */ }
    public void RecordPollLatency(string topic, TimeSpan latency) { /* ... */ }
    public void RecordCommitLatency(string topic, TimeSpan latency) { /* ... */ }
    public void RecordSerializeLatency(Type type, TimeSpan latency) { /* ... */ }
    public void RecordDeserializeLatency(Type type, TimeSpan latency) { /* ... */ }
    public void RecordSerializeError(Type type, Exception ex) { /* ... */ }
    public void RecordDeserializeError(Type type, Exception ex) { /* ... */ }
    public void RecordBatchSize(string topic, int size) { /* ... */ }
    public void RecordLag(string topic, int partition, long lag) { /* ... */ }
    public void RecordTransactionCommit(TimeSpan latency) { /* ... */ }
    public void RecordTransactionAbort(TimeSpan latency) { /* ... */ }
    public void RecordTransactionError(Exception ex) { /* ... */ }
}

var source = new KafkaSourceNode<Order>(
    config,
    metrics: new PrometheusKafkaMetrics(),
    retryStrategy: new ExponentialBackoffRetryStrategy()
);
```

## Complete Example: End-to-End Pipeline

```csharp
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record OrderIn(string OrderId, string CustomerId, decimal Amount);
public sealed record OrderProcessed(string OrderId, string CustomerId, decimal Amount, decimal Tax);

public sealed class OrderProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceConfig = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "orders",
            ConsumerGroupId = "order-processor",
            AutoOffsetReset = AutoOffsetReset.Latest,
        };

        var sinkConfig = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SinkTopic = "orders-processed",
            Acks = Acks.All,
        };

        var source = new KafkaSourceNode<OrderIn>(sourceConfig);
        var sink = new KafkaSinkNode<OrderProcessed>(sinkConfig);

        var sourceHandle = builder.AddSource(source, "orders-source");
        var transformHandle = builder.AddTransform((KafkaMessage<OrderIn> message) =>
        {
            var order = message.Body;
            var tax = order.Amount * 0.1m;
            return new OrderProcessed(order.OrderId, order.CustomerId, order.Amount, tax);
        }, "calculate-tax");
        var sinkHandle = builder.AddSink(sink, "orders-sink");

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}
```

For more information on building pipelines, see the [Core Concepts](../core-concepts/index.md) and [Getting Started](../getting-started/index.md) guides.
