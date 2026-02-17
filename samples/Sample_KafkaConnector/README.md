# Kafka Connector Sample

This sample demonstrates how to use the NPipeline Kafka Connector for high-performance message processing with Apache Kafka.

## Prerequisites

- .NET 8.0 SDK or later
- Docker and Docker Compose (for local Kafka setup)

## Quick Start

### 1. Start Kafka Infrastructure

```bash
docker-compose up -d
```

This starts:

- **Zookeeper** (port 2181) - Kafka coordination service
- **Kafka Broker** (port 9092) - Message broker
- **Schema Registry** (port 8081) - For Avro/Protobuf serialization
- **Kafka UI** (port 9000) - Web interface for monitoring topics and messages

Wait for all services to be healthy (typically 30-60 seconds).

### 2. Verify Topics Are Created

The `kafka-init` container automatically creates the required topics:

- `input-events` - Source topic (3 partitions)
- `output-events` - Sink topic (3 partitions)
- `dead-letter-events` - Dead letter topic (1 partition)

Check topics:

```bash
docker exec kafka-broker kafka-topics --bootstrap-server localhost:9092 --list
```

### 3. Run the Sample

```bash
dotnet run
```

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  input-events   │────▶│   NPipeline      │────▶│  output-events  │
│   (Kafka)       │     │   KafkaSource    │     │   (Kafka)       │
│   3 partitions  │     │   KafkaSink      │     │   3 partitions  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                │
                                ▼
                        ┌─────────────────┐
                        │ dead-letter     │
                        │   (on errors)   │
                        └─────────────────┘
```

## Configuration Options

### Basic Configuration

```csharp
var config = new KafkaConfiguration
{
    // Connection
    BootstrapServers = "localhost:9092",
    ClientId = "my-application",
    
    // Source (Consumer)
    SourceTopic = "input-events",
    ConsumerGroupId = "my-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,
    
    // Sink (Producer)
    SinkTopic = "output-events",
    EnableIdempotence = true,
    BatchSize = 100,
    LingerMs = 10,
    Acks = Acks.All,
    
    // Delivery Semantics
    DeliverySemantic = DeliverySemantic.AtLeastOnce,
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
};
```

> **Note:** This sample enables auto-commit for simplicity. For manual offset control, set `EnableAutoCommit = false` and acknowledge messages explicitly.

### Exactly-Once Semantics

For exactly-once processing with transactions:

```csharp
var config = new KafkaConfiguration
{
    BootstrapServers = "localhost:9092",
    SourceTopic = "input-events",
    SinkTopic = "output-events",
    ConsumerGroupId = "my-consumer-group",
    
    // Enable transactions
    EnableTransactions = true,
    TransactionalId = "my-transactional-producer",
    EnableIdempotence = true,  // Required for transactions
    
    // Must use ExactlyOnce semantic
    DeliverySemantic = DeliverySemantic.ExactlyOnce,
};

// Validates transaction configuration
config.ValidateTransactions();
```

## Custom Partitioning

Control how messages are distributed across partitions:

```csharp
// Partition by a specific property
var partitionProvider = PartitionKeyProvider
    .FromProperty<OrderMessage, string>(m => m.CustomerId);

var sinkNode = new KafkaSinkNode<OrderMessage>(
    config, 
    metrics, 
    retryStrategy,
    partitionProvider);  // Messages with same CustomerId go to same partition
```

## Metrics and Observability

Implement `IKafkaMetrics` to collect metrics:

```csharp
public class MyKafkaMetrics : IKafkaMetrics
{
    public void IncrementMessagesConsumed(string topic, int count)
    {
        // Track consumed messages
        Metrics.Counter("kafka_messages_consumed").Increment(count);
    }
    
    public void IncrementMessagesProduced(string topic, int count)
    {
        // Track produced messages
        Metrics.Counter("kafka_messages_produced").Increment(count);
    }
    
    public void RecordConsumerLag(string topic, string partition, long lag)
    {
        // Track consumer lag
        Metrics.Gauge("kafka_consumer_lag").Set(lag);
    }
    
    // ... other methods
}
```

## Retry Strategy

Configure custom retry behavior:

```csharp
var retryStrategy = new ExponentialBackoffRetryStrategy
{
    MaxRetries = 5,
    BaseDelayMs = 100,
    MaxDelayMs = 30000,
    JitterFactor = 0.2,  // Add 20% randomness to prevent thundering herd
};
```

## Dead Letter Handling

Failed messages are sent to a dead-letter topic:

```csharp
var deadLetterSink = new KafkaDeadLetterSink(
    producer,
    "dead-letter-events");

// Messages include original topic, partition, offset, and error details
```

## Monitoring

### Kafka UI

Access the web interface at <http://localhost:9000> to:

- View topics and partitions
- Browse messages
- Monitor consumer groups
- View schemas

### Command Line

Check consumer group lag:

```bash
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group sample-consumer-group
```

## Stopping the Infrastructure

```bash
docker-compose down -v  # -v removes volumes for clean slate
```

## Troubleshooting

### Connection Refused

Wait for Kafka to be fully started:

```bash
docker-compose logs kafka | grep "started"
```

### Consumer Group Issues

Reset consumer group offset:

```bash
docker exec kafka-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group sample-consumer-group \
  --reset-offsets \
  --to-earliest \
  --topic input-events \
  --execute
```

### Transaction Errors

Ensure:

- `EnableIdempotence = true` when using transactions
- `DeliverySemantic = ExactlyOnce` when using transactions
- `TransactionalId` is unique per producer instance
