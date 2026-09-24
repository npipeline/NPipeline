---
title: "AWS SQS Connector"
description: "Consume from and send to Amazon SQS queues with long polling, batch operations, and visibility timeout."
order: 18
---

# AWS SQS Connector

The `NPipeline.Connectors.Aws.Sqs` package provides source and sink nodes for [Amazon SQS](https://aws.amazon.com/sqs/). Supports long polling, configurable visibility timeout, batch send/receive/delete, message attributes, and multiple acknowledgment strategies.

## Installation

```bash
dotnet add package NPipeline.Connectors.Aws.Sqs
```

**Dependencies:** [AWSSDK.SQS](https://www.nuget.org/packages/AWSSDK.SQS) 4.x, [AWSSDK.Extensions.NETCore.Setup](https://www.nuget.org/packages/AWSSDK.Extensions.NETCore.Setup) 4.x

## Source Node - `SqsSourceNode<T>`

### Constructors

```csharp
public SqsSourceNode(SqsConfiguration configuration)

// Bring your own client
public SqsSourceNode(IAmazonSQS sqsClient, SqsConfiguration configuration)
```

### Example

```csharp
var config = new SqsConfiguration
{
    SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789/orders",
    Region = "us-east-1",
    MaxNumberOfMessages = 10,
    WaitTimeSeconds = 20,       // long polling
    VisibilityTimeout = 60,
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess
};

var source = new SqsSourceNode<Order>(config);
```

## Sink Node - `SqsSinkNode<T>`

### Constructors

```csharp
public SqsSinkNode(SqsConfiguration configuration)

// Bring your own client
public SqsSinkNode(IAmazonSQS sqsClient, SqsConfiguration configuration)
```

### Example

```csharp
var config = new SqsConfiguration
{
    SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789/processed-orders",
    Region = "us-east-1",
    BatchSize = 10,
    DelaySeconds = 0
};

var sink = new SqsSinkNode<ProcessedOrder>(config);
```

## AWS Credentials

The connector resolves credentials in this order:

1. **Explicit credentials** - `AccessKeyId` + `SecretAccessKey` in configuration
2. **Named profile** - `ProfileName` in configuration
3. **Default credential chain** - environment variables, instance profile, etc.

```csharp
// Explicit credentials (development only - use IAM roles in production)
var config = new SqsConfiguration
{
    AccessKeyId = "AKIA...",
    SecretAccessKey = "...",
    Region = "us-east-1",
    SourceQueueUrl = "..."
};

// Named profile
var config = new SqsConfiguration
{
    ProfileName = "my-profile",
    Region = "us-east-1",
    SourceQueueUrl = "..."
};

// Default chain (recommended for production - EC2 instance role, ECS task role, etc.)
var config = new SqsConfiguration
{
    Region = "us-east-1",
    SourceQueueUrl = "..."
};
```

## Configuration

### AWS

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `AccessKeyId` | `string?` | `null` | AWS access key ID |
| `SecretAccessKey` | `string?` | `null` | AWS secret access key |
| `Region` | `string` | `"us-east-1"` | AWS region |
| `ProfileName` | `string?` | `null` | AWS credential profile |

### Queue

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `SourceQueueUrl` | `string` | - | Source queue URL |
| `SinkQueueUrl` | `string` | - | Sink queue URL |

### Polling (Source)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxNumberOfMessages` | `int` | `10` | Messages per receive (1–10) |
| `WaitTimeSeconds` | `int` | `20` | Long polling wait (0–20 seconds) |
| `VisibilityTimeout` | `int` | `30` | Visibility timeout (seconds) |
| `PollingIntervalMs` | `int` | `1000` | Interval between polls (ms) |

### Batching (Sink)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `BatchSize` | `int` | `10` | Messages per send batch (1–10) |
| `DelaySeconds` | `int` | `0` | Message delivery delay |

### Acknowledgment

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `AcknowledgmentStrategy` | `AcknowledgmentStrategy` | `AutoOnSinkSuccess` | `AutoOnSinkSuccess`, `Manual`, or `Delayed` |
| `AcknowledgmentDelayMs` | `int` | `5000` | Delay before acknowledging (`Delayed` strategy) |

### JSON Serialization

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `PropertyNamingPolicy` | `JsonPropertyNamingPolicy` | `CamelCase` | `CamelCase`, `PascalCase`, `Snake_case`, `Kebab-case` |
| `PropertyNameCaseInsensitive` | `bool` | `true` | Case-insensitive deserialization |

### Error Handling

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RetryMode` | `RequestRetryMode?` | `Standard` | AWS SDK retry mode. See [Retries](#retries) |
| `MaxErrorRetry` | `int?` | `3` | AWS SDK retries per call. See [Retries](#retries) |
| `ContinueOnError` | `bool` | `true` | Continue on errors |
| `MessageErrorHandler` | `Func<...>?` | `null` | Custom error handler |

## Retries

The connector leaves retries to the AWS SDK. The SDK identifies throttling and transient SQS errors, applies jittered backoff, and uses a retry quota to prevent flooding failing endpoints. The nodes don't
retry on top of it: an exception that reaches a node has already used up the SDK's retries, and it fails the node.

Two settings configure the SDK client that the nodes create:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `RetryMode` | `RequestRetryMode?` | `Standard` | `Standard` retries throttling, 5xx, and network errors with jittered exponential backoff. `Adaptive` also slows the client down when SQS throttles it. `null` lets the SDK decide (`AWS_RETRY_MODE` or the shared config file) |
| `MaxErrorRetry` | `int?` | `3` | Retries per call, not total attempts. The default makes up to four attempts. `0` turns retries off. `null` lets the SDK decide (`AWS_MAX_ATTEMPTS`, the shared config file, or the SDK default of 2) |

If you pass your own `IAmazonSQS` to a node's constructor, the connector can't reconfigure it, and these two settings
are ignored. Set the retry behavior on the client's own configuration:

```csharp
var client = new AmazonSQSClient(new AmazonSQSConfig
{
    RegionEndpoint = RegionEndpoint.USEast1,
    RetryMode = RequestRetryMode.Standard,
    MaxErrorRetry = 3,
});

var source = new SqsSourceNode<Order>(client, configuration);
```

`AcknowledgmentDelayMs` and `PollingIntervalMs` are pacing, not retry settings, and are unaffected.

SQS standard queues deliver at least once, and a retried `SendMessage` whose first attempt reached SQS can enqueue the
message twice. Use a FIFO queue with a deduplication ID, or make consumers idempotent, if duplicates matter.

## Next Steps

- [Azure Service Bus Connector](azure-service-bus.md) - Azure managed messaging
- [Kafka Connector](kafka.md) - distributed streaming platform
- [RabbitMQ Connector](rabbitmq.md) - self-managed message broker

## Acknowledgment Strategies

| Strategy | Description |
|----------|-------------|
| `AutoOnSinkSuccess` (default) | Message deleted after successful sink processing |
| `Manual` | Call `message.AcknowledgeAsync()` explicitly |
| `Delayed` | Delete after `AcknowledgmentDelayMs` (allows downstream confirmation) |
| `None` | No acknowledgment - message reappears after `VisibilityTimeout` |

### Manual Acknowledgment

```csharp
pipeline.AddTransform<SqsMessage<Order>, ProcessedOrder>(async (msg, ct) =>
{
    var result = Process(msg.Body);
    await msg.AcknowledgeAsync(ct); // deletes from queue
    return result;
});
```

### Batch Acknowledgment

When using `AutoOnSinkSuccess` with batch receive (`MaxNumberOfMessages > 1`), messages are deleted in batch using `DeleteMessageBatch` for efficiency.

## `SqsMessage<T>` Wrapper

Source nodes emit `SqsMessage<T>` which exposes SQS metadata:

| Property | Type | Description |
|----------|------|-------------|
| `Body` | `T` | Deserialized message body |
| `MessageId` | `string` | SQS message ID |
| `ReceiptHandle` | `string` | Receipt handle (for acknowledgment) |
| `MessageAttributes` | `Dictionary<string, MessageAttribute>` | Custom message attributes |
| `ApproximateReceiveCount` | `int` | Number of times message has been received |
| `SentTimestamp` | `DateTimeOffset` | When message was sent |

## Dead-Letter Queue

Configure a DLQ in SQS (not in the connector). After `maxReceiveCount` deliveries, SQS automatically moves the message to the DLQ. Use the connector to process DLQ messages:

```csharp
var dlqSource = new SqsSourceNode<SqsMessage<Order>>(new SqsConfiguration
{
    SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789/orders-dlq",
    MaxNumberOfMessages = 10
});
```

## Best Practices

1. **Use long polling** (`WaitTimeSeconds = 20`) - reduces empty responses and cost
2. **Set `VisibilityTimeout` > processing time** - prevents duplicate processing
3. **Use IAM roles** for credentials in production (EC2 instance role, ECS task role)
4. **Batch operations** - SQS charges per request, batching reduces cost
5. **Configure a DLQ** on the SQS queue for poison messages
6. **Monitor `ApproximateReceiveCount`** to detect stuck messages
7. **Use FIFO queues** when message ordering matters (set `MessageGroupId`)
