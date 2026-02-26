---
title: AWS SQS Connector
description: Read from and write to Amazon Simple Queue Service (SQS) with NPipeline using the AWS SQS connector.
sidebar_position: 6
---

## AWS SQS Connector

The `NPipeline.Connectors.Aws.Sqs` package provides specialized source and sink nodes for working with Amazon Simple Queue Service (SQS). This allows you to easily integrate SQS message processing into your pipelines as an input source or an output destination.

This connector uses the robust [AWSSDK.SQS](https://docs.aws.amazon.com/sdk-for-net/) library under the hood, so it is powerful and highly configurable for production workloads.

## Installation

To use the AWS SQS connector, install the `NPipeline.Connectors.Aws.Sqs` NuGet package:

```bash
dotnet add package NPipeline.Connectors.Aws.Sqs
```

For the core NPipeline package and other available extensions, see the [Installation Guide](../getting-started/installation.md).

## AWS Credentials

The SQS connector supports multiple credential methods to provide flexibility across different deployment scenarios:

### Access Key and Secret Key

```csharp
var config = new SqsConfiguration
{
    AccessKeyId = "AKIAIOSFODNN7EXAMPLE",
    SecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    Region = "us-east-1"
};
```

### AWS Profile

```csharp
var config = new SqsConfiguration
{
    ProfileName = "default",
    Region = "us-east-1"
};
```

### Default Credential Chain

```csharp
var config = new SqsConfiguration
{
    Region = "us-east-1"
};
```

**Why multiple credential methods:** The default credential chain provides the most flexibility for deployment scenarios (EC2, ECS, Lambda), while explicit credentials are useful for local development and testing. The connector automatically falls back to the default AWS credential chain when no explicit credentials are provided.

## `SqsSourceNode<T>`

The `SqsSourceNode<T>` continuously polls an SQS queue and emits each deserialized message as an item of type `SqsMessage<T>`.

### Source Configuration

The constructor for `SqsSourceNode<T>` takes configuration for connecting to SQS:

```csharp
public SqsSourceNode(SqsConfiguration configuration)
public SqsSourceNode(IAmazonSQS sqsClient, SqsConfiguration configuration)
```

- **`configuration`**: The [`SqsConfiguration`](../../../src/NPipeline.Connectors.Aws.Sqs/Configuration/SqsConfiguration.cs:10) object with queue URL, polling settings, and AWS credentials.
- **`sqsClient`**: *(Optional)* A custom `IAmazonSQS` client. If not provided, one is created from the configuration.

### Example: Reading from an SQS Queue

```csharp
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record OrderMessage(string OrderId, string CustomerId, decimal Amount, DateTime OrderDate);

public sealed class SqsReaderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var config = new SqsConfiguration
        {
            Region = "us-east-1",
            SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
            MaxNumberOfMessages = 10,
            WaitTimeSeconds = 20,
            VisibilityTimeout = 30
        };

        var sourceNode = new SqsSourceNode<OrderMessage>(config);
        var source = builder.AddSource(sourceNode, "sqs_source");
        var sink = builder.AddSink<ConsoleSinkNode, SqsMessage<OrderMessage>>("console_sink");

        builder.Connect(source, sink);
    }
}

public sealed class ConsoleSinkNode : SinkNode<SqsMessage<OrderMessage>>
{
    public override async Task ExecuteAsync(
        IDataPipe<SqsMessage<OrderMessage>> input,
        PipelineContext context,
        IPipelineActivity parentActivity,
        CancellationToken cancellationToken)
    {
        await foreach (var message in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Received: {message.Body}");
        }
    }
}
```

### Polling Configuration

The source node supports configurable polling behavior:

| Property | Type | Default | Description |
|-----------|------|---------|-------------|
| `SourceQueueUrl` | `string` | Required | SQS queue URL to poll from |
| `MaxNumberOfMessages` | `int` | `10` | Maximum messages per poll (1-10, SQS API limit) |
| `WaitTimeSeconds` | `int` | `20` | Long polling wait time (0-20 seconds) |
| `VisibilityTimeout` | `int` | `30` | Message visibility timeout in seconds |
| `PollingIntervalMs` | `int` | `1000` | Polling interval when queue is empty (milliseconds) |

**Why long polling:** Long polling (`WaitTimeSeconds > 0`) reduces cost and empty responses by keeping the request open until messages arrive or the timeout expires. The default of 20 seconds maximizes cost efficiency by minimizing the number of empty poll requests.

## `SqsSinkNode<T>`

The `SqsSinkNode<T>` writes items from the pipeline to an SQS queue by serializing them to JSON.

### Sink Configuration

The constructor for `SqsSinkNode<T>` takes configuration for connecting to SQS:

```csharp
public SqsSinkNode(SqsConfiguration configuration)
public SqsSinkNode(IAmazonSQS sqsClient, SqsConfiguration configuration)
```

- **`configuration`**: The [`SqsConfiguration`](../../../src/NPipeline.Connectors.Aws.Sqs/Configuration/SqsConfiguration.cs:10) object with queue URL, batch settings, and AWS credentials.
- **`sqsClient`**: *(Optional)* A custom `IAmazonSQS` client. If not provided, one is created from the configuration.

### Example: Writing to an SQS Queue

```csharp
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed record ProcessedOrder(string OrderId, string CustomerId, decimal Amount, bool IsValid);

public sealed class SqsWriterPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var config = new SqsConfiguration
        {
            Region = "us-east-1",
            SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/processed-orders",
            BatchSize = 10
        };

        var source = builder.AddSource<InMemorySourceNode<ProcessedOrder>, ProcessedOrder>("source");
        var sinkNode = new SqsSinkNode<ProcessedOrder>(config);
        var sink = builder.AddSink(sinkNode, "sqs_sink");

        builder.Connect(source, sink);
    }
}
```

### Sending Configuration

The sink node supports configurable sending behavior:

| Property | Type | Default | Description |
|-----------|------|---------|-------------|
| `SinkQueueUrl` | `string` | Required | SQS queue URL to send messages to |
| `BatchSize` | `int` | `10` | Batch size for sending messages (1-10) |
| `DelaySeconds` | `int` | `0` | Message delivery delay (0-900 seconds) |
| `MessageAttributes` | `IDictionary<string, MessageAttributeValue>?` | `null` | Message attributes to add to all outgoing messages |

## Acknowledgment Strategies

The SQS connector provides multiple acknowledgment strategies to handle different processing scenarios:

### AutoOnSinkSuccess (Default)

Messages are automatically acknowledged immediately after successful sink processing:

```csharp
var config = new SqsConfiguration
{
    SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/input-queue",
    SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/output-queue",
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess
};
```

**Why this is default:** Provides the best developer experience with automatic message cleanup after successful processing, reducing the risk of duplicate message handling.

### Manual

Messages are sent to the sink but not acknowledged. You must manually call [`AcknowledgeAsync()`](../../../src/NPipeline.Connectors.Aws.Sqs/Models/SqsMessage.cs:107):

```csharp
var config = new SqsConfiguration
{
    AcknowledgmentStrategy = AcknowledgmentStrategy.Manual
};

// In a transform node
public class ManualAckTransform : ITransformNode<SqsMessage<OrderMessage>, SqsMessage<OrderMessage>>
{
    public async Task<SqsMessage<OrderMessage>> ExecuteAsync(
        SqsMessage<OrderMessage> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Process the message
        var processed = ProcessOrder(input.Body);
        
        // Manually acknowledge when ready
        await input.AcknowledgeAsync(cancellationToken);
        
        return input.WithBody(processed);
    }
}
```

**When to use:** Use when you need fine-grained control over acknowledgment timing, such as when processing depends on external systems with their own transaction boundaries.

### Delayed

Messages are acknowledged after a configurable delay:

```csharp
var config = new SqsConfiguration
{
    AcknowledgmentStrategy = AcknowledgmentStrategy.Delayed,
    AcknowledgmentDelayMs = 5000 // 5 seconds
};
```

**When to use:** Useful when downstream systems need time to process messages before they are removed from the queue, providing a window for recovery if processing fails.

### None

Messages are never acknowledged automatically:

```csharp
var config = new SqsConfiguration
{
    AcknowledgmentStrategy = AcknowledgmentStrategy.None
};
```

**Warning:** Messages remain in the queue until their visibility timeout expires and become available for reprocessing. Use with caution.

## Batch Acknowledgment

Batch acknowledgment improves performance by reducing the number of SQS API calls:

```csharp
var config = new SqsConfiguration
{
    SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/input-queue",
    SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/output-queue",
    AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
    
    BatchAcknowledgment = new BatchAcknowledgmentOptions
    {
        // Maximum messages per batch (1-10)
        BatchSize = 10,
        
        // Maximum wait time before flushing partial batch (milliseconds)
        FlushTimeoutMs = 1000,
        
        // Enable automatic batching
        EnableAutomaticBatching = true,
        
        // Maximum concurrent batch operations
        MaxConcurrentBatches = 3
    }
};
```

**Why batch acknowledgment:** Reduces SQS API calls and costs by acknowledging multiple messages in a single `DeleteMessageBatch` operation. The timeout-based flush ensures messages are acknowledged even when the batch size is not reached.

### Batch Acknowledgment Options

| Property | Type | Default | Description |
|-----------|------|---------|-------------|
| `BatchSize` | `int` | `10` | Maximum messages per batch (1-10) |
| `FlushTimeoutMs` | `int` | `1000` | Maximum wait before flushing partial batch (milliseconds) |
| `EnableAutomaticBatching` | `bool` | `true` | Enable automatic batch acknowledgment |
| `MaxConcurrentBatches` | `int` | `3` | Maximum concurrent batch operations |

## Parallel Processing

Enable parallel processing for high-throughput scenarios:

```csharp
var config = new SqsConfiguration
{
    SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/input-queue",
    SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/output-queue",
    
    EnableParallelProcessing = true,
    MaxDegreeOfParallelism = 4
};
```

**When to use:** Ideal for CPU-intensive transformations or when processing speed is critical and message order is not important. For ordered processing, keep parallelism disabled.

## JSON Serialization

The connector uses System.Text.Json for serialization with configurable options:

| Property | Type | Default | Description |
|-----------|------|---------|-------------|
| `PropertyNamingPolicy` | `JsonPropertyNamingPolicy` | `CamelCase` | JSON property naming policy |
| `PropertyNameCaseInsensitive` | `bool` | `true` | Case-insensitive property matching |

### JsonPropertyNamingPolicy

| Value | Description |
|-------|-------------|
| `LowerCase` | Property names converted to lowercase |
| `CamelCase` | Property names converted to camelCase (default) |
| `SnakeCase` | Property names converted to snake_case |
| `PascalCase` | Property names converted to PascalCase |
| `AsIs` | Property names used as-is |

## Error Handling

### Retry Logic

The connector automatically retries on transient errors with exponential backoff:

```csharp
var config = new SqsConfiguration
{
    MaxRetries = 3,
    RetryBaseDelayMs = 1000
};
```

**Retry behavior:** Transient errors (ServiceUnavailable, TooManyRequests, InternalServerError) trigger exponential backoff with jitter to avoid thundering herd problems.

### Message Error Handler

Handle deserialization errors per message:

```csharp
var config = new SqsConfiguration
{
    SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/input-queue",
    
    // Return true to skip the message, false to fail the pipeline
    MessageErrorHandler = (exception, message) =>
    {
        Console.WriteLine($"Failed to deserialize message {message.MessageId}: {exception.Message}");
        return true; // Skip and continue
    },
    
    ContinueOnError = true
};
```

### Continue on Error

Configure whether to continue processing on errors:

```csharp
var config = new SqsConfiguration
{
    ContinueOnError = true // Continue processing on send failures
};
```

## `SqsMessage<T>`

The [`SqsMessage<T>`](../../../src/NPipeline.Connectors.Aws.Sqs/Models/SqsMessage.cs:17) wraps deserialized messages with acknowledgment capability:

```csharp
public sealed class SqsMessage<T> : IAcknowledgableMessage<T>
{
    // The deserialized message body
    public T Body { get; }
    
    // SQS message ID
    public string MessageId { get; }
    
    // Receipt handle for deletion
    public string ReceiptHandle { get; }
    
    // Message attributes/metadata
    public IDictionary<string, MessageAttributeValue> Attributes { get; }
    
    // Message timestamp
    public DateTime Timestamp { get; }
    
    // Whether the message has been acknowledged
    public bool IsAcknowledged { get; }
    
    // Acknowledge the message (deletes from queue)
    public Task AcknowledgeAsync(CancellationToken cancellationToken = default);
    
    // Create a new message with different body but same acknowledgment behavior
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body);
}
```

**Why this wrapper:** Preserves acknowledgment context through transformations, allowing you to modify message content while maintaining the ability to delete the original message from the queue.

## Complete Pipeline Example

```csharp
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Pipeline;

public sealed record OrderMessage(string OrderId, string CustomerId, decimal Amount, DateTime OrderDate);

public sealed record ProcessedOrder(string OrderId, string CustomerId, decimal Amount, bool IsValid, DateTime ProcessedAt);

public sealed class SqsProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var config = new SqsConfiguration
        {
            Region = "us-east-1",
            SourceQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/orders",
            SinkQueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/processed-orders",
            MaxNumberOfMessages = 10,
            WaitTimeSeconds = 20,
            AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
            BatchAcknowledgment = new BatchAcknowledgmentOptions
            {
                BatchSize = 10,
                FlushTimeoutMs = 1000
            }
        };

        // Add SQS source
        var source = builder.AddSource(
            new SqsSourceNode<OrderMessage>(config),
            "sqs-source");

        // Add transform to validate and process orders
        var transform = builder.AddTransform<OrderTransform, SqsMessage<OrderMessage>, ProcessedOrder>("transform");

        // Add SQS sink
        var sink = builder.AddSink(
            new SqsSinkNode<ProcessedOrder>(config),
            "sqs-sink");

        // Connect the nodes
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public sealed class OrderTransform : ITransformNode<SqsMessage<OrderMessage>, ProcessedOrder>
{
    public Task<ProcessedOrder> ExecuteAsync(
        SqsMessage<OrderMessage> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var order = input.Body;
        
        return Task.FromResult(new ProcessedOrder
        {
            OrderId = order.OrderId,
            CustomerId = order.CustomerId,
            Amount = order.Amount,
            IsValid = order.Amount > 0,
            ProcessedAt = DateTime.UtcNow
        });
    }
}
```

## Configuration Reference

### SqsConfiguration

| Property | Type | Default | Description |
|-----------|------|---------|-------------|
| `AccessKeyId` | `string?` | `null` | AWS access key ID |
| `SecretAccessKey` | `string?` | `null` | AWS secret access key |
| `Region` | `string` | `"us-east-1"` | AWS region |
| `ProfileName` | `string?` | `null` | AWS profile name from ~/.aws/credentials |
| `SourceQueueUrl` | `string` | `""` | SQS queue URL for source |
| `SinkQueueUrl` | `string` | `""` | SQS queue URL for sink |
| `MaxNumberOfMessages` | `int` | `10` | Maximum messages per poll (1-10) |
| `WaitTimeSeconds` | `int` | `20` | Long polling wait time (0-20) |
| `VisibilityTimeout` | `int` | `30` | Message visibility timeout (seconds) |
| `PollingIntervalMs` | `int` | `1000` | Polling interval when empty (ms) |
| `BatchSize` | `int` | `10` | Batch size for sending (1-10) |
| `DelaySeconds` | `int` | `0` | Message delivery delay (0-900) |
| `MessageAttributes` | `IDictionary<string, MessageAttributeValue>?` | `null` | Message attributes for outgoing messages |
| `PropertyNamingPolicy` | `JsonPropertyNamingPolicy` | `CamelCase` | JSON property naming policy |
| `PropertyNameCaseInsensitive` | `bool` | `true` | Case-insensitive property matching |
| `MaxRetries` | `int` | `3` | Maximum retry attempts for transient errors |
| `RetryBaseDelayMs` | `int` | `1000` | Base delay for retry backoff (ms) |
| `ContinueOnError` | `bool` | `true` | Continue processing on errors |
| `MessageErrorHandler` | `Func<Exception, SqsMessage<object>, bool>?` | `null` | Handler for message mapping errors |
| `AcknowledgmentStrategy` | `AcknowledgmentStrategy` | `AutoOnSinkSuccess` | Message acknowledgment strategy |
| `AcknowledgmentDelayMs` | `int` | `5000` | Delay for delayed acknowledgment (ms) |
| `BatchAcknowledgment` | `BatchAcknowledgmentOptions?` | `null` | Batch acknowledgment options |
| `MaxConnectionPoolSize` | `int` | `10` | Maximum SQS client connections to pool |
| `MaxDegreeOfParallelism` | `int` | `1` | Maximum degree of parallelism |
| `EnableParallelProcessing` | `bool` | `false` | Enable parallel message processing |

### AcknowledgmentStrategy

| Value | Description |
|-------|-------------|
| `AutoOnSinkSuccess` | Acknowledge immediately after successful sink processing (default) |
| `Manual` | Manual acknowledgment via [`AcknowledgeAsync()`](../../../src/NPipeline.Connectors.Aws.Sqs/Models/SqsMessage.cs:107) |
| `Delayed` | Acknowledge after configurable delay |
| `None` | Never acknowledge automatically |

### BatchAcknowledgmentOptions

| Property | Type | Default | Description |
|-----------|------|---------|-------------|
| `BatchSize` | `int` | `10` | Maximum messages per batch (1-10) |
| `FlushTimeoutMs` | `int` | `1000` | Maximum wait before flushing partial batch (ms) |
| `EnableAutomaticBatching` | `bool` | `true` | Enable automatic batch acknowledgment |
| `MaxConcurrentBatches` | `int` | `3` | Maximum concurrent batch operations |

For more advanced configuration, refer to the [AWS SDK for .NET documentation](https://docs.aws.amazon.com/sdk-for-net/).
