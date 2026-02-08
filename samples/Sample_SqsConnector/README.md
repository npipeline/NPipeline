# SQS Connector Sample

This sample demonstrates how to use the AWS SQS Connector with NPipeline to process order messages from an SQS queue.

## Overview

The sample implements a simple order processing pipeline that:

1. **Consumes** order messages from an input SQS queue using [`SqsSourceNode<T>`](../..//src/NPipeline.Connectors.Aws.Sqs/Nodes/SqsSourceNode.cs)
2. **Processes** each order through validation logic in [`OrderProcessor`](SqsConnectorPipeline.cs)
3. **Publishes** processed orders to an output SQS queue using [`SqsSinkNode<T>`](../..//src/NPipeline.Connectors.Aws.Sqs/Nodes/SqsSinkNode.cs)
4. **Automatically acknowledges** messages after successful processing (default behavior)

## Pipeline Structure

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ SQS Order Processing Pipeline                                               │
└─────────────────────────────────────────────────────────────────────────────┘

Flow:
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│  SqsSourceNode   │─────▶│  OrderProcessor  │─────▶│   SqsSinkNode    │
│   (Order)        │      │  (Transform)     │      │ (ProcessedOrder) │
└──────────────────┘      └──────────────────┘      └──────────────────┘
        │                           │                         │
        ▼                           ▼                         ▼
Input SQS Queue              Order Processing          Output SQS Queue
(input-orders-queue)         & Validation              (processed-orders-queue)
```

## Prerequisites

### AWS Account

- An active AWS account with appropriate permissions
- SQS service enabled in your region

### SQS Queues

Create two SQS queues:

1. **Input Queue**: `input-orders-queue`
   - Standard queue type
   - Messages contain JSON order data

2. **Output Queue**: `processed-orders-queue`
   - Standard queue type
   - Receives processed order results

### AWS Credentials

Configure AWS credentials using one of the following methods:

1. **Environment Variables** (Recommended for development):

   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

2. **AWS Profile** (Recommended for production):

   ```bash
   aws configure --profile npipeline
   ```

   Then set the profile name in [`SqsConfiguration`](SqsConnectorPipeline.cs)

3. **IAM Role** (Recommended for EC2/ECS):
   Attach an IAM role with appropriate SQS permissions

### Sample Message Format

Send messages to the input queue in the following JSON format:

```json
{
  "orderId": "ORD-001",
  "customerId": "CUST-12345",
  "totalAmount": 99.99,
  "status": "Pending",
  "createdAt": "2025-01-15T10:30:00Z"
}
```

## Configuration

Update the [`SqsConnectorPipeline`](SqsConnectorPipeline.cs) class with your AWS configuration:

```csharp
private const string InputQueueUrl = "https://sqs.{region}.amazonaws.com/{account-id}/input-orders-queue";
private const string OutputQueueUrl = "https://sqs.{region}.amazonaws.com/{account-id}/processed-orders-queue";
private const string Region = "us-east-1";
```

Replace:

- `{region}` with your AWS region (e.g., `us-east-1`, `eu-west-2`)
- `{account-id}` with your AWS account ID (12-digit number)

### SQS Configuration Options

The sample uses the following [`SqsConfiguration`](../..//src/NPipeline.Connectors.Aws.Sqs/Configuration/SqsConfiguration.cs) settings:

| Property | Value | Description |
|----------|--------|-------------|
| `SourceQueueUrl` | Input queue URL | Queue to consume messages from |
| `SinkQueueUrl` | Output queue URL | Queue to publish processed messages to |
| `MaxNumberOfMessages` | 10 | Maximum messages per poll (1-10) |
| `WaitTimeSeconds` | 20 | Long polling wait time (0-20 seconds) |
| `VisibilityTimeout` | 30 | Message visibility timeout in seconds |
| `AcknowledgmentStrategy` | `AutoOnSinkSuccess` | Auto-acknowledge on successful sink |

## Running the Sample

### Build the Sample

```bash
cd samples/Sample_SqsConnector
dotnet build
```

### Run the Sample

```bash
dotnet run
```

### Expected Output

When messages are available in the input queue:

```
=== NPipeline Sample: SQS Connector for Order Processing ===

Registered NPipeline services and scanned assemblies for nodes.

Pipeline Description:
Pipeline Structure:
┌─────────────────────────────────────────────────────────────────────────────┐
│ SQS Order Processing Pipeline                                               │
└─────────────────────────────────────────────────────────────────────────────┘
...

Starting pipeline execution...
Press Ctrl+C to stop.

Processing Order ID: ORD-001, Customer: CUST-12345, Amount: $99.99
  ✓ Order ORD-001 processed successfully

Processing Order ID: ORD-002, Customer: CUST-67890, Amount: $0.00
  ⚠ Order ORD-002 rejected: Invalid amount
```

## Key Features Demonstrated

### 1. Automatic Message Acknowledgment

The sample uses `AcknowledgmentStrategy.AutoOnSinkSuccess`, which means:

- Messages are automatically acknowledged after successful processing
- If processing fails, the message remains in the queue (after visibility timeout)
- No manual acknowledgment code required

### 2. Order Validation

The [`OrderProcessor`](SqsConnectorPipeline.cs) validates orders:

- Rejects orders with `TotalAmount <= 0`
- Sets status to "Rejected" or "Completed"
- Includes processing notes

### 3. JSON Serialization

The SQS connector automatically handles:

- JSON deserialization from SQS messages
- JSON serialization to SQS messages
- CamelCase property naming (configurable)

### 4. Continuous Polling

[`SqsSourceNode<T>`](../..//src/NPipeline.Connectors.Aws.Sqs/Nodes/SqsSourceNode.cs) continuously polls the queue:

- Uses long polling (20 seconds) for cost efficiency
- Retrieves up to 10 messages per poll
- Handles transient errors with automatic retry

## Advanced Usage

### Manual Acknowledgment

To manually acknowledge messages, change the acknowledgment strategy:

```csharp
AcknowledgmentStrategy = AcknowledgmentStrategy.Manual
```

Then call `await input.AcknowledgeAsync()` in your transform node.

### Delayed Acknowledgment

For delayed acknowledgment:

```csharp
AcknowledgmentStrategy = AcknowledgmentStrategy.Delayed,
AcknowledgmentDelayMs = 5000  // 5 seconds
```

### Batch Acknowledgment

Enable batch acknowledgment for improved performance:

```csharp
BatchAcknowledgment = new BatchAcknowledgmentOptions
{
    EnableAutomaticBatching = true,
    BatchSize = 10,
    FlushTimeoutMs = 5000,
    MaxConcurrentBatches = 5
}
```

### Parallel Processing

Enable parallel message processing:

```csharp
EnableParallelProcessing = true,
MaxDegreeOfParallelism = 4
```

## Troubleshooting

### No messages being processed

1. Verify queue URLs are correct
2. Check AWS credentials are configured
3. Ensure messages exist in the input queue
4. Verify IAM permissions for SQS operations

### Messages not being acknowledged

1. Check if `AcknowledgmentStrategy` is set correctly
2. Verify the sink node is publishing successfully
3. Check SQS queue permissions for delete operations

### Connection errors

1. Verify network connectivity to AWS
2. Check region configuration
3. Ensure AWS credentials are valid
4. Review SQS service status in your region

## Related Documentation

- [AWS SQS Connector Design Document](../../plans/aws-sqs-connector-design.md)
- [SqsSourceNode Documentation](../..//src/NPipeline.Connectors.Aws.Sqs/Nodes/SqsSourceNode.cs)
- [SqsSinkNode Documentation](../..//src/NPipeline.Connectors.Aws.Sqs/Nodes/SqsSinkNode.cs)
- [SqsConfiguration Documentation](../..//src/NPipeline.Connectors.Aws.Sqs/Configuration/SqsConfiguration.cs)

## License

This sample is part of the NPipeline project. See the main [LICENSE](../../LICENSE) file for details.
