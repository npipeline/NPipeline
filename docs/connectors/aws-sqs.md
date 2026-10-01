---
title: "AWS SQS Connector"
description: "Receive from and send to Amazon SQS standard and FIFO queues with long polling, batched deletes and batched sends."
order: 19
---

# AWS SQS Connector

The `NPipeline.Connectors.Aws.Sqs` package receives from Amazon SQS queues with long polling and sends to them in
batches. Serialization, settlement, undeserializable messages and failed writes work as in every message-queue
connector; see [Message Queues: Shared Behaviour](message-queues.md).

## Installation

```bash
dotnet add package NPipeline.Connectors.Aws.Sqs
```

**Dependencies:** [AWSSDK.SQS](https://www.nuget.org/packages/AWSSDK.SQS) 4.x

## Connection

Pass a shared `IAmazonSQS` as `Client`, or let each node create one from these settings (and dispose it):

| Option | Default | Description |
| --- | --- | --- |
| `Client` | `null` | A client to use, which the caller owns |
| `Region` | SDK's | The region (`AWS_REGION`, the profile) |
| `Credentials` | SDK's default chain | Credentials; otherwise `ProfileName`, then the default chain (environment, instance or task role) |
| `ProfileName` | `null` | A named profile from the shared credentials file |
| `ServiceUrl` | `null` | A service URL, such as Floci's |
| `RetryMode`, `MaxErrorRetry` | `Standard`, 3 | The SDK's retries; see [Retries](#retries) |

In production, prefer the default chain with an IAM role over explicit credentials.

## Receiving

```csharp
var orders = SqsConnector.Source<Order>("https://sqs.ap-southeast-2.amazonaws.com/123456789012/orders",
    o => o with { VisibilityTimeout = TimeSpan.FromMinutes(2) });
```

Each message is an `SqsMessage<T>` with its `Body`, `MessageId`, `ReceiptHandle`, `Attributes` (the sender's message
attributes), `SystemAttributes`, `SentAt` and `ReceiveCount`. Acknowledging a message deletes it, in batches of up to
ten (`DeleteLinger`, 50 ms), so acknowledging costs a tenth of a request. A message not acknowledged within its
visibility timeout is delivered again, so set `VisibilityTimeout` longer than a message takes to write.
`RejectAsync(requeue: true)` makes it visible again at once; `RejectAsync(requeue: false)` deletes it.

A delete that fails leaves the message to be delivered again (it is logged and counted as `delete_failed`), which
at-least-once delivery allows. When the read ends, the source sends its outstanding deletes once the messages it
handed on are settled (up to `SettleTimeout`).

### Source options

| Option | Default | Description |
| --- | --- | --- |
| `QueueUrl` | required | The queue |
| `MaxMessages` | 10 | Messages per receive, 1 to 10 |
| `WaitTime` | 20 s | Long polling: how long a receive waits for a message (fewer empty receives, lower cost) |
| `VisibilityTimeout` | the queue's | How long a received message stays hidden |
| `RowErrorHandler`, `RawExcerptLength` | fail, 256 | See [Messages that don't deserialize](message-queues.md#messages-that-dont-deserialize); `Skip` deletes the message, and `Fail` leaves it to be delivered again after its visibility timeout |
| `DeleteLinger` | 50 ms | How long an acknowledged message waits to be deleted with others |
| `SettleTimeout` | 30 s | How long the source waits for handed-on messages after the read ends |

`ReceiveCount` is how many times SQS has delivered the message, so a rising count flags a message that keeps failing.

A node that does its own settling calls `AcknowledgeAsync` (delete) once a message is handled, or `RejectAsync` to
release or delete it:

```csharp
await message.AcknowledgeAsync(cancellationToken);
```

### Dead-letter queues

SQS moves a message to a dead-letter queue only through the queue's redrive policy, after `maxReceiveCount`
deliveries; configure it on the queue. A message that keeps failing is then set aside without the pipeline doing
anything. Read the dead-letter queue with its own source.

## Sending

```csharp
var invoices = SqsConnector.Sink<Invoice>(invoicesUrl, o => o with { Client = client });
builder.AddSink(invoices.Acknowledging(), "invoices");
```

The sink sends `SendMessageBatch` requests of up to ten messages, split when a batch would pass SQS's 256 KB request
limit. Entries SQS rejects are handled as `FailedMessages` says. A message received from SQS keeps its message
attributes (`CopyMessageAttributes`).

For a FIFO queue, set `MessageGroupId`; messages of a group are delivered in order. Without content-based
deduplication, `DeduplicationId` chooses each message's deduplication id, and when it is `null` a message received from
a queue keeps its message id, so a retried write isn't delivered twice.

### Sink options

| Option | Default | Description |
| --- | --- | --- |
| `QueueUrl` | required | The queue |
| `BatchSize`, `BatchLinger` | 10, 10 ms | Messages per request, and the longest a batch waits to fill |
| `Delay` | none | Delays each message up to 15 minutes; standard queues only |
| `MessageAttributes`, `CopyMessageAttributes` | none, `true` | Attributes added to every message, and whether a received message keeps its own |
| `MessageGroupId`, `DeduplicationId` | `null` | FIFO queues |
| `FailedMessages` | `Fail` | See [Failed writes](message-queues.md#failed-writes) |

## Retries

The connector leaves retries to the AWS SDK, which recognizes throttling and transient errors, backs off with jitter,
and uses a retry quota so a failing endpoint isn't flooded. An exception that reaches a node has used up the SDK's
retries.

| Option | Default | Description |
| --- | --- | --- |
| `RetryMode` | `Standard` | `Standard` retries throttling, 5xx and network errors with jittered exponential backoff; `Adaptive` also slows the client when SQS throttles it; `null` lets the SDK decide (`AWS_RETRY_MODE`) |
| `MaxErrorRetry` | 3 | Retries per call, not attempts (four attempts); `0` turns them off; `null` lets the SDK decide (`AWS_MAX_ATTEMPTS`) |

Both apply only to a client the node creates. A `Client` you pass keeps its own `AmazonSQSConfig` settings.

Standard queues deliver at least once, and a retried send whose first attempt reached SQS can enqueue a message twice.
Use a FIFO queue with deduplication, or make consumers idempotent, where duplicates matter.

## Best practices

1. **Use long polling** (`WaitTime` of 20 s, the default): fewer empty responses, lower cost.
2. **Set `VisibilityTimeout` longer than a message takes to write**, so it isn't delivered again while it is in flight.
3. **Use IAM roles** for credentials in production (an EC2 instance role or ECS task role).
4. **Configure a dead-letter queue** on the SQS queue for poison messages.
5. **Use FIFO queues** when order matters, with a `MessageGroupId`.
6. **Keep batching on.** SQS charges per request, and batched deletes and sends cost a tenth as many.

## Next Steps

- [Message Queues: Shared Behaviour](message-queues.md)
- [Kafka](kafka.md), [RabbitMQ](rabbitmq.md), [Azure Service Bus](azure-service-bus.md)
