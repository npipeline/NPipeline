# SQS Connector Sample

This sample processes order messages from an Amazon SQS queue with the NPipeline AWS SQS connector. It receives orders
from an input queue, validates them, and sends the results to an output queue. Each input message is deleted from its
queue only once its result has been sent.

## Pipeline

```text
SqsConnector.Source<Order>                     input-orders-queue
  -> OrderProcessor                            message.WithBody(processed)
    -> SqsConnector.Sink<ProcessedOrder>       processed-orders-queue
       .Acknowledging()

Undeserializable messages -> ConsoleDeadLetterSink
```

## Prerequisites

- .NET 10 SDK
- Two standard SQS queues, for example `input-orders-queue` and `processed-orders-queue`, in AWS or in
  [LocalStack](https://www.localstack.cloud/)
- AWS credentials the SDK's default chain can find: environment variables (`AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`), a shared profile (`AWS_PROFILE`), or an IAM role

## Configuration

The sample reads its settings from environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `SQS_INPUT_QUEUE_URL` | A placeholder URL | Queue that orders are received from |
| `SQS_OUTPUT_QUEUE_URL` | A placeholder URL | Queue that results are sent to |
| `AWS_REGION` | `us-east-1` | The queues' region |
| `SQS_SERVICE_URL` | None | A custom endpoint, such as LocalStack's `http://localhost:4566` |

## Run the sample

```bash
export SQS_INPUT_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/input-orders-queue
export SQS_OUTPUT_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/processed-orders-queue
dotnet run
```

Send an order to the input queue:

```bash
aws sqs send-message --queue-url "$SQS_INPUT_QUEUE_URL" --message-body \
  '{"orderId":"ORD-001","customerId":"CUST-12345","totalAmount":99.99,"status":"Pending","createdAt":"2026-01-15T10:30:00Z"}'
```

The sample prints each order as it processes it. An order with a `totalAmount` of zero or less is sent on with the
status `Rejected`. A body that isn't a valid order is printed by the dead-letter sink. Press Ctrl+C to stop.

## What the code shows

- **Shared client.** `Program` creates one `AmazonSQSClient` and passes it to both nodes with
  `o => o with { Client = client }`. Without a client, each node creates its own from `Region`, `ServiceUrl`,
  `ProfileName` or `Credentials`.
- **Source.** `SqsConnector.Source<Order>(queueUrl, o => o with { ... })` long-polls the queue (`WaitTime`, up to 20
  seconds) for up to `MaxMessages` messages at a time, and emits `SqsMessage<Order>`, which carries the receipt
  handle, attributes and `ReceiveCount`. `VisibilityTimeout` is how long a message stays hidden; one not
  acknowledged by then is delivered again.
- **Transform.** `OrderProcessor` maps the body and keeps the message with `input.WithBody(processed)`, so the message
  can still be acknowledged downstream.
- **Sink.** `SqsConnector.Sink<ProcessedOrder>(queueUrl).Acknowledging()` sends results in batches of up to 10 and
  acknowledges each input message, which deletes it from the input queue, once SQS accepts its result. The result
  keeps the input message's attributes.
- **Bad messages.** `RowErrorHandler = _ => RowErrorAction.DeadLetter` sends a message whose body doesn't deserialize
  to the pipeline's dead-letter sink as a `MessageFailure` (source queue, message id, body and attributes), then
  deletes it. `ConsoleDeadLetterSink` prints it; a real pipeline might send it to another queue for inspection.

## Troubleshooting

- **No messages are processed.** Check the queue URLs and region, and that the input queue has messages.
- **Messages come back.** A message is delivered again if it isn't acknowledged within its visibility timeout, for
  example because the output queue rejected its result. Check the output queue URL and permissions
  (`sqs:SendMessage` on the output queue; `sqs:ReceiveMessage` and `sqs:DeleteMessage` on the input queue).
- **Credential errors.** Check that the SDK's default credential chain can find credentials.

## Related documentation

- [AWS SQS connector](../../docs/connectors/aws-sqs.md)
- [Message queues: shared behaviour](../../docs/connectors/message-queues.md)
