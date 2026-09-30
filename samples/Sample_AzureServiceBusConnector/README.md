# Azure Service Bus Connector Sample

This sample processes order messages with the NPipeline Azure Service Bus connector. It receives orders from the
`input-orders` queue, validates them, and sends the results to the `processed-orders` queue. Each input message is
completed only once its result has been sent.

## Prerequisites

- .NET 10 SDK
- An Azure Service Bus namespace with two queues: `input-orders` and `processed-orders`
- The namespace's connection string in `SERVICEBUS_CONNECTION_STRING`

## Run the sample

```bash
export SERVICEBUS_CONNECTION_STRING="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=..."
dotnet run
```

Send a message to `input-orders`, for example with Service Bus Explorer in the Azure portal:

```json
{ "orderId": 1001, "customerId": 42, "totalAmount": 99.99, "status": "Pending", "createdAt": "2026-01-15T10:30:00Z" }
```

The result appears in `processed-orders`. An order with a `totalAmount` of zero or less is sent on with the status
`Rejected`. A body that isn't a valid order moves to the dead-letter sub-queue of `input-orders`. Press Ctrl+C to stop.

## What the code shows

- **Shared client.** `Program` creates one `ServiceBusClient` for the source and the sink. For dependency injection,
  `services.AddServiceBusConnector(connectionString)` registers one and a `ServiceBusNodeFactory` that creates nodes
  on it.
- **Source.** `ServiceBusConnector.Source<Order>(client, "input-orders", o => o with { ... })` emits
  `ServiceBusMessage<Order>`, with the delivery count, session, correlation id and application properties. The
  source renews each message's lock while it waits to be settled, for up to `MaxLockRenewal`.
- **Transform.** `OrderProcessor` maps the body and keeps the message with `input.WithBody(processed)`, so the message
  can still be completed downstream.
- **Sink.** `ServiceBusConnector.Sink<ProcessedOrder>(client, "processed-orders").Acknowledging()` sends results in
  batches and completes each input message once its result is sent. The result keeps the input's message id, so
  duplicate detection works across the hop, along with its correlation id and application properties.
- **Bad messages.** `RowErrorHandler = _ => RowErrorAction.Skip` moves a message whose body doesn't deserialize to
  the queue's dead-letter sub-queue. With `RowErrorAction.DeadLetter`, it goes to the pipeline's dead-letter sink as
  a `MessageFailure` instead.

A message that is never completed, for example because the sample stopped first, is abandoned and delivered again.

## Topics and sessions

To read a topic's subscription or a session-enabled entity, swap the source:

```csharp
var billing = ServiceBusConnector.SubscriptionSource<Order>(client, "orders-topic", "billing");
var bySession = ServiceBusConnector.SessionSource<Order>(client, "orders-by-customer", configure: o => o with { MaxConcurrentSessions = 4 });
```

A session source hands each session's messages on in order. To send to a session-enabled entity, choose each
message's session: `ServiceBusConnector.Sink<ProcessedOrder>(client, entity, o => o with { SessionId = p => p.CustomerId.ToString() })`.
