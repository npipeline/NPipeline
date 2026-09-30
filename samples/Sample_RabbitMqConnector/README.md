# RabbitMQ Connector Sample

This sample consumes order events from a RabbitMQ queue, enriches them, and publishes them to a topic exchange with
the NPipeline RabbitMQ connector. Each order is acknowledged only once the broker confirms its enriched copy.

## Prerequisites

- .NET 10 SDK
- Docker and Docker Compose

## Run the sample

1. Start RabbitMQ:

   ```bash
   docker compose up -d
   ```

2. Run the sample:

   ```bash
   dotnet run
   ```

   The source declares the `orders` quorum queue and binds it to the `orders-exchange` topic exchange for
   `order.created` and `order.updated`. The program also declares `enriched-orders` (bound to
   `enriched-orders-exchange` for `order.enriched`) and `orders-dead-letter`, so you can inspect the output.

3. In the management UI at <http://localhost:15672> (guest/guest), open **Exchanges** > `orders-exchange` and
   publish a message with routing key `order.created` and this payload:

   ```json
   { "orderId": "ORD-1", "customerId": "US-42", "amount": 19.99, "createdAt": "2026-01-01T00:00:00Z" }
   ```

   The enriched order appears in `enriched-orders`. A payload that isn't a valid order, such as `not json`, appears in
   `orders-dead-letter`.

Press Ctrl+C to stop the sample.

## What the code shows

- **One shared connection.** `RabbitMqConnector.Connect(new RabbitMqConnectionOptions { ... })` creates a connection
  that the source, sink and dead-letter sink share. Dispose it when the application stops.
- **Source.** `RabbitMqConnector.Source<OrderEvent>(connection, "orders", o => o with { ... })` emits
  `RabbitMqMessage<OrderEvent>`, with the routing key, headers and other properties. `PrefetchCount` bounds the
  unacknowledged messages in flight, and `Topology` declares the queue and its bindings.
- **Transform.** `OrderEnricher` maps the body and keeps the message with `input.WithBody(enriched)`, so the message
  can still be acknowledged downstream.
- **Sink.** `RabbitMqConnector.Sink<EnrichedOrder>(connection, exchange, routingKey).Acknowledging()` publishes each
  body with publisher confirms and acknowledges its source message once the broker confirms it. The published message
  keeps the source message's headers, correlation id and message id.
- **Bad messages.** `RowErrorHandler = _ => RowErrorAction.DeadLetter` sends a message whose body doesn't deserialize to
  the pipeline's dead-letter sink as a `MessageFailure`, then rejects it. `RabbitMqDeadLetterSink` publishes it with
  its original body and the error in `x-death-*` headers.

A message that is never acknowledged, for example because the sample stopped first, goes back on the queue.

For dependency injection, `services.AddRabbitMq(options)` registers one shared connection and a
`RabbitMqNodeFactory` that creates nodes on it.

## Clean up

```bash
docker compose down -v
```
