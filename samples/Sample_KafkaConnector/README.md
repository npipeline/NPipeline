# Kafka Connector Sample

This sample consumes events from a Kafka topic, enriches them, and produces them to another topic with the NPipeline
Kafka connector. Each input message is acknowledged, so its offset can be committed, only once its enriched copy is on
the output topic.

## Prerequisites

- .NET 10 SDK
- Docker and Docker Compose

## Quick start

1. Start Kafka:

   ```bash
   docker compose up -d
   ```

   This starts ZooKeeper (port 2181), a Kafka broker (port 9092), and Kafka UI (<http://localhost:9000>). The
   `kafka-init` container creates the topics: `input-events` and `output-events` (3 partitions each) and
   `dead-letter-events` (1 partition).

2. Run the sample:

   ```bash
   dotnet run
   ```

   To write through a transactional sink instead, run `dotnet run -- --exactly-once`.

3. Produce a test event:

   ```bash
   echo '{"id":"8f7c1d2e-6b0a-4c55-9a51-2f2f8e3c9a10","customerId":"C-100","eventType":"OrderPlaced","timestamp":"2026-01-01T00:00:00Z","payload":{"amount":42}}' | \
     docker exec -i kafka-broker kafka-console-producer --bootstrap-server localhost:9092 --topic input-events
   ```

   Press Ctrl+C to stop the sample.

## Pipeline

```text
KafkaConnector.Source<SampleMessage>          input-events, group sample-consumer-group
  -> MessageEnricher                          message.WithBody(enriched)
    -> KafkaConnector.Sink<SampleMessage>     output-events, keyed by CustomerId
       .Acknowledging()

Undeserializable messages -> KafkaDeadLetterSink (dead-letter-events)
```

## What the code shows

`KafkaConnectorPipeline.cs` defines the pipeline:

- **Source.** `KafkaConnector.Source<SampleMessage>(bootstrap, topic, groupId, o => o with { ... })` reads JSON
  bodies and emits `KafkaMessage<SampleMessage>`, which carries the topic, partition, offset, key and headers.
  `AutoOffsetReset.Earliest` reads a new group's partitions from the start.
- **Transform.** `MessageEnricher` maps the body and keeps the message with `input.WithBody(enriched)`, so the
  message can still be acknowledged downstream.
- **Sink.** `KafkaConnector.Sink<SampleMessage>(...).Acknowledging()` produces each body and acknowledges its source
  message once the broker has it. Acknowledging stores the offset, and the consumer commits stored offsets every
  `CommitInterval` (5 seconds) and when the read ends. A message that is never acknowledged is read again after a
  restart. `KeySelector` keys each message by customer, so a customer's events stay on one partition, in order.
- **Bad messages.** `RowErrorHandler = _ => RowErrorAction.DeadLetter` sends a message whose body isn't a valid
  `SampleMessage` to the pipeline's dead-letter sink as a `MessageFailure`, and the read moves on.
  `KafkaDeadLetterSink` produces it to `dead-letter-events` with its original value and key, and the error in
  `x-dead-letter-*` headers, so it can be replayed once fixed.
- **Resilience.** The source retries a retriable consume error through `KafkaConnectorResilience.Default`, here with
  the backoff capped at five seconds. The sink doesn't retry: librdkafka and the idempotent producer already do.

## Exactly-once

With `--exactly-once`, the sink sets a `TransactionalId`:

```csharp
KafkaConnector.Sink<SampleMessage>(BootstrapServers, OutputTopic, o => o with { TransactionalId = "sample-kafka-connector-1" })
```

Each batch is then one transaction that also commits the offsets of the messages it came from, so a batch and its
input offsets commit or abort together. Each running instance needs its own transactional id. Consumers of
`output-events` should read committed messages only, which is librdkafka's default.

## Monitoring

Kafka UI at <http://localhost:9000> shows topics, messages and consumer groups. To check the group's lag from the
command line:

```bash
docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group sample-consumer-group
```

To read the group's topic from the start again, stop the sample and reset its offsets:

```bash
docker exec kafka-broker kafka-consumer-groups --bootstrap-server localhost:9092 --group sample-consumer-group \
  --reset-offsets --to-earliest --topic input-events --execute
```

## Clean up

```bash
docker compose down -v
```
