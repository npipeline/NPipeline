# NPipeline Kafka Connector

Source and sink nodes for Apache Kafka in NPipeline pipelines, on NPipeline's shared messaging layer.

## About NPipeline

NPipeline is a high-performance, extensible data processing framework for .NET that enables developers to build scalable and efficient pipeline-based
applications. It provides a rich set of components for data transformation, aggregation, branching, and parallel processing, with built-in support for
resilience patterns and error handling.

## Installation

```bash
dotnet add package NPipeline.Connectors.Kafka
```

Targets .NET 8.0, 9.0 and 10.0.

## Features

- **Ordered offset commits**: acknowledging stores offset + 1, and a partition's commit never passes a message still
  being handled or one that failed; offsets commit in the background, not per message.
- **Exactly-once** with a transactional id: each batch commits together with the offsets of the messages it came from.
- **Avro and Protobuf** serializers backed by a schema registry, with subjects derived from the topic.
- **A clean leave**: a finished read commits and leaves the group, so its partitions move on at once.
- **One messaging model**: messages are acknowledged or rejected once, and `sink.Acknowledging()` makes any sink (SQL,
  HTTP, another broker) acknowledge each message once it is written.
- **Shared JSON defaults** with the JSON connector (camelCase, case-insensitive, enums as names, `[Column]`), or a
  source-generated `JsonSerializerContext` for Native AOT.
- **Undeserializable messages** go through the shared row-error handler: fail the read, skip, or send the whole message
  to the pipeline's dead-letter sink.
- **Failed writes** fail, requeue the source message, or go to the dead-letter sink.

## Usage

```csharp
using NPipeline.Connectors.Kafka;
using NPipeline.Connectors.Messaging;

var orders = KafkaConnector.Source<Order>("kafka:9092", "orders", groupId: "billing",
    o => o with { AutoOffsetReset = AutoOffsetReset.Earliest });
var invoices = KafkaConnector.Sink<Invoice>("kafka:9092", "invoices", o => o with { KeySelector = i => i.CustomerId });

builder.AddSink(invoices.Acknowledging(), "invoices");
```

See the [Kafka connector documentation](https://docs.npipeline.net/connectors/kafka) and
[Message Queues: Shared Behaviour](https://docs.npipeline.net/connectors/message-queues) for every option.

## Related Packages

- **[NPipeline](https://www.nuget.org/packages/NPipeline)** - Core pipeline framework
- **[NPipeline.Connectors](https://www.nuget.org/packages/NPipeline.Connectors)** - Shared messaging layer, storage abstractions and base connectors
- **[NPipeline.Extensions.DependencyInjection](https://www.nuget.org/packages/NPipeline.Extensions.DependencyInjection)** - Dependency injection integration

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

**Free for non-production use.** Production use is free for organizations with 4 or fewer developers and annual revenue of $5M AUD or less. Larger organizations require a [commercial license](https://npipeline.com). This license automatically converts to MIT two years after each release.
