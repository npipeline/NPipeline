using Confluent.Kafka;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Kafka;
using NPipeline.Connectors.Kafka.DeadLetter;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Kafka.Reliability;
using NPipeline.Connectors.Messaging;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_KafkaConnector;

/// <summary>
///     Consumes events from <c>input-events</c>, enriches them, and produces them to <c>output-events</c>. Each input
///     message is acknowledged (its offset committed) once its enriched copy is on the output topic.
/// </summary>
/// <param name="deadLetterProducer">The producer the dead-letter sink uses; the caller owns it.</param>
/// <param name="exactlyOnce">Whether the sink writes in transactions that also commit the input offsets.</param>
public sealed class KafkaConnectorPipeline(IProducer<byte[]?, byte[]> deadLetterProducer, bool exactlyOnce) : IPipelineDefinition
{
    public const string BootstrapServers = "localhost:9092";
    public const string InputTopic = "input-events";
    public const string OutputTopic = "output-events";
    public const string DeadLetterTopic = "dead-letter-events";
    public const string ConsumerGroup = "sample-consumer-group";

    /// <inheritdoc />
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            KafkaConnector.Source<SampleMessage>(BootstrapServers, InputTopic, ConsumerGroup, o => o with
            {
                ClientId = "sample-kafka-connector",
                AutoOffsetReset = AutoOffsetReset.Earliest,

                // A message that isn't a valid SampleMessage goes to the dead-letter topic, and the read moves on.
                RowErrorHandler = _ => RowErrorAction.DeadLetter,

                // Retry a retriable consume error up to three times, waiting at most five seconds between attempts.
                Resilience = KafkaConnectorResilience.Default with
                {
                    Backoff = KafkaConnectorResilience.Default.Backoff with { MaximumDelay = TimeSpan.FromSeconds(5) },
                },
            }),
            "kafka-source");

        var enrich = builder.AddTransform<MessageEnricher, KafkaMessage<SampleMessage>, IAcknowledgableMessage<SampleMessage>>("message-enricher");

        // Acknowledging() settles each input message once the broker has its output. Keying by customer keeps each
        // customer's events on one partition, in order.
        var sink = builder.AddSink(
            KafkaConnector.Sink<SampleMessage>(BootstrapServers, OutputTopic, o => o with
            {
                ClientId = "sample-kafka-connector",
                KeySelector = message => message.CustomerId,
                BatchSize = 100,

                // Exactly-once: each batch and the offsets of the messages it came from commit in one transaction.
                // Each running instance needs its own id.
                TransactionalId = exactlyOnce ? "sample-kafka-connector-1" : null,
            }).Acknowledging(),
            "kafka-sink");

        builder.Connect(source, enrich);
        builder.Connect(enrich, sink);

        builder.AddDeadLetterSink(new KafkaDeadLetterSink(deadLetterProducer, DeadLetterTopic));
    }

    /// <summary>Describes what the pipeline does.</summary>
    public static string GetDescription(bool exactlyOnce) =>
        $"""
         KafkaConnector.Source<SampleMessage>   ({InputTopic}, group {ConsumerGroup})
           -> MessageEnricher                   (message.WithBody(enriched))
             -> KafkaConnector.Sink<SampleMessage>.Acknowledging()   ({OutputTopic}, keyed by CustomerId)

         Undeserializable messages -> KafkaDeadLetterSink ({DeadLetterTopic})
         Mode: {(exactlyOnce ? "exactly-once (transactional sink)" : "at-least-once")}
         """;
}

/// <summary>Adds processing metadata to each event, keeping the Kafka message so it can be acknowledged downstream.</summary>
public sealed class MessageEnricher : TransformNode<KafkaMessage<SampleMessage>, IAcknowledgableMessage<SampleMessage>>
{
    /// <inheritdoc />
    public override ValueTask<IAcknowledgableMessage<SampleMessage>> TransformAsync(KafkaMessage<SampleMessage> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        var enriched = input.Body with
        {
            ProcessedAt = DateTime.UtcNow,
            ProcessingNode = Environment.MachineName,
        };

        Console.WriteLine($"Processed {enriched.Id} from {input.Topic}/{input.Partition}@{input.Offset} - {enriched.EventType}");

        return ValueTask.FromResult(input.WithBody(enriched));
    }
}

/// <summary>An event read from and written to Kafka as JSON.</summary>
public sealed record SampleMessage
{
    public Guid Id { get; init; }
    public string CustomerId { get; init; } = string.Empty;
    public string EventType { get; init; } = string.Empty;
    public DateTime Timestamp { get; init; }
    public object? Payload { get; init; }
    public DateTime? ProcessedAt { get; init; }
    public string? ProcessingNode { get; init; }
}
