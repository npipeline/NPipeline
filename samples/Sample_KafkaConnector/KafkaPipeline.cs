using Confluent.Kafka;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Connectors.Kafka.Retry;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using DeliverySemantic = NPipeline.Connectors.Kafka.Configuration.DeliverySemantic;

namespace Sample_KafkaConnector;

/// <summary>
///     Pipeline demonstrating Kafka connector usage for streaming event processing.
/// </summary>
public sealed class KafkaConnectorPipeline : IPipelineDefinition
{
    /// <inheritdoc />
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<KafkaSourceNode<SampleMessage>, KafkaMessage<SampleMessage>>(
            "kafka-source");

        var enrich = builder.AddTransform<MessageEnricher, KafkaMessage<SampleMessage>, SampleMessage>(
            "message-enricher");

        var sink = builder.AddSink<KafkaSinkNode<SampleMessage>, SampleMessage>(
            "kafka-sink");

        builder.Connect(source, enrich);
        builder.Connect(enrich, sink);
    }

    /// <summary>
    ///     Creates the default Kafka configuration for this sample.
    /// </summary>
    public static KafkaConfiguration CreateConfiguration()
    {
        return new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            ClientId = "sample-kafka-connector",
            SourceTopic = "input-events",
            ConsumerGroupId = "sample-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            SinkTopic = "output-events",
            EnableIdempotence = true,
            BatchSize = 100,
            LingerMs = 10,
            Acks = Acks.All,
            DeliverySemantic = DeliverySemantic.AtLeastOnce,
            AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
            ContinueOnError = false,
        };
    }

    /// <summary>
    ///     Creates the retry strategy used by the sample.
    /// </summary>
    public static IRetryStrategy CreateRetryStrategy()
    {
        return new ExponentialBackoffRetryStrategy
        {
            MaxRetries = 3,
            BaseDelayMs = 100,
            MaxDelayMs = 5000,
            JitterFactor = 0.2,
        };
    }

    /// <summary>
    ///     Gets a description of what the Kafka pipeline demonstrates.
    /// </summary>
    public static string GetDescription()
    {
        return """
               Kafka Connector Sample:

               This sample demonstrates an end-to-end Kafka pipeline using NPipeline:

               Pipeline Flow:
               KafkaSourceNode<SampleMessage>
                 -> MessageEnricher (adds processing metadata)
                   -> KafkaSinkNode<SampleMessage>

               Key Features:
               - Kafka consumer group processing from input-events
               - Simple enrichment transform with metadata
               - Batched Kafka production to output-events
               - Configurable retry strategy and partitioning
               """;
    }
}

/// <summary>
///     Transform node that enriches messages with processing metadata.
/// </summary>
public sealed class MessageEnricher : TransformNode<KafkaMessage<SampleMessage>, SampleMessage>
{
    /// <inheritdoc />
    public override Task<SampleMessage> ExecuteAsync(
        KafkaMessage<SampleMessage> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var message = input.Body;

        var enriched = new SampleMessage
        {
            Id = message.Id,
            CustomerId = message.CustomerId,
            EventType = message.EventType,
            Timestamp = message.Timestamp,
            Payload = message.Payload,
            ProcessedAt = DateTime.UtcNow,
            ProcessingNode = Environment.MachineName,
        };

        Console.WriteLine($"Processed {enriched.Id} from {input.Topic}/{input.Partition} - {enriched.EventType}");

        return Task.FromResult(enriched);
    }
}

/// <summary>
///     Sample message type for demonstration.
/// </summary>
public sealed class SampleMessage
{
    public Guid Id { get; set; }
    public string CustomerId { get; set; } = string.Empty;
    public string EventType { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public object? Payload { get; set; }
    public DateTime? ProcessedAt { get; set; }
    public string? ProcessingNode { get; set; }
}

/// <summary>
///     Simple console-based metrics implementation for demonstration.
/// </summary>
public sealed class ConsoleKafkaMetrics : IKafkaMetrics
{
    public void RecordProduced(string topic, int count)
    {
        Log("Produced", topic, count);
    }

    public void RecordProduceLatency(string topic, TimeSpan latency)
    {
        Log("ProduceLatency", topic, latency.TotalMilliseconds);
    }

    public void RecordProduceError(string topic, Exception ex)
    {
        Log("ProduceError", topic, ex.Message);
    }

    public void RecordBatchSize(string topic, int size)
    {
        Log("BatchSize", topic, size);
    }

    public void RecordConsumed(string topic, int count)
    {
        Log("Consumed", topic, count);
    }

    public void RecordPollLatency(string topic, TimeSpan latency)
    {
        Log("PollLatency", topic, latency.TotalMilliseconds);
    }

    public void RecordCommitLatency(string topic, TimeSpan latency)
    {
        Log("CommitLatency", topic, latency.TotalMilliseconds);
    }

    public void RecordCommitError(string topic, Exception ex)
    {
        Log("CommitError", topic, ex.Message);
    }

    public void RecordLag(string topic, int partition, long lag)
    {
        Log($"Lag[{partition}]", topic, lag);
    }

    public void RecordSerializeLatency(Type type, TimeSpan latency)
    {
        Console.WriteLine($"[METRIC] SerializeLatency - Type: {type.Name}, Latency: {latency.TotalMilliseconds}ms");
    }

    public void RecordDeserializeLatency(Type type, TimeSpan latency)
    {
        Console.WriteLine($"[METRIC] DeserializeLatency - Type: {type.Name}, Latency: {latency.TotalMilliseconds}ms");
    }

    public void RecordSerializeError(Type type, Exception ex)
    {
        Console.WriteLine($"[METRIC] SerializeError - Type: {type.Name}, Error: {ex.Message}");
    }

    public void RecordDeserializeError(Type type, Exception ex)
    {
        Console.WriteLine($"[METRIC] DeserializeError - Type: {type.Name}, Error: {ex.Message}");
    }

    public void RecordTransactionCommit(TimeSpan latency)
    {
        Console.WriteLine($"[METRIC] TransactionCommit - Latency: {latency.TotalMilliseconds}ms");
    }

    public void RecordTransactionAbort(TimeSpan latency)
    {
        Console.WriteLine($"[METRIC] TransactionAbort - Latency: {latency.TotalMilliseconds}ms");
    }

    public void RecordTransactionError(Exception ex)
    {
        Console.WriteLine($"[METRIC] TransactionError - Error: {ex.Message}");
    }

    private static void Log(string metric, string topic, object? value = null)
    {
        Console.WriteLine($"[METRIC] {metric} - Topic: {topic}{(value != null ? $", Value: {value}" : string.Empty)}");
    }
}
