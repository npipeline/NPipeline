using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.DeadLetter;

/// <summary>
///     Dead-letter sink that sends failed items to a Kafka topic.
///     Implements NPipeline's <see cref="IDeadLetterSink" /> interface for consistency with other connectors.
/// </summary>
public sealed class KafkaDeadLetterSink : IDeadLetterSink
{
    private readonly string _deadLetterTopic;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly IProducer<string, byte[]> _producer;

    /// <summary>
    ///     Initializes a new instance of <see cref="KafkaDeadLetterSink" />.
    /// </summary>
    /// <param name="producer">The Kafka producer to use for publishing dead-letter messages.</param>
    /// <param name="deadLetterTopic">The topic to publish dead-letter messages to.</param>
    public KafkaDeadLetterSink(
        IProducer<string, byte[]> producer,
        string deadLetterTopic)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _deadLetterTopic = deadLetterTopic ?? throw new ArgumentNullException(nameof(deadLetterTopic));

        _jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = false,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    /// <inheritdoc />
    public async Task HandleAsync(
        NPipeline.ErrorHandling.DeadLetterEnvelope envelope,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Get correlation ID from attribution or context
        var correlationId = envelope.Attribution.CorrelationId?.ToString() ?? GetCorrelationId(context);

        // Create Kafka-specific dead-letter envelope with metadata
        var kafkaEnvelope = new Models.DeadLetterEnvelope
        {
            NodeId = envelope.Attribution.DecisionNodeId,
            OriginalItem = envelope.Item,
            ExceptionType = envelope.Error.GetType().FullName,
            ExceptionMessage = envelope.Error.Message,
            StackTrace = envelope.Error.StackTrace,
            Timestamp = DateTime.UtcNow,
            CorrelationId = correlationId,
        };

        // Extract Kafka-specific metadata if item implements IKafkaMessageMetadata
        if (envelope.Item is IKafkaMessageMetadata kafkaMetadata)
        {
            kafkaEnvelope.OriginalTopic = kafkaMetadata.Topic;
            kafkaEnvelope.Partition = kafkaMetadata.Partition;
            kafkaEnvelope.Offset = kafkaMetadata.Offset;
        }

        // Serialize the envelope to JSON
        var payload = JsonSerializer.SerializeToUtf8Bytes(kafkaEnvelope, _jsonOptions);

        // Create the message with headers
        var message = new Message<string, byte[]>
        {
            Key = correlationId ?? Guid.NewGuid().ToString(),
            Value = payload,
            Headers = new Headers
            {
                { "x-dead-letter-reason", Encoding.UTF8.GetBytes(envelope.Error.GetType().Name) },
                { "x-original-node", Encoding.UTF8.GetBytes(envelope.Attribution.DecisionNodeId) },
                { "x-origin-node", Encoding.UTF8.GetBytes(envelope.Attribution.OriginNodeId) },
            },
        };

        // Produce to dead-letter topic (no retries in sink - let NPipeline handle retries)
        await _producer.ProduceAsync(_deadLetterTopic, message, cancellationToken).ConfigureAwait(false);
    }

    private static string? GetCorrelationId(PipelineContext context)
    {
        // Try to get correlation ID from context parameters
        if (context.Parameters.TryGetValue("CorrelationId", out var correlationIdObj) && correlationIdObj is string correlationId)
            return correlationId;

        // Try to get from Items dictionary
        if (context.Items.TryGetValue("CorrelationId", out var itemsCorrelationIdObj) && itemsCorrelationIdObj is string itemsCorrelationId)
            return itemsCorrelationId;

        // Generate a new correlation ID if not present
        return Guid.NewGuid().ToString();
    }
}
