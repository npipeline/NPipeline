using System.Text;
using Confluent.Kafka;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Messaging;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.DeadLetter;

/// <summary>
///     A pipeline dead-letter sink that produces failed items to a Kafka topic, with the error in <c>x-dead-letter-*</c>
///     headers. A message that could not be deserialized (<see cref="MessageFailure" />) is produced with its original value,
///     key and headers, so it can be replayed once fixed; anything else is serialized with the serializer.
/// </summary>
public sealed class KafkaDeadLetterSink : IDeadLetterSink
{
    private readonly IProducer<byte[]?, byte[]> _producer;
    private readonly IMessageSerializer _serializer;
    private readonly string _topic;

    /// <summary>Creates a sink that produces to <paramref name="topic" /> through <paramref name="producer" />, which the caller owns.</summary>
    public KafkaDeadLetterSink(IProducer<byte[]?, byte[]> producer, string topic, IMessageSerializer? serializer = null)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
        _serializer = serializer ?? JsonMessageSerializer.Default;
    }

    /// <inheritdoc />
    public async Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var headers = new Headers
        {
            { "x-dead-letter-reason", Encoding.UTF8.GetBytes(envelope.Error.Message) },
            { "x-dead-letter-exception", Encoding.UTF8.GetBytes(envelope.Error.GetType().FullName ?? envelope.Error.GetType().Name) },
            { "x-dead-letter-node", Encoding.UTF8.GetBytes(envelope.Attribution.DecisionNodeId) },
            { "x-dead-letter-origin-node", Encoding.UTF8.GetBytes(envelope.Attribution.OriginNodeId) },
        };

        var message = new Message<byte[]?, byte[]> { Headers = headers };

        switch (envelope.Item)
        {
            case MessageFailure failure:
                message.Value = failure.Body.ToArray();
                headers.Add("x-dead-letter-source", Encoding.UTF8.GetBytes(failure.Source));
                headers.Add("x-dead-letter-message-id", Encoding.UTF8.GetBytes(failure.MessageId));

                if (failure.Metadata.TryGetValue("Key", out var key) && key is string text)
                    message.Key = Encoding.UTF8.GetBytes(text);

                break;
            case IAcknowledgableMessage received:
                message.Value = _serializer.Serialize(received.Body, new MessageContext(_topic));
                headers.Add("x-dead-letter-message-id", Encoding.UTF8.GetBytes(received.MessageId));

                if (received is IKafkaReceived { Key: { } receivedKey })
                    message.Key = Encoding.UTF8.GetBytes(receivedKey);

                break;
            default:
                message.Value = _serializer.Serialize(envelope.Item, new MessageContext(_topic));
                break;
        }

        _ = await _producer.ProduceAsync(_topic, message, cancellationToken).ConfigureAwait(false);
    }
}
