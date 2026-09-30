using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.DeadLetter;

/// <summary>
///     A pipeline dead-letter sink that publishes failed items to a RabbitMQ exchange, with the error in <c>x-death-*</c>
///     headers. A message that could not be deserialized (<see cref="MessageFailure" />) is published with its original
///     body and headers, so it can be published again once fixed; anything else is serialized with the serializer.
/// </summary>
public sealed class RabbitMqDeadLetterSink : IDeadLetterSink
{
    private readonly IRabbitMqConnectionManager _connection;
    private readonly string _exchange;
    private readonly ILogger _logger;
    private readonly string _routingKey;
    private readonly IMessageSerializer _serializer;

    /// <summary>Creates a sink that publishes to <paramref name="exchange" /> with <paramref name="routingKey" />.</summary>
    public RabbitMqDeadLetterSink(IRabbitMqConnectionManager connection, string exchange, string routingKey = "dead-letter",
        IMessageSerializer? serializer = null, ILogger<RabbitMqDeadLetterSink>? logger = null)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _exchange = exchange ?? throw new ArgumentNullException(nameof(exchange));
        _routingKey = routingKey ?? throw new ArgumentNullException(nameof(routingKey));
        _serializer = serializer ?? JsonMessageSerializer.Default;
        _logger = logger ?? NullLogger<RabbitMqDeadLetterSink>.Instance;
    }

    /// <inheritdoc />
    public async Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var nodeId = envelope.Attribution.DecisionNodeId;
        LogMessages.DeadLetterPublishing(_logger, _exchange, nodeId);

        var headers = new Dictionary<string, object?>
        {
            ["x-death-reason"] = Encoding.UTF8.GetBytes(envelope.Error.Message),
            ["x-death-node"] = Encoding.UTF8.GetBytes(nodeId),
            ["x-death-origin-node"] = Encoding.UTF8.GetBytes(envelope.Attribution.OriginNodeId),
            ["x-death-timestamp"] = Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O")),
            ["x-death-exception-type"] = Encoding.UTF8.GetBytes(envelope.Error.GetType().FullName ?? envelope.Error.GetType().Name),
        };

        var properties = new BasicProperties { Persistent = true, MessageId = Guid.NewGuid().ToString("N"), Headers = headers };
        ReadOnlyMemory<byte> body;

        switch (envelope.Item)
        {
            case MessageFailure failure:
                body = failure.Body;
                properties.MessageId = failure.MessageId;
                headers["x-original-source"] = Encoding.UTF8.GetBytes(failure.Source);
                break;
            case IAcknowledgableMessage message:
                body = _serializer.Serialize(message.Body, new MessageContext(_exchange));
                properties.ContentType = _serializer.ContentType;
                properties.MessageId = message.MessageId;

                if (message is IRabbitMqReceived { Properties: var received })
                {
                    properties.CorrelationId = received.CorrelationId;

                    foreach (var (key, value) in received.Headers ?? new Dictionary<string, object?>())
                    {
                        headers.TryAdd(key, value);
                    }
                }

                break;
            default:
                body = _serializer.Serialize(envelope.Item, new MessageContext(_exchange));
                properties.ContentType = _serializer.ContentType;
                break;
        }

        var channel = await _connection.GetPooledChannelAsync(true, cancellationToken).ConfigureAwait(false);

        try
        {
            await channel.BasicPublishAsync(_exchange, _routingKey, false, properties, body, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            LogMessages.DeadLetterPublishFailed(_logger, ex, _exchange);
            throw;
        }
        finally
        {
            _connection.ReturnChannel(channel);
        }
    }
}
