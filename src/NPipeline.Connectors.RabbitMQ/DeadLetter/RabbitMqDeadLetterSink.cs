using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.DeadLetter;

/// <summary>
///     Dead-letter sink that publishes failed items to a RabbitMQ dead-letter exchange
///     with enriched headers. This handles pipeline-level dead lettering (transform failures).
///     Broker-level dead lettering is handled natively via DLX queue arguments.
/// </summary>
public sealed class RabbitMqDeadLetterSink : IDeadLetterSink, IAsyncDisposable
{
    private readonly IRabbitMqConnectionManager _connectionManager;
    private readonly string _deadLetterExchange;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly ILogger _logger;
    private readonly string _routingKey;

    /// <summary>
    ///     Initializes a new instance of <see cref="RabbitMqDeadLetterSink" />.
    /// </summary>
    /// <param name="connectionManager">The connection manager.</param>
    /// <param name="deadLetterExchange">The exchange to publish dead-letter messages to.</param>
    /// <param name="routingKey">The routing key for dead-letter messages. Default is "dead-letter".</param>
    /// <param name="logger">Optional logger.</param>
    public RabbitMqDeadLetterSink(
        IRabbitMqConnectionManager connectionManager,
        string deadLetterExchange,
        string routingKey = "dead-letter",
        ILogger<RabbitMqDeadLetterSink>? logger = null)
    {
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _deadLetterExchange = deadLetterExchange ?? throw new ArgumentNullException(nameof(deadLetterExchange));
        _routingKey = routingKey;
        _logger = logger ?? NullLogger<RabbitMqDeadLetterSink>.Instance;

        _jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = false,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        };
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        // Connection manager disposal handles channel cleanup
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public async Task HandleAsync(
        DeadLetterEnvelope envelope,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var nodeId = envelope.Attribution.DecisionNodeId;
        LogMessages.DeadLetterPublishing(_logger, _deadLetterExchange, nodeId);

        IChannel? channel = null;

        try
        {
            channel = await _connectionManager.GetPooledChannelAsync(cancellationToken).ConfigureAwait(false);

            var body = JsonSerializer.SerializeToUtf8Bytes(envelope.Item, _jsonOptions);

            var properties = new BasicProperties
            {
                ContentType = "application/json",
                Persistent = true,
                MessageId = Guid.NewGuid().ToString(),
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
                Headers = new Dictionary<string, object?>
                {
                    ["x-death-reason"] = Encoding.UTF8.GetBytes(envelope.Error.Message),
                    ["x-death-node"] = Encoding.UTF8.GetBytes(nodeId),
                    ["x-death-origin-node"] = Encoding.UTF8.GetBytes(envelope.Attribution.OriginNodeId),
                    ["x-death-timestamp"] = Encoding.UTF8.GetBytes(DateTimeOffset.UtcNow.ToString("O")),
                    ["x-death-exception-type"] = Encoding.UTF8.GetBytes(envelope.Error.GetType().FullName ?? envelope.Error.GetType().Name),
                },
            };

            if (envelope.Error.StackTrace is not null)
            {
                // Truncate stack trace to avoid oversized headers
                var truncated = envelope.Error.StackTrace.Length > 2048
                    ? envelope.Error.StackTrace[..2048]
                    : envelope.Error.StackTrace;

                properties.Headers["x-death-stack-trace"] = Encoding.UTF8.GetBytes(truncated);
            }

            // Preserve original message metadata if available
            if (envelope.Item is IRabbitMqMessageMetadata sourceMeta)
            {
                properties.Headers["x-original-exchange"] = Encoding.UTF8.GetBytes(sourceMeta.Exchange);
                properties.Headers["x-original-routing-key"] = Encoding.UTF8.GetBytes(sourceMeta.RoutingKey);

                if (sourceMeta is IAcknowledgableMessage { MessageId: not null } ackMsg)
                    properties.Headers["x-original-message-id"] = Encoding.UTF8.GetBytes(ackMsg.MessageId);
            }

            // BasicPublishAsync waits for broker confirmation when
            // PublisherConfirmationTrackingEnabled is set on the channel.
            await channel.BasicPublishAsync(
                _deadLetterExchange,
                _routingKey,
                false,
                properties,
                body,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            LogMessages.DeadLetterPublishFailed(_logger, ex, _deadLetterExchange);
            throw;
        }
        finally
        {
            if (channel is not null)
                _connectionManager.ReturnChannel(channel);
        }
    }
}
