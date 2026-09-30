using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Connectors.RabbitMQ.Topology;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Nodes;

/// <summary>
///     Publishes to a RabbitMQ exchange. Messages go in batches of <see cref="RabbitMqWriteOptions{T}.BatchSize" />, each
///     batch's publishes issued together and their confirms awaited together. Written through <c>Acknowledging()</c>, each
///     received message is acknowledged once its body is confirmed, and one that cannot be published is handled as
///     <see cref="RabbitMqWriteOptions{T}.FailedMessages" /> says. Create one with <see cref="RabbitMqConnector.Sink{T}" />.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class RabbitMqSinkNode<T> : SinkNode<T>, IMessageSink<T>
{
    private readonly RabbitMqWriteOptions<T> _options;
    private readonly Resilience? _retries;
    private ILogger _logger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

    /// <summary>Creates a sink and validates <paramref name="options" />.</summary>
    public RabbitMqSinkNode(RabbitMqWriteOptions<T> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _retries = options.Resilience.Attempts > 1
            ? (options.Resilience with { Attempts = options.Resilience.Attempts - 1 }).WithListener(OnResilienceEvent)
            : null;
    }

    /// <inheritdoc />
    public override Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken) =>
        WriteAsync(input, static item => item, static _ => null, context, cancellationToken);

    /// <inheritdoc />
    public Task ConsumeMessagesAsync(IDataStream<IAcknowledgableMessage<T>> input, PipelineContext context, CancellationToken cancellationToken) =>
        WriteAsync(input, static message => message.Body, static message => message, context, cancellationToken);

    private async Task WriteAsync<TItem>(IDataStream<TItem> input, Func<TItem, T> body, Func<TItem, IAcknowledgableMessage?> source,
        PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);

        _logger = context.Observability.LoggerFactory.CreateLogger(typeof(RabbitMqSinkNode<T>).FullName ?? nameof(RabbitMqSinkNode<T>));
        var deadLetters = OpenDeadLetterChannel(context);
        await DeclareAsync(cancellationToken).ConfigureAwait(false);

        await foreach (var batch in input.BatchAsync(_options.BatchSize, _options.BatchLinger, cancellationToken).ConfigureAwait(false))
        {
            var outgoing = new Outgoing[batch.Count];
            var i = 0;

            foreach (var item in batch)
            {
                var value = body(item);
                var from = source(item);
                var routingKey = _options.RoutingKeySelector?.Invoke(value) ?? _options.RoutingKey;
                var bytes = _options.Serializer.Serialize(value, new MessageContext(_options.Exchange.Length > 0 ? _options.Exchange : routingKey));
                outgoing[i++] = new Outgoing(value, from, routingKey, Properties(from), bytes);
            }

            await PublishBatchAsync(outgoing, deadLetters, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task PublishBatchAsync(Outgoing[] batch, DeadLetterChannel deadLetters, CancellationToken cancellationToken)
    {
        var errors = new Exception?[batch.Length];
        var channel = await _options.Connection.GetPooledChannelAsync(_options.PublisherConfirms, cancellationToken).ConfigureAwait(false);

        try
        {
            // Issue every publish, then await the confirms: the broker confirms a batch in about one round trip.
            using var confirms = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            if (_options.PublisherConfirms)
                confirms.CancelAfter(_options.ConfirmTimeout);

            // Each is awaited exactly once below, as RabbitMQ.Client's batch-confirm pattern does.
#pragma warning disable CA2012
            var publishes = new ValueTask[batch.Length];

            for (var i = 0; i < batch.Length; i++)
            {
                publishes[i] = Publish(channel, batch[i], confirms.Token, cancellationToken);
            }
#pragma warning restore CA2012

            for (var i = 0; i < batch.Length; i++)
            {
                try
                {
                    await publishes[i].ConfigureAwait(false);
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    errors[i] = ex;
                }
            }
        }
        finally
        {
            _options.Connection.ReturnChannel(channel);
        }

        // The batch was each message's first attempt. A failure the classifier calls transient gets the policy's remaining
        // attempts, one message at a time, on a fresh channel if the old one broke, after the policy's first backoff.
        var retried = false;

        for (var i = 0; i < batch.Length; i++)
        {
            if (errors[i] is not { } first || _retries is null || _options.Resilience.Classifier.ClassifyException(first).Kind == VerdictKind.Permanent)
                continue;

            if (!retried)
            {
                await Task.Delay(_options.Resilience.Backoff.TransientBase, cancellationToken).ConfigureAwait(false);
                retried = true;
            }

            try
            {
                await _retries.RunAsync(static (state, ct) => state.Node.PublishOnceAsync(state.Message, ct), (Node: this, Message: batch[i]), cancellationToken)
                    .ConfigureAwait(false);

                errors[i] = null;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                errors[i] = ex;
            }
        }

        // Settle what was published before dealing with failures: RabbitMQ settles messages one by one, so a published
        // message left unacknowledged would only be delivered and published again.
        var published = 0;
        var acknowledgements = new List<Task>();

        for (var i = 0; i < batch.Length; i++)
        {
            if (errors[i] is not null)
                continue;

            published++;

            if (batch[i].Source is { } from)
                acknowledgements.Add(from.AcknowledgeAsync(cancellationToken));
        }

        // Settled together: a broker that settles each message with its own request (Service Bus) takes one round trip, not one per message.
        await Task.WhenAll(acknowledgements).ConfigureAwait(false);
        ConnectorDiagnostics.RecordRowsWritten(RabbitMqSourceNode<T>.ConnectorName, RabbitMqSourceNode<T>.ConnectorName, published);

        for (var i = 0; i < batch.Length; i++)
        {
            if (errors[i] is not { } error)
                continue;

            LogMessages.PublishFailed(_logger, error, _options.Exchange, error.Message);

            switch (_options.FailedMessages)
            {
                case FailedMessageAction.Requeue:
                    // A plain body has no source message to requeue, so its failure fails the write.
                    if (batch[i].Source is not { } requeue)
                        throw error;

                    await requeue.RejectAsync(true, cancellationToken).ConfigureAwait(false);
                    break;
                case FailedMessageAction.DeadLetter:
                    await deadLetters.SendAsync(batch[i].Body!, error, cancellationToken).ConfigureAwait(false);

                    if (batch[i].Source is { } handled)
                        await handled.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);

                    break;
                default:
                    // The failed message and the rest of the batch's failures stay unsettled, so they are delivered again.
                    throw error;
            }
        }
    }

    private async ValueTask PublishOnceAsync(Outgoing message, CancellationToken cancellationToken)
    {
        var channel = await _options.Connection.GetPooledChannelAsync(_options.PublisherConfirms, cancellationToken).ConfigureAwait(false);

        try
        {
            using var confirm = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            if (_options.PublisherConfirms)
                confirm.CancelAfter(_options.ConfirmTimeout);

            await Publish(channel, message, confirm.Token, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _options.Connection.ReturnChannel(channel);
        }
    }

    /// <summary>Publishes and, with confirms, waits for the broker's until <paramref name="confirm" /> fires.</summary>
    private async ValueTask Publish(IChannel channel, Outgoing message, CancellationToken confirm, CancellationToken cancellationToken)
    {
        try
        {
            await channel.BasicPublishAsync(_options.Exchange, message.RoutingKey, _options.Mandatory, message.Properties, message.Bytes, confirm)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (confirm.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            // The confirm deadline passed, which the resilience policy treats as a timeout to retry.
            throw new TimeoutException($"The broker did not confirm the publish to exchange '{_options.Exchange}' within {_options.ConfirmTimeout}.", ex);
        }
    }

    private async Task DeclareAsync(CancellationToken cancellationToken)
    {
        if (_options.Topology is null)
            return;

        var channel = await _options.Connection.GetPooledChannelAsync(_options.PublisherConfirms, cancellationToken).ConfigureAwait(false);

        try
        {
            await TopologyDeclarer.DeclareSinkExchangeAsync(channel, _options.Exchange, _options.Topology, _logger, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _options.Connection.ReturnChannel(channel);
        }
    }

    private BasicProperties Properties(IAcknowledgableMessage? source)
    {
        var properties = new BasicProperties
        {
            ContentType = _options.ContentType ?? _options.Serializer.ContentType,
            Persistent = _options.Persistent,
            MessageId = Guid.NewGuid().ToString("N"),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            AppId = _options.AppId,
        };

        if (!_options.CopyMessageProperties || source is null)
            return properties;

        // The message keeps its id across the hop, so a consumer can drop a duplicate a retry produced.
        properties.MessageId = source.MessageId;

        if (source is IRabbitMqReceived { Properties: var received })
        {
            properties.CorrelationId = received.CorrelationId;
            properties.Type = received.Type;
            properties.Priority = received.Priority;

            if (received.Headers is { Count: > 0 } headers)
                properties.Headers = new Dictionary<string, object?>(headers);
        }

        return properties;
    }

    private void OnResilienceEvent(CallEvent callEvent)
    {
        if (callEvent.Kind == CallEventKind.Retrying)
        {
            LogMessages.PublishRetrying(_logger, callEvent.Exception, _options.Exchange, (callEvent.Delay ?? TimeSpan.Zero).TotalMilliseconds,
                callEvent.AttemptNumber, _options.Resilience.Attempts);
        }
    }

    private sealed record Outgoing(T Body, IAcknowledgableMessage? Source, string RoutingKey, BasicProperties Properties, byte[] Bytes);
}
