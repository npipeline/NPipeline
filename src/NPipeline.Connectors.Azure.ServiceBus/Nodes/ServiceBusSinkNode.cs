using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Internal;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using SdkMessage = Azure.Messaging.ServiceBus.ServiceBusMessage;

namespace NPipeline.Connectors.Azure.ServiceBus.Nodes;

/// <summary>
///     Sends to a Service Bus queue or topic, in batches of up to <see cref="ServiceBusWriteOptions{T}.BatchSize" /> that fit
///     a Service Bus message batch. Written through <c>Acknowledging()</c>, each received message is settled once its body
///     is sent, and a message from Service Bus keeps its id, session, correlation id, subject and properties. Create one
///     with <see cref="ServiceBusConnector.Sink{T}" />.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class ServiceBusSinkNode<T> : SinkNode<T>, IMessageSink<T>, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly ServiceBusWriteOptions<T> _options;
    private readonly bool _ownsClient;
    private readonly ServiceBusSender _sender;
    private ILogger _logger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

    /// <summary>Creates a sink and validates <paramref name="options" />.</summary>
    public ServiceBusSinkNode(ServiceBusWriteOptions<T> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        (_client, _ownsClient) = ServiceBusClients.For(options);

        // The sink's own sender, whatever client it shares, so disposing the sink never closes a sender someone else uses.
        _sender = _client.CreateSender(options.Entity);
    }

    /// <summary>Closes the sink's sender, and its client if it created one.</summary>
    public async ValueTask DisposeAsync()
    {
        await _sender.DisposeAsync().ConfigureAwait(false);

        if (_ownsClient)
            await _client.DisposeAsync().ConfigureAwait(false);
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

        _logger = context.Observability.LoggerFactory.CreateLogger(typeof(ServiceBusSinkNode<T>).FullName ?? nameof(ServiceBusSinkNode<T>));
        var deadLetters = OpenDeadLetterChannel(context);

        await foreach (var items in input.BatchAsync(_options.BatchSize, _options.BatchLinger, cancellationToken).ConfigureAwait(false))
        {
            var outgoing = items.Select(item =>
            {
                var value = body(item);
                var from = source(item);
                return new Outgoing(value, from, Message(value, from));
            }).ToList();

            await SendAsync(outgoing, deadLetters, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task SendAsync(List<Outgoing> outgoing, DeadLetterChannel deadLetters, CancellationToken cancellationToken)
    {
        var errors = new Exception?[outgoing.Count];
        var start = 0;

        // As many as fit each Service Bus batch; one too large for any batch is sent, and fails, alone.
        while (start < outgoing.Count)
        {
            using var batch = await _sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
            var end = start;

            while (end < outgoing.Count && batch.TryAddMessage(outgoing[end].Message))
            {
                end++;
            }

            try
            {
                if (end == start)
                {
                    await _sender.SendMessageAsync(outgoing[start].Message, cancellationToken).ConfigureAwait(false);
                    end = start + 1;
                }
                else
                {
                    await _sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is ServiceBusException && !cancellationToken.IsCancellationRequested)
            {
                ServiceBusLogMessages.SendFailed(_logger, ex, Math.Max(1, end - start), _options.Entity);
                end = Math.Max(end, start + 1);

                for (var i = start; i < end; i++)
                {
                    errors[i] = ex;
                }
            }

            start = end;
        }

        var sent = 0;
        var acknowledgements = new List<Task>();

        for (var i = 0; i < outgoing.Count; i++)
        {
            if (errors[i] is not null)
                continue;

            sent++;

            if (outgoing[i].Source is { } from)
                acknowledgements.Add(from.AcknowledgeAsync(cancellationToken));
        }

        // Settled together: a broker that settles each message with its own request (Service Bus) takes one round trip, not one per message.
        await Task.WhenAll(acknowledgements).ConfigureAwait(false);
        ConnectorDiagnostics.RecordRowsWritten(ServiceBusConnector.Name, ServiceBusConnector.Name, sent);

        for (var i = 0; i < outgoing.Count; i++)
        {
            if (errors[i] is not { } error)
                continue;

            switch (_options.FailedMessages)
            {
                case FailedMessageAction.Requeue:
                    // A plain body has no source message to requeue, so its failure fails the write.
                    if (outgoing[i].Source is not { } requeue)
                        throw error;

                    await requeue.RejectAsync(true, cancellationToken).ConfigureAwait(false);
                    break;
                case FailedMessageAction.DeadLetter:
                    await deadLetters.SendAsync(outgoing[i].Body!, error, cancellationToken).ConfigureAwait(false);

                    if (outgoing[i].Source is { } handled)
                        await handled.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);

                    break;
                default:
                    throw error;
            }
        }
    }

    private SdkMessage Message(T value, IAcknowledgableMessage? source)
    {
        var bytes = BinaryData.FromBytes(_options.Serializer.Serialize(value, new MessageContext(_options.Entity)));

        // A copy of a received message keeps its id, session, correlation id, subject, time to live and properties.
        var message = _options.CopyMessageProperties && source is IServiceBusReceived { Received: var received }
            ? new SdkMessage(received) { Body = bytes }
            : new SdkMessage(bytes);

        message.ContentType = _options.Serializer.ContentType;

        if (_options.MessageId is { } id)
            message.MessageId = id(value);

        if (_options.SessionId is { } session)
            message.SessionId = session(value);

        if (_options.Subject is { } subject)
            message.Subject = subject(value);

        if (_options.TimeToLive is { } ttl)
            message.TimeToLive = ttl;

        return message;
    }

    private sealed record Outgoing(T Body, IAcknowledgableMessage? Source, SdkMessage Message);
}
