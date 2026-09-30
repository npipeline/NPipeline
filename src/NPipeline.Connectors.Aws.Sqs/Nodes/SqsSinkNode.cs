using System.Globalization;
using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Internal;
using NPipeline.Connectors.Aws.Sqs.Models;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Nodes;

/// <summary>
///     Sends to an SQS queue with <c>SendMessageBatch</c>, up to ten messages and 256 KB a request. Written through
///     <c>Acknowledging()</c>, each received message is acknowledged once SQS has its body, and one SQS rejects is handled
///     as <see cref="SqsWriteOptions{T}.FailedMessages" /> says. Create one with <see cref="SqsConnector.Sink{T}" />.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class SqsSinkNode<T> : SinkNode<T>, IMessageSink<T>, IAsyncDisposable
{
    // SQS's limit for a batch request, with room for the request's own fields.
    private const int MaxBatchBytes = 256 * 1024 - 4 * 1024;

    private readonly IAmazonSQS _client;
    private readonly SqsWriteOptions<T> _options;
    private readonly bool _ownsClient;
    private ILogger _logger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

    /// <summary>Creates a sink and validates <paramref name="options" />.</summary>
    public SqsSinkNode(SqsWriteOptions<T> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        (_client, _ownsClient) = SqsClientFactory.For(options);
    }

    /// <summary>Disposes the client, if the sink created it.</summary>
    public ValueTask DisposeAsync()
    {
        if (_ownsClient)
            _client.Dispose();

        return ValueTask.CompletedTask;
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

        _logger = context.Observability.LoggerFactory.CreateLogger(typeof(SqsSinkNode<T>).FullName ?? nameof(SqsSinkNode<T>));
        var deadLetters = OpenDeadLetterChannel(context);

        await foreach (var items in input.BatchAsync(_options.BatchSize, _options.BatchLinger, cancellationToken).ConfigureAwait(false))
        {
            var batch = new List<Outgoing>(items.Count);
            var bytes = 0;

            foreach (var item in items)
            {
                var value = body(item);
                var outgoing = new Outgoing(value, source(item), Encoding.UTF8.GetString(_options.Serializer.Serialize(value, new MessageContext(_options.QueueUrl))));
                var size = Encoding.UTF8.GetByteCount(outgoing.Text) + AttributeBytes(outgoing.Source);

                // A batch over SQS's size limit is sent in parts.
                if (batch.Count > 0 && bytes + size > MaxBatchBytes)
                {
                    await SendAsync(batch, deadLetters, cancellationToken).ConfigureAwait(false);
                    batch.Clear();
                    bytes = 0;
                }

                batch.Add(outgoing);
                bytes += size;
            }

            await SendAsync(batch, deadLetters, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task SendAsync(List<Outgoing> batch, DeadLetterChannel deadLetters, CancellationToken cancellationToken)
    {
        var errors = new Exception?[batch.Count];
        var request = new SendMessageBatchRequest { QueueUrl = _options.QueueUrl, Entries = [.. batch.Select(Entry)] };

        try
        {
            var response = await _client.SendMessageBatchAsync(request, cancellationToken).ConfigureAwait(false);

            foreach (var failed in response.Failed ?? [])
            {
                var index = int.Parse(failed.Id, CultureInfo.InvariantCulture);
                SqsLogMessages.SendFailed(_logger, index, _options.QueueUrl, failed.Code, failed.Message);
                errors[index] = new AmazonSQSException($"SQS rejected the message ({failed.Code}): {failed.Message}") { ErrorCode = failed.Code };
            }
        }
        catch (Exception ex) when (ex is AmazonSQSException or HttpRequestException && !cancellationToken.IsCancellationRequested)
        {
            // The SDK already retried the request; every message in it failed.
            Array.Fill(errors, ex);
        }

        var sent = 0;
        var acknowledgements = new List<Task>();

        for (var i = 0; i < batch.Count; i++)
        {
            if (errors[i] is not null)
                continue;

            sent++;

            if (batch[i].Source is { } from)
                acknowledgements.Add(from.AcknowledgeAsync(cancellationToken));
        }

        // Settled together: a broker that settles each message with its own request (Service Bus) takes one round trip, not one per message.
        await Task.WhenAll(acknowledgements).ConfigureAwait(false);
        ConnectorDiagnostics.RecordRowsWritten(SqsConnector.Name, SqsConnector.Name, sent);

        for (var i = 0; i < batch.Count; i++)
        {
            if (errors[i] is not { } error)
                continue;

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
                    throw error;
            }
        }
    }

    private SendMessageBatchRequestEntry Entry(Outgoing outgoing, int index)
    {
        var entry = new SendMessageBatchRequestEntry(index.ToString(CultureInfo.InvariantCulture), outgoing.Text);

        // FIFO queues reject a per-message delay, so it is only sent when asked for.
        if (_options.Delay > TimeSpan.Zero)
            entry.DelaySeconds = (int)_options.Delay.TotalSeconds;

        var received = outgoing.Source as ISqsReceived;

        if (_options.MessageAttributes.Count > 0 || (_options.CopyMessageAttributes && received is { Attributes.Count: > 0 }))
        {
            entry.MessageAttributes = [];

            if (_options.CopyMessageAttributes && received is not null)
            {
                foreach (var (key, value) in received.Attributes)
                {
                    entry.MessageAttributes[key] = value;
                }
            }

            foreach (var (key, value) in _options.MessageAttributes)
            {
                entry.MessageAttributes[key] = value;
            }
        }

        if (_options.MessageGroupId is { } group)
            entry.MessageGroupId = group(outgoing.Body);

        if (_options.DeduplicationId is { } deduplication)
            entry.MessageDeduplicationId = deduplication(outgoing.Body);
        else if (entry.MessageGroupId is not null && outgoing.Source is { } source)
            entry.MessageDeduplicationId = source.MessageId;

        return entry;
    }

    /// <summary>The bytes a message's attributes add to a request: SQS counts their names, types and values against its limit.</summary>
    private int AttributeBytes(IAcknowledgableMessage? source)
    {
        var total = 0;

        void Add(IEnumerable<KeyValuePair<string, MessageAttributeValue>> attributes)
        {
            foreach (var (key, value) in attributes)
            {
                total += Encoding.UTF8.GetByteCount(key) + Encoding.UTF8.GetByteCount(value.DataType ?? string.Empty) +
                         (value.StringValue is { } text ? Encoding.UTF8.GetByteCount(text) : 0) + (int)(value.BinaryValue?.Length ?? 0);
            }
        }

        Add(_options.MessageAttributes);

        if (_options.CopyMessageAttributes && source is ISqsReceived received)
            Add(received.Attributes);

        return total;
    }

    private sealed record Outgoing(T Body, IAcknowledgableMessage? Source, string Text);
}
