using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
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
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Nodes;

/// <summary>
///     Receives from an SQS queue with long polling. Each message is handed on as an <see cref="SqsMessage{T}" />;
///     acknowledging it deletes it, in batches of up to ten. When the read ends, the source keeps its client until the
///     messages handed on are settled (up to <see cref="SqsReadOptions.SettleTimeout" />) and their deletes sent. Create one
///     with <see cref="SqsConnector.Source{T}" />.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class SqsSourceNode<T> : SourceNode<SqsMessage<T>>, IAsyncDisposable
{
    private static readonly IReadOnlyDictionary<string, MessageAttributeValue> NoAttributes = new Dictionary<string, MessageAttributeValue>();
    private static readonly IReadOnlyDictionary<string, string> NoSystemAttributes = new Dictionary<string, string>();

    private readonly List<Task> _closing = [];
    private readonly MessageDecoder<T> _decoder;
    private readonly CancellationTokenSource _disposing = new();
    private bool _disposed;
    private readonly SqsReadOptions _options;

    /// <summary>Creates a source and validates <paramref name="options" />.</summary>
    public SqsSourceNode(SqsReadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _decoder = new MessageDecoder<T>(SqsConnector.Name, options.QueueUrl, options.Serializer, options.RowErrorHandler, options.RawExcerptLength);
    }

    /// <summary>Sends the outstanding deletes of finished reads and releases their clients, without waiting for unsettled messages.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _disposing.CancelAsync().ConfigureAwait(false);

        Task[] closing;

        lock (_closing)
        {
            closing = [.. _closing];
        }

        await Task.WhenAll(closing).ConfigureAwait(false);
        _disposing.Dispose();
    }

    /// <inheritdoc />
    public override IDataStream<SqsMessage<T>> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new DataStream<SqsMessage<T>>(ReceiveAsync(context, cancellationToken), $"SqsSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<SqsMessage<T>> ReceiveAsync(PipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var logger = context.Observability.LoggerFactory.CreateLogger(typeof(SqsSourceNode<T>).FullName ?? nameof(SqsSourceNode<T>));
        var deadLetters = OpenDeadLetterChannel(context);
        var inFlight = new InFlightMessages();
        var (client, owned) = SqsClientFactory.For(_options);
        var deleter = new SqsDeleter(client, _options.QueueUrl, _options.DeleteLinger, logger);
        long sequence = 0;

        try
        {
            while (true)
            {
                // Cancellation surfaces as OperationCanceledException rather than ending the stream as if it had drained.
                cancellationToken.ThrowIfCancellationRequested();

                // A failed receive is not retried here: the SDK client already retried it.
                var response = await client.ReceiveMessageAsync(Request(), cancellationToken).ConfigureAwait(false);

                // The AWS SDK v4 returns null, not an empty list, when no message arrived.
                foreach (var received in response.Messages ?? [])
                {
                    sequence++;

                    if (!TryDecode(received.Body, out var body, out var error))
                    {
                        SqsLogMessages.DeserializationFailed(logger, error, received.MessageId, _options.QueueUrl);

                        // Throws for Fail: the message stays in the queue and is delivered again after its visibility timeout.
                        _ = await _decoder.HandleFailureAsync(error, Encoding.UTF8.GetBytes(received.Body), received.MessageId, sequence, Metadata(received),
                            deadLetters, cancellationToken).ConfigureAwait(false);

                        deleter.Enqueue(received.ReceiptHandle);
                        ConnectorDiagnostics.RecordMessagesSettled(SqsConnector.Name, "rejected");
                        continue;
                    }

                    inFlight.Add();
                    ConnectorDiagnostics.RecordRowsRead(SqsConnector.Name, SqsConnector.Name, 1);
                    yield return Message(body, received, client, deleter, inFlight);
                }
            }
        }
        finally
        {
            lock (_closing)
            {
                _closing.Add(CloseWhenSettledAsync(client, owned, deleter, inFlight));
            }
        }
    }

    private ReceiveMessageRequest Request() =>
        new()
        {
            QueueUrl = _options.QueueUrl,
            MaxNumberOfMessages = _options.MaxMessages,
            WaitTimeSeconds = (int)_options.WaitTime.TotalSeconds,
            VisibilityTimeout = _options.VisibilityTimeout is { } visibility ? (int)visibility.TotalSeconds : null,
            MessageSystemAttributeNames = ["All"],
            MessageAttributeNames = ["All"],
        };

    private bool TryDecode(string text, out T body, [NotNullWhen(false)] out Exception? error)
    {
        var bytes = ArrayPool<byte>.Shared.Rent(Encoding.UTF8.GetMaxByteCount(text.Length));

        try
        {
            var length = Encoding.UTF8.GetBytes(text, bytes);
            var decoded = _decoder.TryDecode(bytes.AsSpan(0, length), out var value, out error);
            body = value!;
            return decoded;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }

    private SqsMessage<T> Message(T body, Message received, IAmazonSQS client, SqsDeleter deleter, InFlightMessages inFlight)
    {
        var receiptHandle = received.ReceiptHandle;

        var settlement = new MessageSettlement(
            _ =>
            {
                deleter.Enqueue(receiptHandle);
                inFlight.Remove();
                ConnectorDiagnostics.RecordMessagesSettled(SqsConnector.Name, "acknowledged");
                return Task.CompletedTask;
            },
            async (requeue, ct) =>
            {
                try
                {
                    if (requeue)
                        _ = await client.ChangeMessageVisibilityAsync(_options.QueueUrl, receiptHandle, 0, ct).ConfigureAwait(false);
                    else
                        deleter.Enqueue(receiptHandle);

                    ConnectorDiagnostics.RecordMessagesSettled(SqsConnector.Name, requeue ? "requeued" : "rejected");
                }
                finally
                {
                    inFlight.Remove();
                }
            });

        return new SqsMessage<T>(body, received.MessageId, receiptHandle, _options.QueueUrl,
            received.MessageAttributes is { Count: > 0 } attributes ? new Dictionary<string, MessageAttributeValue>(attributes) : NoAttributes,
            received.Attributes is { Count: > 0 } system ? new Dictionary<string, string>(system) : NoSystemAttributes,
            settlement);
    }

    private async Task CloseWhenSettledAsync(IAmazonSQS client, bool owned, SqsDeleter deleter, InFlightMessages inFlight)
    {
        try
        {
            _ = await inFlight.WhenSettledAsync(_options.SettleTimeout, _disposing.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Disposed: send what is acknowledged now.
        }

        await deleter.CompleteAsync().ConfigureAwait(false);

        if (owned)
            client.Dispose();
    }

    private Dictionary<string, object> Metadata(Message received)
    {
        var metadata = new Dictionary<string, object> { ["QueueUrl"] = _options.QueueUrl };

        foreach (var (key, value) in received.Attributes ?? [])
        {
            metadata[key] = value;
        }

        foreach (var (key, value) in received.MessageAttributes ?? [])
        {
            metadata[$"Attribute.{key}"] = value.StringValue ?? (object?)value.BinaryValue ?? string.Empty;
        }

        return metadata;
    }
}
