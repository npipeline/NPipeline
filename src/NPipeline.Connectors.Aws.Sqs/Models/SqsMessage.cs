using Amazon.SQS;
using Amazon.SQS.Model;
using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors.Aws.Sqs.Models;

internal interface IAwsSqsAcknowledgableMessage
{
    void MarkAcknowledged();
}

/// <summary>
///     SQS-specific implementation of IAcknowledgableMessage that wraps an SQS message
///     with acknowledgment capability. Supports both individual and batch acknowledgment.
/// </summary>
/// <typeparam name="T">The deserialized message body type.</typeparam>
public sealed class SqsMessage<T> : IAcknowledgableMessage<T>, IAwsSqsAcknowledgableMessage
{
    private readonly object _ackLock = new();
    private readonly Func<CancellationToken, Task> _acknowledgeCallback;
    private readonly Dictionary<string, object> _metadata;
    private Task? _ackTask;
    private volatile bool _isAcknowledged;

    /// <summary>
    ///     Internal constructor used by SqsSourceNode with direct acknowledgment.
    /// </summary>
    internal SqsMessage(
        T body,
        string messageId,
        string receiptHandle,
        IDictionary<string, MessageAttributeValue> attributes,
        DateTime timestamp,
        IAmazonSQS sqsClient,
        string queueUrl)
    {
        Body = body;
        MessageId = messageId;
        ReceiptHandle = receiptHandle;
        Attributes = attributes ?? new Dictionary<string, MessageAttributeValue>();
        Timestamp = timestamp;
        _acknowledgeCallback = ct => AcknowledgeDirectlyAsync(sqsClient, queueUrl, receiptHandle, ct);
        _metadata = BuildMetadata();
    }

    /// <summary>
    ///     Internal constructor used by SqsSourceNode with batch acknowledgment callback.
    /// </summary>
    internal SqsMessage(
        T body,
        string messageId,
        string receiptHandle,
        IDictionary<string, MessageAttributeValue> attributes,
        DateTime timestamp,
        Func<CancellationToken, Task> acknowledgeCallback)
    {
        Body = body;
        MessageId = messageId;
        ReceiptHandle = receiptHandle;
        Attributes = attributes ?? new Dictionary<string, MessageAttributeValue>();
        Timestamp = timestamp;
        _acknowledgeCallback = acknowledgeCallback;
        _metadata = BuildMetadata();
    }

    /// <summary>
    ///     Gets the receipt handle used to delete the message.
    /// </summary>
    public string ReceiptHandle { get; }

    /// <summary>
    ///     Gets the message attributes (metadata).
    /// </summary>
    public IDictionary<string, MessageAttributeValue> Attributes { get; }

    /// <summary>
    ///     Gets the timestamp when the message was sent.
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    ///     Gets the deserialized message body.
    /// </summary>
    public T Body { get; }

    object IAcknowledgableMessage.Body => Body!;

    /// <summary>
    ///     Gets the SQS message ID.
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    ///     Gets a value indicating whether this message has been acknowledged.
    /// </summary>
    public bool IsAcknowledged => _isAcknowledged;

    /// <summary>
    ///     Gets metadata associated with the message.
    /// </summary>
    public IReadOnlyDictionary<string, object> Metadata => _metadata;

    /// <summary>
    ///     Acknowledges the message by deleting it from the SQS queue.
    ///     This method is idempotent - calling it multiple times has no effect.
    /// </summary>
    public async Task AcknowledgeAsync(CancellationToken cancellationToken = default)
    {
        Task ackTask;

        lock (_ackLock)
        {
            if (_isAcknowledged)
                return;

            _ackTask ??= _acknowledgeCallback(cancellationToken);
            ackTask = _ackTask;
        }

        try
        {
            await ackTask.ConfigureAwait(false);

            lock (_ackLock)
            {
                _isAcknowledged = true;
                _ackTask = Task.CompletedTask;
            }
        }
        catch
        {
            lock (_ackLock)
            {
                if (ReferenceEquals(_ackTask, ackTask))
                    _ackTask = null;
            }

            throw;
        }
    }

    /// <summary>
    ///     Creates a new SqsMessage with the provided body while preserving acknowledgment behavior.
    /// </summary>
    /// <typeparam name="TNew">The new body type.</typeparam>
    /// <param name="body">The new message body.</param>
    /// <returns>A new SqsMessage with the same acknowledgment callback.</returns>
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body)
    {
        return new SqsMessage<TNew>(
            body,
            MessageId,
            ReceiptHandle,
            Attributes,
            Timestamp,
            _acknowledgeCallback);
    }

    void IAwsSqsAcknowledgableMessage.MarkAcknowledged()
    {
        lock (_ackLock)
        {
            _isAcknowledged = true;
            _ackTask = Task.CompletedTask;
        }
    }

    private static async Task AcknowledgeDirectlyAsync(
        IAmazonSQS sqsClient,
        string queueUrl,
        string receiptHandle,
        CancellationToken cancellationToken)
    {
        await sqsClient.DeleteMessageAsync(
            queueUrl,
            receiptHandle,
            cancellationToken).ConfigureAwait(false);
    }

    private Dictionary<string, object> BuildMetadata()
    {
        var metadata = new Dictionary<string, object>
        {
            ["Timestamp"] = Timestamp,
            ["ReceiptHandle"] = ReceiptHandle,
        };

        // Add message attributes to metadata
        foreach (var attr in Attributes)
        {
            object value = attr.Value.DataType switch
            {
                "String" => attr.Value.StringValue,
                "Number" => attr.Value.StringValue,
                "Binary" => attr.Value.BinaryValue,
                _ => attr.Value.StringValue,
            };

            metadata[$"Attribute.{attr.Key}"] = value;
        }

        return metadata;
    }
}
