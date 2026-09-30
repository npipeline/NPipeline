using Amazon.SQS.Model;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Aws.Sqs.Models;

/// <summary>
///     A message received from an SQS queue. Acknowledging it deletes it from the queue (in batches, shortly after); a
///     message not acknowledged within its visibility timeout is delivered again.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class SqsMessage<T> : IAcknowledgableMessage<T>, ISqsReceived
{
    private readonly Lazy<IReadOnlyDictionary<string, object>> _metadata;
    private readonly MessageSettlement _settlement;

    internal SqsMessage(T body, string messageId, string receiptHandle, string queueUrl, IReadOnlyDictionary<string, MessageAttributeValue> attributes,
        IReadOnlyDictionary<string, string> systemAttributes, MessageSettlement settlement, Lazy<IReadOnlyDictionary<string, object>>? metadata = null)
    {
        Body = body;
        MessageId = messageId;
        ReceiptHandle = receiptHandle;
        QueueUrl = queueUrl;
        Attributes = attributes;
        SystemAttributes = systemAttributes;
        _settlement = settlement;
        _metadata = metadata ?? new Lazy<IReadOnlyDictionary<string, object>>(BuildMetadata);
    }

    /// <summary>The handle this receipt of the message is deleted or made visible with.</summary>
    public string ReceiptHandle { get; }

    /// <summary>The queue the message was received from.</summary>
    public string QueueUrl { get; }

    /// <summary>The message attributes the sender set.</summary>
    public IReadOnlyDictionary<string, MessageAttributeValue> Attributes { get; }

    /// <summary>SQS's attributes: <c>SentTimestamp</c>, <c>ApproximateReceiveCount</c>, <c>MessageGroupId</c> and the rest.</summary>
    public IReadOnlyDictionary<string, string> SystemAttributes { get; }

    /// <summary>When the message was sent, from <c>SentTimestamp</c>.</summary>
    public DateTimeOffset? SentAt =>
        SystemAttributes.TryGetValue("SentTimestamp", out var sent) && long.TryParse(sent, out var ms) ? DateTimeOffset.FromUnixTimeMilliseconds(ms) : null;

    /// <summary>How many times the message has been received, this time included, from <c>ApproximateReceiveCount</c>.</summary>
    public int ReceiveCount =>
        SystemAttributes.TryGetValue("ApproximateReceiveCount", out var count) && int.TryParse(count, out var n) ? n : 1;

    /// <inheritdoc />
    public T Body { get; }

    object? IAcknowledgableMessage.Body => Body;

    /// <inheritdoc />
    public string MessageId { get; }

    /// <inheritdoc />
    public bool IsSettled => _settlement.IsSettled;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, object> Metadata => _metadata.Value;

    /// <inheritdoc />
    public Task AcknowledgeAsync(CancellationToken cancellationToken = default) => _settlement.AcknowledgeAsync(cancellationToken);

    /// <summary>
    ///     Rejects the message: with <paramref name="requeue" /> it becomes visible again at once, so it is delivered again;
    ///     without, it is deleted. (SQS moves a message to a dead-letter queue only through the queue's redrive policy.)
    /// </summary>
    public Task RejectAsync(bool requeue, CancellationToken cancellationToken = default) => _settlement.RejectAsync(requeue, cancellationToken);

    /// <inheritdoc />
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body) =>
        new SqsMessage<TNew>(body, MessageId, ReceiptHandle, QueueUrl, Attributes, SystemAttributes, _settlement, _metadata);

    private IReadOnlyDictionary<string, object> BuildMetadata()
    {
        var metadata = new Dictionary<string, object> { ["QueueUrl"] = QueueUrl };

        foreach (var (key, value) in SystemAttributes)
        {
            metadata[key] = value;
        }

        foreach (var (key, value) in Attributes)
        {
            metadata[$"Attribute.{key}"] = value.StringValue ?? (object?)value.BinaryValue ?? string.Empty;
        }

        return metadata;
    }
}

/// <summary>What an SQS sink carries on from a received message whatever its body type.</summary>
internal interface ISqsReceived
{
    string MessageId { get; }

    IReadOnlyDictionary<string, MessageAttributeValue> Attributes { get; }
}
