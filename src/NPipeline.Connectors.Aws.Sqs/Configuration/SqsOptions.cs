using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Aws.Sqs.Configuration;

/// <summary>
///     The client settings every SQS source and sink has. Pass a <see cref="Client" /> to share one; otherwise the node
///     creates its own from the other settings, and disposes it.
/// </summary>
public abstract record SqsNodeOptions
{
    /// <summary>The queue's URL.</summary>
    public required string QueueUrl { get; init; }

    /// <summary>A client to use, which the caller owns. When <c>null</c>, the node creates one from the settings below.</summary>
    public IAmazonSQS? Client { get; init; }

    /// <summary>The region for a client the node creates; <c>null</c> lets the SDK resolve it (<c>AWS_REGION</c>, the profile).</summary>
    public string? Region { get; init; }

    /// <summary>A service URL for a client the node creates, such as LocalStack's.</summary>
    public string? ServiceUrl { get; init; }

    /// <summary>A named profile from the shared credentials file, for a client the node creates.</summary>
    public string? ProfileName { get; init; }

    /// <summary>Credentials for a client the node creates; <c>null</c> uses the SDK's default credential chain.</summary>
    public AWSCredentials? Credentials { get; init; }

    /// <summary>
    ///     The SDK's retry mode for a client the node creates. Defaults to <see cref="RequestRetryMode.Standard" />; the SDK is
    ///     the only layer that retries SQS calls.
    /// </summary>
    public RequestRetryMode? RetryMode { get; init; } = RequestRetryMode.Standard;

    /// <summary>Retries per call (not attempts) for a client the node creates. Defaults to 3.</summary>
    public int? MaxErrorRetry { get; init; } = 3;

    /// <summary>The body serializer; it must write UTF-8 text, since SQS bodies are text. Defaults to <see cref="JsonMessageSerializer.Default" />.</summary>
    public IMessageSerializer Serializer { get; init; } = JsonMessageSerializer.Default;

    /// <summary>Throws when a setting is missing or out of range.</summary>
    public virtual void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(QueueUrl, nameof(QueueUrl));
        ArgumentNullException.ThrowIfNull(Serializer, nameof(Serializer));

        if (MaxErrorRetry is < 0)
            throw new ArgumentOutOfRangeException(nameof(MaxErrorRetry), "MaxErrorRetry must be zero or more.");
    }
}

/// <summary>How an SQS source receives. Create one with <see cref="SqsConnector.Source{T}" />.</summary>
public sealed record SqsReadOptions : SqsNodeOptions
{
    /// <summary>Messages per receive, 1 to 10. Defaults to 10.</summary>
    public int MaxMessages { get; init; } = 10;

    /// <summary>How long a receive waits for a message (long polling), 0 to 20 seconds. Defaults to 20.</summary>
    public TimeSpan WaitTime { get; init; } = TimeSpan.FromSeconds(20);

    /// <summary>
    ///     How long a received message stays hidden from other receivers; it is delivered again unless acknowledged by then.
    ///     <c>null</c> (the default) uses the queue's setting.
    /// </summary>
    public TimeSpan? VisibilityTimeout { get; init; }

    /// <summary>
    ///     Decides what happens to a message whose body does not deserialize: <c>Fail</c> (the default, when <c>null</c>)
    ///     fails the read, and the message is delivered again after its visibility timeout; <c>Skip</c> deletes it;
    ///     <c>DeadLetter</c> sends it to the pipeline's dead-letter sink as a <see cref="MessageFailure" /> and deletes it.
    /// </summary>
    public RowErrorHandler? RowErrorHandler { get; init; }

    /// <summary>The most characters of a failed body kept in <see cref="RowError.RawExcerpt" />. Defaults to 256.</summary>
    public int RawExcerptLength { get; init; } = FileSourceOptions.DefaultRawExcerptLength;

    /// <summary>
    ///     How long an acknowledged message waits to be deleted with others (up to 10 per request). Defaults to 50 ms;
    ///     outstanding deletes are sent when the read ends.
    /// </summary>
    public TimeSpan DeleteLinger { get; init; } = TimeSpan.FromMilliseconds(50);

    /// <summary>
    ///     How long the source keeps its client after the read ends, so a sink can still acknowledge the messages it was
    ///     given. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan SettleTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxMessages, 1, nameof(MaxMessages));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(MaxMessages, 10, nameof(MaxMessages));
        ArgumentOutOfRangeException.ThrowIfLessThan(WaitTime, TimeSpan.Zero, nameof(WaitTime));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(WaitTime, TimeSpan.FromSeconds(20), nameof(WaitTime));

        if (VisibilityTimeout is { } visibility && (visibility < TimeSpan.Zero || visibility > TimeSpan.FromHours(12)))
            throw new ArgumentOutOfRangeException(nameof(VisibilityTimeout), "VisibilityTimeout must be between 0 and 12 hours.");

        ArgumentOutOfRangeException.ThrowIfNegative(RawExcerptLength, nameof(RawExcerptLength));
        ArgumentOutOfRangeException.ThrowIfLessThan(DeleteLinger, TimeSpan.Zero, nameof(DeleteLinger));
        ArgumentOutOfRangeException.ThrowIfLessThan(SettleTimeout, TimeSpan.Zero, nameof(SettleTimeout));
    }
}

/// <summary>How an SQS sink sends. Create one with <see cref="SqsConnector.Sink{T}" />.</summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed record SqsWriteOptions<T> : SqsNodeOptions
{
    /// <summary>Messages per <c>SendMessageBatch</c>, 1 to 10; a batch over 256 KB is split. Defaults to 10.</summary>
    public int BatchSize { get; init; } = 10;

    /// <summary>The longest a batch waits to fill, measured from its first message. Defaults to 10 ms.</summary>
    public TimeSpan BatchLinger { get; init; } = TimeSpan.FromMilliseconds(10);

    /// <summary>How long each message is delayed before it can be received, up to 15 minutes. Defaults to none.</summary>
    public TimeSpan Delay { get; init; } = TimeSpan.Zero;

    /// <summary>Attributes added to every message.</summary>
    public IReadOnlyDictionary<string, MessageAttributeValue> MessageAttributes { get; init; } = new Dictionary<string, MessageAttributeValue>();

    /// <summary>Whether a message received from SQS keeps its attributes. Defaults to <c>true</c>.</summary>
    public bool CopyMessageAttributes { get; init; } = true;

    /// <summary>For a FIFO queue: each message's group, which orders messages within it. Required for FIFO queues.</summary>
    public Func<T, string>? MessageGroupId { get; init; }

    /// <summary>
    ///     For a FIFO queue without content-based deduplication: each message's deduplication id. When <c>null</c>, a
    ///     message received from a queue keeps its message id, so a retried write is not delivered twice.
    /// </summary>
    public Func<T, string>? DeduplicationId { get; init; }

    /// <summary>What happens to a message SQS rejected. Defaults to <see cref="FailedMessageAction.Fail" />.</summary>
    public FailedMessageAction FailedMessages { get; init; } = FailedMessageAction.Fail;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentOutOfRangeException.ThrowIfLessThan(BatchSize, 1, nameof(BatchSize));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(BatchSize, 10, nameof(BatchSize));

        if (BatchLinger < TimeSpan.Zero && BatchLinger != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(BatchLinger), "BatchLinger must be zero or more, or Timeout.InfiniteTimeSpan.");

        ArgumentOutOfRangeException.ThrowIfLessThan(Delay, TimeSpan.Zero, nameof(Delay));
        ArgumentOutOfRangeException.ThrowIfGreaterThan(Delay, TimeSpan.FromMinutes(15), nameof(Delay));
        ArgumentNullException.ThrowIfNull(MessageAttributes, nameof(MessageAttributes));

        if (MessageGroupId is not null && Delay > TimeSpan.Zero)
            throw new ArgumentException("FIFO queues (MessageGroupId) do not accept a per-message Delay; set the queue's delivery delay instead.", nameof(Delay));
    }
}
