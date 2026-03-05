using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors.Azure.ServiceBus.Models;

/// <summary>
///     Azure Service Bus-specific implementation of <see cref="IAcknowledgableMessage{T}" /> that wraps
///     a consumed message with explicit settlement support (Complete, Abandon, DeadLetter, Defer).
/// </summary>
/// <typeparam name="T">The deserialized message body type.</typeparam>
/// <remarks>
///     <para>
///         Settlement methods are idempotent: calling <see cref="CompleteAsync" /> (or any other settlement
///         method) more than once is safe — only the first call takes effect.
///     </para>
///     <para>
///         <see cref="AcknowledgeAsync" /> delegates to <see cref="CompleteAsync" />.
///         <see cref="NegativeAcknowledgeAsync" /> delegates to <see cref="AbandonAsync" /> when
///         <c>requeue = true</c> (the default), or <see cref="DeadLetterAsync" /> when <c>requeue = false</c>.
///     </para>
///     <para>
///         The source node integrates a settlement <see cref="TaskCompletionSource{T}" /> so that the
///         underlying Service Bus processor handler blocks until settlement completes, keeping the message
///         lock valid for the full processing duration.
///     </para>
/// </remarks>
public sealed class ServiceBusMessage<T> : IAcknowledgableMessage<T>, IServiceBusMessageMetadata
{
    private readonly Func<IDictionary<string, object>?, CancellationToken, Task> _abandonCallback;
    private readonly Func<CancellationToken, Task> _completeCallback;
    private readonly Func<string?, string?, CancellationToken, Task> _deadLetterCallback;
    private readonly Func<IDictionary<string, object>?, CancellationToken, Task> _deferCallback;
    private readonly Dictionary<string, object> _metadata;
    private readonly TaskCompletionSource<bool>? _settlementTcs;
    private int _settlementState; // 0 = unsettled, 1 = settled

    /// <summary>
    ///     Internal constructor used by source nodes; captures real settlement callbacks from
    ///     <see cref="ProcessMessageEventArgs" /> and a <see cref="TaskCompletionSource{T}" />
    ///     that unblocks the processor handler once settlement completes.
    /// </summary>
    internal ServiceBusMessage(
        T body,
        string messageId,
        ServiceBusReceivedMessage rawMessage,
        Func<CancellationToken, Task> completeCallback,
        Func<IDictionary<string, object>?, CancellationToken, Task> abandonCallback,
        Func<string?, string?, CancellationToken, Task> deadLetterCallback,
        Func<IDictionary<string, object>?, CancellationToken, Task> deferCallback,
        TaskCompletionSource<bool>? settlementTcs = null)
    {
        Body = body;
        MessageId = messageId;
        SessionId = rawMessage.SessionId;
        CorrelationId = rawMessage.CorrelationId;
        ReplyTo = rawMessage.ReplyTo;
        To = rawMessage.To;
        Subject = rawMessage.Subject;
        ReplyToSessionId = rawMessage.ReplyToSessionId;
        EnqueuedTime = rawMessage.EnqueuedTime;
        DeliveryCount = rawMessage.DeliveryCount;
        PartitionKey = rawMessage.PartitionKey;
        TimeToLive = rawMessage.TimeToLive;
        ContentType = rawMessage.ContentType;

        ApplicationProperties = rawMessage.ApplicationProperties
            .ToDictionary(k => k.Key, v => v.Value);

        _completeCallback = completeCallback;
        _abandonCallback = abandonCallback;
        _deadLetterCallback = deadLetterCallback;
        _deferCallback = deferCallback;
        _settlementTcs = settlementTcs;
        _metadata = BuildMetadata(rawMessage);
    }

    /// <summary>
    ///     Testing / manual construction — all settlement callbacks are optional delegates.
    /// </summary>
    public ServiceBusMessage(
        T body,
        string messageId,
        Func<CancellationToken, Task>? completeCallback = null,
        Func<IDictionary<string, object>?, CancellationToken, Task>? abandonCallback = null,
        Func<string?, string?, CancellationToken, Task>? deadLetterCallback = null,
        Func<IDictionary<string, object>?, CancellationToken, Task>? deferCallback = null,
        IReadOnlyDictionary<string, object>? applicationProperties = null)
    {
        Body = body;
        MessageId = messageId;
        _completeCallback = completeCallback ?? (_ => Task.CompletedTask);
        _abandonCallback = abandonCallback ?? ((_, _) => Task.CompletedTask);
        _deadLetterCallback = deadLetterCallback ?? ((_, _, _) => Task.CompletedTask);
        _deferCallback = deferCallback ?? ((_, _) => Task.CompletedTask);

        ApplicationProperties = applicationProperties != null
            ? new Dictionary<string, object>(applicationProperties)
            : new Dictionary<string, object>();

        _metadata = [];
    }

    // ── IAcknowledgableMessage<T> ────────────────────────────────────────────────

    /// <inheritdoc />
    public T Body { get; }

    object IAcknowledgableMessage.Body => Body!;

    /// <inheritdoc />
    public string MessageId { get; }

    /// <inheritdoc />
    public bool IsAcknowledged => Volatile.Read(ref _settlementState) == 1;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, object> Metadata => _metadata;

    /// <summary>
    ///     Acknowledges the message by completing it on the broker.
    ///     Equivalent to <see cref="CompleteAsync" />.
    /// </summary>
    /// <inheritdoc />
    public Task AcknowledgeAsync(CancellationToken cancellationToken = default)
    {
        return CompleteAsync(cancellationToken);
    }

    /// <summary>
    ///     Negatively acknowledges the message.
    ///     When <paramref name="requeue" /> is <c>true</c> (default), the message is abandoned and
    ///     becomes available for redelivery.  When <c>false</c>, the message is dead-lettered.
    /// </summary>
    public Task NegativeAcknowledgeAsync(bool requeue = true, CancellationToken cancellationToken = default)
    {
        return requeue
            ? AbandonAsync(cancellationToken: cancellationToken)
            : DeadLetterAsync(cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public IAcknowledgableMessage<TNew> WithBody<TNew>(TNew body)
    {
        return new ServiceBusMessage<TNew>(
            body,
            MessageId,
            _completeCallback,
            _abandonCallback,
            _deadLetterCallback,
            _deferCallback,
            ApplicationProperties);
    }

    // ── IServiceBusMessageMetadata ───────────────────────────────────────────────

    /// <inheritdoc />
    public string? SessionId { get; }

    /// <inheritdoc />
    public string? CorrelationId { get; }

    /// <inheritdoc />
    public string? ReplyTo { get; }

    /// <inheritdoc />
    public string? To { get; }

    /// <inheritdoc />
    public string? Subject { get; }

    /// <inheritdoc />
    public string? ReplyToSessionId { get; }

    /// <inheritdoc />
    public DateTimeOffset EnqueuedTime { get; }

    /// <inheritdoc />
    public int DeliveryCount { get; }

    /// <inheritdoc />
    public string? PartitionKey { get; }

    /// <inheritdoc />
    public TimeSpan TimeToLive { get; }

    /// <inheritdoc />
    public string? ContentType { get; }

    /// <inheritdoc />
    public bool IsSettled => Volatile.Read(ref _settlementState) == 1;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, object> ApplicationProperties { get; }

    // ── Settlement API ───────────────────────────────────────────────────────────

    /// <summary>
    ///     Completes the message, removing it from the queue or subscription.
    /// </summary>
    public async Task CompleteAsync(CancellationToken cancellationToken = default)
    {
        if (!TryMarkSettled())
            return;

        try
        {
            await _completeCallback(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            SignalSettlementTcs();
        }
    }

    /// <summary>
    ///     Abandons the message, making it available for immediate redelivery.
    /// </summary>
    /// <param name="propertiesToModify">Optional message properties to update before abandoning.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task AbandonAsync(
        IDictionary<string, object>? propertiesToModify = null,
        CancellationToken cancellationToken = default)
    {
        if (!TryMarkSettled())
            return;

        try
        {
            await _abandonCallback(propertiesToModify, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            SignalSettlementTcs();
        }
    }

    /// <summary>
    ///     Moves the message to the dead-letter sub-queue.
    /// </summary>
    /// <param name="reason">A short reason description.</param>
    /// <param name="description">A longer description of the failure.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task DeadLetterAsync(
        string? reason = null,
        string? description = null,
        CancellationToken cancellationToken = default)
    {
        if (!TryMarkSettled())
            return;

        try
        {
            await _deadLetterCallback(reason, description, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            SignalSettlementTcs();
        }
    }

    /// <summary>
    ///     Defers the message; it remains in the entity but must be received explicitly by sequence number.
    /// </summary>
    /// <param name="propertiesToModify">Optional properties to modify.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task DeferAsync(
        IDictionary<string, object>? propertiesToModify = null,
        CancellationToken cancellationToken = default)
    {
        if (!TryMarkSettled())
            return;

        try
        {
            await _deferCallback(propertiesToModify, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            SignalSettlementTcs();
        }
    }

    // ── Private Helpers ──────────────────────────────────────────────────────────

    /// <returns><c>true</c> if this call is the first settlement; <c>false</c> if already settled.</returns>
    private bool TryMarkSettled()
    {
        return Interlocked.CompareExchange(ref _settlementState, 1, 0) == 0;
    }

    private void SignalSettlementTcs()
    {
        _settlementTcs?.TrySetResult(true);
    }

    private static Dictionary<string, object> BuildMetadata(ServiceBusReceivedMessage message)
    {
        var metadata = new Dictionary<string, object>
        {
            ["EnqueuedTime"] = message.EnqueuedTime,
            ["DeliveryCount"] = message.DeliveryCount,
        };

        if (message.SessionId != null)
            metadata["SessionId"] = message.SessionId;

        if (message.CorrelationId != null)
            metadata["CorrelationId"] = message.CorrelationId;

        if (message.ReplyTo != null)
            metadata["ReplyTo"] = message.ReplyTo;

        if (message.PartitionKey != null)
            metadata["PartitionKey"] = message.PartitionKey;

        if (message.ContentType != null)
            metadata["ContentType"] = message.ContentType;

        if (message.Subject != null)
            metadata["Subject"] = message.Subject;

        foreach (var prop in message.ApplicationProperties)
        {
            metadata[$"ApplicationProperty.{prop.Key}"] = prop.Value;
        }

        return metadata;
    }
}
