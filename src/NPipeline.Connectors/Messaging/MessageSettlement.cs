namespace NPipeline.Connectors.Messaging;

/// <summary>
///     The settlement state of one received message, shared by the message and every copy made with
///     <see cref="IAcknowledgableMessage{T}.WithBody{TNew}" />. The first call to <see cref="AcknowledgeAsync" /> or
///     <see cref="RejectAsync" /> settles the message; later calls return its task, so a message is never settled twice.
/// </summary>
public sealed class MessageSettlement
{
    private readonly Func<CancellationToken, Task> _acknowledge;
    private readonly Func<bool, CancellationToken, Task> _reject;
    private Task? _settled;

    /// <summary>Creates the state for a message settled with the broker by <paramref name="acknowledge" /> or <paramref name="reject" />.</summary>
    /// <param name="acknowledge">Acknowledges the message with the broker.</param>
    /// <param name="reject">Rejects the message with the broker; the argument says whether it is requeued.</param>
    public MessageSettlement(Func<CancellationToken, Task> acknowledge, Func<bool, CancellationToken, Task> reject)
    {
        _acknowledge = acknowledge ?? throw new ArgumentNullException(nameof(acknowledge));
        _reject = reject ?? throw new ArgumentNullException(nameof(reject));
    }

    /// <summary>Whether the message has been settled.</summary>
    public bool IsSettled => Volatile.Read(ref _settled) is not null;

    /// <summary>Acknowledges the message, unless it is already settled.</summary>
    public Task AcknowledgeAsync(CancellationToken cancellationToken = default) => Settle(() => _acknowledge(cancellationToken));

    /// <summary>Rejects the message, unless it is already settled.</summary>
    public Task RejectAsync(bool requeue, CancellationToken cancellationToken = default) => Settle(() => _reject(requeue, cancellationToken));

    /// <summary>
    ///     Settles the message some other way the broker offers, unless it is already settled: dead-lettering with a reason,
    ///     say, or deferring it.
    /// </summary>
    public Task SettleWith(Func<CancellationToken, Task> settle, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settle);
        return Settle(() => settle(cancellationToken));
    }

    private Task Settle(Func<Task> settle)
    {
        if (Volatile.Read(ref _settled) is { } existing)
            return existing;

        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        if (Interlocked.CompareExchange(ref _settled, pending.Task, null) is { } raced)
            return raced;

        _ = RunAsync(settle, pending);
        return pending.Task;
    }

    private static async Task RunAsync(Func<Task> settle, TaskCompletionSource pending)
    {
        try
        {
            await settle().ConfigureAwait(false);
            pending.SetResult();
        }
        catch (OperationCanceledException ex)
        {
            pending.SetCanceled(ex.CancellationToken);
        }
        catch (Exception ex)
        {
            pending.SetException(ex);
        }
    }
}
