using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;

namespace NPipeline.Connectors.Azure.ServiceBus.Internal;

/// <summary>
///     Keeps held messages locked while a sink works on them: a message's lock (or, for a session, the session's lock) is
///     renewed once half its span has passed, for at most the source's <c>MaxLockRenewal</c> after it was received.
/// </summary>
internal sealed class LockRenewer : IAsyncDisposable
{
    private static readonly TimeSpan Tick = TimeSpan.FromSeconds(1);

    private readonly ConcurrentDictionary<ServiceBusReceivedMessage, Held> _messages = new(ReferenceEqualityComparer.Instance);
    private readonly ConcurrentDictionary<ServiceBusSessionReceiver, Held> _sessions = new(ReferenceEqualityComparer.Instance);
    private readonly ILogger _logger;
    private readonly Task _loop;
    private readonly TimeSpan _maxRenewal;
    private readonly CancellationTokenSource _stop = new();

    public LockRenewer(TimeSpan maxRenewal, ILogger logger)
    {
        _maxRenewal = maxRenewal;
        _logger = logger;
        _loop = Task.Run(RunAsync);
    }

    /// <summary>The messages still held, so they can be abandoned when the source closes.</summary>
    public IEnumerable<(ServiceBusReceiver Receiver, ServiceBusReceivedMessage Message)> HeldMessages => _messages.Select(p => (p.Value.Receiver, p.Key));

    public void Track(ServiceBusReceiver receiver, ServiceBusReceivedMessage message) =>
        _messages[message] = new Held(receiver, DateTimeOffset.UtcNow, message.LockedUntil - DateTimeOffset.UtcNow);

    public void Untrack(ServiceBusReceivedMessage message) => _messages.TryRemove(message, out _);

    public void TrackSession(ServiceBusSessionReceiver session) =>
        _sessions[session] = new Held(session, DateTimeOffset.UtcNow, session.SessionLockedUntil - DateTimeOffset.UtcNow);

    public void UntrackSession(ServiceBusSessionReceiver session) => _sessions.TryRemove(session, out _);

    /// <summary>Stops renewing.</summary>
    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);

        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Stopped.
        }

        _stop.Dispose();
    }

    private async Task RunAsync()
    {
        using var timer = new PeriodicTimer(Tick);

        while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
        {
            var now = DateTimeOffset.UtcNow;

            foreach (var (message, held) in _messages)
            {
                if (now - held.Since > _maxRenewal)
                    _ = _messages.TryRemove(message, out _);
                else if (message.LockedUntil - now < held.Span / 2)
                    await RenewAsync(() => held.Receiver.RenewMessageLockAsync(message, _stop.Token)).ConfigureAwait(false);
            }

            foreach (var (session, held) in _sessions)
            {
                if (session.SessionLockedUntil - now < held.Span / 2)
                    await RenewAsync(() => session.RenewSessionLockAsync(_stop.Token)).ConfigureAwait(false);
            }
        }
    }

    private async Task RenewAsync(Func<Task> renew)
    {
        try
        {
            await renew().ConfigureAwait(false);
        }
        catch (ServiceBusException ex)
        {
            // The lock is lost, so the message will be delivered again; settling it will fail and say so.
            ServiceBusLogMessages.LockRenewalFailed(_logger, ex);
        }
    }

    private sealed record Held(ServiceBusReceiver Receiver, DateTimeOffset Since, TimeSpan Span);
}
