using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Internal;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.ServiceBus.Nodes;

/// <summary>
///     Receives from a Service Bus queue or subscription in peek-lock mode. Messages are received in batches while fewer than
///     <see cref="ServiceBusReadOptions.MaxInFlight" /> are unsettled, their locks are renewed while they wait, and each is
///     handed on as a <see cref="ServiceBusMessage{T}" /> to be settled whenever the pipeline is done with it, so a sink can
///     batch as many as it holds. For a session-enabled entity, up to
///     <see cref="ServiceBusReadOptions.MaxConcurrentSessions" /> sessions are received at once, each in order. Create one with
///     <see cref="ServiceBusConnector" />.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class ServiceBusSourceNode<T> : SourceNode<ServiceBusMessage<T>>, IAsyncDisposable
{
    private readonly List<Task> _closing = [];
    private readonly MessageDecoder<T> _decoder;
    private readonly CancellationTokenSource _disposing = new();
    private bool _disposed;
    private readonly ServiceBusReadOptions _options;
    private readonly bool _sessions;

    /// <summary>Creates a source and validates <paramref name="options" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="sessions">Whether the entity is session-enabled.</param>
    public ServiceBusSourceNode(ServiceBusReadOptions options, bool sessions = false)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        if (sessions && options.SubQueue != SubQueue.None)
            throw new ArgumentException("A session source reads the entity itself; read a sub-queue with a plain source.", nameof(options));

        _options = options;
        _sessions = sessions;
        _decoder = new MessageDecoder<T>(ServiceBusConnector.Name, EntityPath, options.Serializer, options.RowErrorHandler, options.RawExcerptLength);
    }

    private string EntityPath => _options.Subscription is null ? _options.Entity : $"{_options.Entity}/subscriptions/{_options.Subscription}";

    /// <summary>Abandons messages still held by finished reads and closes their receivers now.</summary>
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
    public override IDataStream<ServiceBusMessage<T>> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new DataStream<ServiceBusMessage<T>>(ReceiveAsync(context, cancellationToken), $"ServiceBusSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<ServiceBusMessage<T>> ReceiveAsync(PipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var logger = context.Observability.LoggerFactory.CreateLogger(typeof(ServiceBusSourceNode<T>).FullName ?? nameof(ServiceBusSourceNode<T>));
        var (client, owned) = ServiceBusClients.For(_options);

        var read = new Read(OpenDeadLetterChannel(context), new LockRenewer(_options.MaxLockRenewal, logger), logger,
            Channel.CreateBounded<ServiceBusMessage<T>>(new BoundedChannelOptions(_options.MaxInFlight) { SingleReader = true }));

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        ServiceBusLogMessages.ReceiverStarted(logger, EntityPath);

        Task[] workers = _sessions
            ? [.. Enumerable.Range(0, _options.MaxConcurrentSessions).Select(_ => AcceptSessionsAsync(client, read, stop.Token))]
            : [ReceiveAsync(Receiver(client), read, null, new InFlightMessages(), stop.Token)];

        // A worker that fails ends the read with its error; stopping the read ends them without one.
        var running = Task.WhenAll(workers).ContinueWith(t => read.Output.Writer.TryComplete(stop.IsCancellationRequested ? null : t.Exception?.GetBaseException()),
            CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        try
        {
            await foreach (var message in read.Output.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return message;
            }
        }
        finally
        {
            await stop.CancelAsync().ConfigureAwait(false);
            await running.ConfigureAwait(false);

            // Received but never handed on: back to the entity at once.
            while (read.Output.Reader.TryRead(out var unread))
            {
                await IgnoreFailure(() => unread.RejectAsync(true), logger).ConfigureAwait(false);
            }

            lock (_closing)
            {
                _closing.Add(CloseWhenSettledAsync(read, client, owned));
            }
        }
    }

    private ServiceBusReceiver Receiver(ServiceBusClient client)
    {
        var options = new ServiceBusReceiverOptions { PrefetchCount = _options.PrefetchCount, SubQueue = _options.SubQueue };

        return _options.Subscription is null
            ? client.CreateReceiver(_options.Entity, options)
            : client.CreateReceiver(_options.Entity, _options.Subscription, options);
    }

    /// <summary>Accepts sessions one after another, receiving each until it has been idle for the session idle timeout.</summary>
    private async Task AcceptSessionsAsync(ServiceBusClient client, Read read, CancellationToken cancellationToken)
    {
        var options = new ServiceBusSessionReceiverOptions { PrefetchCount = _options.PrefetchCount };

        while (!cancellationToken.IsCancellationRequested)
        {
            ServiceBusSessionReceiver session;

            try
            {
                session = _options.Subscription is null
                    ? await client.AcceptNextSessionAsync(_options.Entity, options, cancellationToken).ConfigureAwait(false)
                    : await client.AcceptNextSessionAsync(_options.Entity, _options.Subscription, options, cancellationToken).ConfigureAwait(false);
            }
            catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.ServiceTimeout)
            {
                // No session has messages; ask again.
                continue;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            ServiceBusLogMessages.SessionAccepted(read.Logger, session.SessionId, EntityPath);
            read.Renewer.TrackSession(session);

            // The session's receiver closes once its own messages are settled, so a finished session does not hold its lock.
            var inSession = new InFlightMessages();

            try
            {
                await ReceiveAsync(session, read, _options.SessionIdleTimeout, inSession, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                lock (_closing)
                {
                    _closing.Add(CloseSessionWhenSettledAsync(session, inSession, read));
                }
            }
        }
    }

    /// <summary>Receives until cancelled, or until <paramref name="idle" /> passes with nothing received.</summary>
    private async Task ReceiveAsync(ServiceBusReceiver receiver, Read read, TimeSpan? idle, InFlightMessages scope, CancellationToken cancellationToken)
    {
        read.Receivers.Add(receiver);
        var lastReceived = DateTimeOffset.UtcNow;

        // Sessions share the in-flight budget.
        var budget = _sessions ? Math.Max(1, _options.MaxInFlight / _options.MaxConcurrentSessions) : _options.MaxInFlight;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await scope.WaitForRoomAsync(budget, cancellationToken).ConfigureAwait(false);
                await read.InFlight.WaitForRoomAsync(_options.MaxInFlight, cancellationToken).ConfigureAwait(false);

                var room = Math.Min(100, Math.Max(1, budget - scope.Count));
                var wait = idle is { } limit ? TimeSpan.FromTicks(Math.Min(limit.Ticks, TimeSpan.FromSeconds(1).Ticks)) : TimeSpan.FromSeconds(1);
                var received = await receiver.ReceiveMessagesAsync(room, wait, cancellationToken).ConfigureAwait(false);

                if (received.Count == 0)
                {
                    if (idle is { } timeout && DateTimeOffset.UtcNow - lastReceived > timeout)
                        return;

                    continue;
                }

                lastReceived = DateTimeOffset.UtcNow;

                foreach (var message in received)
                {
                    if (await HandOnAsync(receiver, message, read, scope, cancellationToken).ConfigureAwait(false) is { } handedOn)
                        await read.Output.Writer.WriteAsync(handedOn, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The read ended.
        }
    }

    /// <summary>The message to hand on, or <c>null</c> when its body did not deserialize and the row-error handler settled it.</summary>
    private async Task<ServiceBusMessage<T>?> HandOnAsync(ServiceBusReceiver receiver, ServiceBusReceivedMessage message, Read read, InFlightMessages scope,
        CancellationToken cancellationToken)
    {
        var sequence = Interlocked.Increment(ref read.Sequence);
        var body = message.Body.ToMemory();

        if (!_decoder.TryDecode(body.Span, out var value, out var error))
        {
            ServiceBusLogMessages.DeserializationFailed(read.Logger, error, message.MessageId, EntityPath);
            RowErrorAction action;

            try
            {
                action = await _decoder.HandleFailureAsync(error, body, message.MessageId, sequence, Metadata(message), read.DeadLetters, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (RecordMappingException)
            {
                // Fail: back to the entity, so it is delivered again (the broker dead-letters it after its maximum delivery count).
                await IgnoreFailure(() => receiver.AbandonMessageAsync(message, cancellationToken: CancellationToken.None), read.Logger).ConfigureAwait(false);
                throw;
            }

            if (action == RowErrorAction.Skip)
                await receiver.DeadLetterMessageAsync(message, "DeserializationFailed", Truncate(error.Message), cancellationToken).ConfigureAwait(false);
            else
                await receiver.CompleteMessageAsync(message, cancellationToken).ConfigureAwait(false);

            ConnectorDiagnostics.RecordMessagesSettled(ServiceBusConnector.Name, action == RowErrorAction.Skip ? "dead_lettered" : "acknowledged");
            return null;
        }

        read.InFlight.Add();
        scope.Add();
        read.Renewer.Track(receiver, message);

        async Task Settle(Func<Task> settle, string outcome)
        {
            try
            {
                await settle().ConfigureAwait(false);
                ConnectorDiagnostics.RecordMessagesSettled(ServiceBusConnector.Name, outcome);
            }
            finally
            {
                read.Renewer.Untrack(message);
                scope.Remove();
                read.InFlight.Remove();
            }
        }

        var settlement = new MessageSettlement(
            ct => Settle(() => receiver.CompleteMessageAsync(message, ct), "acknowledged"),
            (requeue, ct) => requeue
                ? Settle(() => receiver.AbandonMessageAsync(message, cancellationToken: ct), "requeued")
                : Settle(() => receiver.DeadLetterMessageAsync(message, "Rejected", cancellationToken: ct), "dead_lettered"));

        ConnectorDiagnostics.RecordRowsRead(ServiceBusConnector.Name, ServiceBusConnector.Name, 1);
        return new ServiceBusMessage<T>(value, message, receiver, settlement);
    }

    private async Task CloseWhenSettledAsync(Read read, ServiceBusClient client, bool owned)
    {
        try
        {
            _ = await read.InFlight.WhenSettledAsync(_options.SettleTimeout, _disposing.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Disposed: close now.
        }

        // Whatever is still held goes back to the entity rather than waiting for its lock to expire.
        foreach (var (receiver, message) in read.Renewer.HeldMessages.ToList())
        {
            await IgnoreFailure(() => receiver.AbandonMessageAsync(message), read.Logger).ConfigureAwait(false);
        }

        await read.Renewer.DisposeAsync().ConfigureAwait(false);

        foreach (var receiver in read.Receivers)
        {
            await IgnoreFailure(() => receiver.CloseAsync(), read.Logger).ConfigureAwait(false);
        }

        if (owned)
            await client.DisposeAsync().ConfigureAwait(false);
    }

    private async Task CloseSessionWhenSettledAsync(ServiceBusSessionReceiver session, InFlightMessages inSession, Read read)
    {
        try
        {
            _ = await inSession.WhenSettledAsync(_options.SettleTimeout, _disposing.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Disposed: the whole read closes.
        }

        read.Renewer.UntrackSession(session);
        await IgnoreFailure(() => session.CloseAsync(), read.Logger).ConfigureAwait(false);
    }

    private static async Task IgnoreFailure(Func<Task> action, ILogger logger)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is ServiceBusException or ObjectDisposedException or InvalidOperationException)
        {
            ServiceBusLogMessages.CloseFailed(logger, ex);
        }
    }

    private static string Truncate(string text) => text.Length <= 1024 ? text : text[..1024];

    private static Dictionary<string, object> Metadata(ServiceBusReceivedMessage message)
    {
        var metadata = new Dictionary<string, object> { ["SequenceNumber"] = message.SequenceNumber, ["DeliveryCount"] = message.DeliveryCount };

        if (message.SessionId is not null)
            metadata["SessionId"] = message.SessionId;

        foreach (var (key, value) in message.ApplicationProperties)
        {
            metadata[$"Property.{key}"] = value;
        }

        return metadata;
    }

    /// <summary>The state of one read, shared by its workers.</summary>
    private sealed class Read(DeadLetterChannel deadLetters, LockRenewer renewer, ILogger logger, Channel<ServiceBusMessage<T>> output)
    {
        public long Sequence;

        public DeadLetterChannel DeadLetters { get; } = deadLetters;

        public LockRenewer Renewer { get; } = renewer;

        public ILogger Logger { get; } = logger;

        public Channel<ServiceBusMessage<T>> Output { get; } = output;

        public InFlightMessages InFlight { get; } = new();

        public ConcurrentBag<ServiceBusReceiver> Receivers { get; } = [];
    }
}
