using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Messaging;

/// <summary>
///     A sink that writes messages itself and settles each one when it is written: the message-queue sinks, which also carry
///     message ids, sessions and headers from a received message to the one they publish.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public interface IMessageSink<T>
{
    /// <summary>Writes the messages' bodies and settles each message once its body is written.</summary>
    Task ConsumeMessagesAsync(IDataStream<IAcknowledgableMessage<T>> input, PipelineContext context, CancellationToken cancellationToken);
}

/// <summary>
///     A sink that reports when what it received is durably written, so the messages behind it can be acknowledged then:
///     the SQL sinks after each committed batch, the HTTP sink after each request.
/// </summary>
public interface IReportsWrites
{
    /// <summary>
    ///     Registers <paramref name="written" />, which the sink calls, in order and never concurrently, with the number of
    ///     items it has durably handled so far, counted from the start of its input: written, or sent to the dead-letter
    ///     sink. Call before <see cref="SinkNode{TIn}.ConsumeAsync" />; a later registration replaces an earlier one.
    /// </summary>
    void ReportWritesTo(Func<long, CancellationToken, ValueTask> written);
}

/// <summary>Makes any sink a sink of received messages.</summary>
public static class AcknowledgingSinkExtensions
{
    /// <summary>
    ///     A sink of messages that writes their bodies through <paramref name="sink" /> and acknowledges each message once it
    ///     is written: through the sink's own message handling (<see cref="IMessageSink{T}" />), as the sink reports its
    ///     writes (<see cref="IReportsWrites" />), or, for a sink that does neither, when it has written the whole stream.
    /// </summary>
    public static SinkNode<IAcknowledgableMessage<T>> Acknowledging<T>(this SinkNode<T> sink) => new AcknowledgingSink<T>(sink);
}

/// <summary>See <see cref="AcknowledgingSinkExtensions.Acknowledging{T}" />.</summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class AcknowledgingSink<T> : SinkNode<IAcknowledgableMessage<T>>, IAsyncDisposable
{
    /// <summary>Wraps <paramref name="inner" />.</summary>
    public AcknowledgingSink(SinkNode<T> inner)
    {
        Inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    /// <summary>The sink the bodies are written to.</summary>
    public SinkNode<T> Inner { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Inner is IAsyncDisposable disposable)
            await disposable.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public override async Task ConsumeAsync(IDataStream<IAcknowledgableMessage<T>> input, PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);

        if (Inner is IMessageSink<T> messages)
        {
            await messages.ConsumeMessagesAsync(input, context, cancellationToken).ConfigureAwait(false);
            return;
        }

        var pending = new ConcurrentQueue<IAcknowledgableMessage<T>>();
        long acknowledged = 0;

        async ValueTask AcknowledgeUpTo(long written, CancellationToken token)
        {
            while (acknowledged < written && pending.TryDequeue(out var message))
            {
                await message.AcknowledgeAsync(token).ConfigureAwait(false);
                acknowledged++;
            }
        }

        if (Inner is IReportsWrites reporter)
            reporter.ReportWritesTo(AcknowledgeUpTo);

        await Inner.ConsumeAsync(new DataStream<T>(Bodies(input, pending, cancellationToken), input.StreamName), context, cancellationToken)
            .ConfigureAwait(false);

        // The sink finished without error, so everything it was given is written.
        await AcknowledgeUpTo(long.MaxValue, cancellationToken).ConfigureAwait(false);
    }

    private static async IAsyncEnumerable<T> Bodies(IDataStream<IAcknowledgableMessage<T>> input, ConcurrentQueue<IAcknowledgableMessage<T>> pending,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var message in input.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            pending.Enqueue(message);
            yield return message.Body;
        }
    }
}
