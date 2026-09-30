using System.Runtime.CompilerServices;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Messaging.RoundTrip.Tests.Harnesses;

/// <summary>What a source does with a message whose body does not deserialize.</summary>
public enum Undeserializable
{
    /// <summary>The read fails.</summary>
    Fail,

    /// <summary>The message is settled so it is not delivered again, and the read continues.</summary>
    Skip,

    /// <summary>The message goes to the pipeline's dead-letter sink, is settled, and the read continues.</summary>
    DeadLetter,
}

/// <summary>Source settings a scenario sets; <c>null</c> keeps the connector's default.</summary>
public sealed record ReadSettings
{
    public static ReadSettings Defaults { get; } = new();

    public Undeserializable? OnUndeserializable { get; init; }

    /// <summary>The row-error handler for <see cref="OnUndeserializable" />.</summary>
    public RowErrorHandler? Handler => OnUndeserializable switch
    {
        null => null,
        Undeserializable.Fail => static _ => RowErrorAction.Fail,
        Undeserializable.Skip => static _ => RowErrorAction.Skip,
        _ => static _ => RowErrorAction.DeadLetter,
    };
}

/// <summary>
///     One broker behind the connectors under test. Destinations are created per test. <see cref="PublishRawAsync" /> and
///     <see cref="ReceiveRawAsync" /> use the broker's own client, so a scenario can check what a sink wrote and feed a
///     source bodies no connector would write. Nodes are disposed with the harness, at the end of the test, so a sink can
///     still settle messages after the read that produced them has ended.
/// </summary>
public abstract class MessagingHarness : IAsyncDisposable
{
    private readonly List<object> _nodes = [];

    public abstract string Name { get; }

    /// <summary>How long a message received and not settled takes to be delivered again.</summary>
    public virtual TimeSpan RedeliveryDelay => TimeSpan.Zero;

    /// <summary>Whether settling removes a message from the broker. Kafka keeps it in the log and moves the group past it.</summary>
    public virtual bool RemovesSettledMessages => true;

    public virtual async ValueTask DisposeAsync()
    {
        await DisposeNodesAsync();
        GC.SuppressFinalize(this);
    }

    /// <summary>Disposes the nodes created so far, as a pipeline does when its run ends.</summary>
    public async Task DisposeNodesAsync()
    {
        foreach (var node in _nodes)
        {
            if (node is IAsyncDisposable disposable)
                await disposable.DisposeAsync();
        }

        _nodes.Clear();
    }

    public abstract Task<string> CreateDestinationAsync();

    public abstract Task PublishRawAsync(string destination, params string[] bodies);

    /// <summary>Receives and removes up to <paramref name="count" /> bodies, waiting at most <paramref name="timeout" />.</summary>
    public abstract Task<List<string>> ReceiveRawAsync(string destination, int count, TimeSpan timeout);

    /// <summary>The connector's source over <paramref name="destination" /> with <paramref name="settings" />.</summary>
    public abstract IAsyncEnumerable<IAcknowledgableMessage<T>> ReadAsync<T>(string destination, ReadSettings settings, PipelineContext context,
        CancellationToken cancellationToken);

    /// <summary>The connector's sink to <paramref name="destination" /> with its default settings.</summary>
    protected abstract SinkNode<T> Sink<T>(string destination);

    /// <summary>Writes bodies through the sink.</summary>
    public Task WriteAsync<T>(string destination, IAsyncEnumerable<T> items, PipelineContext context, CancellationToken cancellationToken) =>
        Track(Sink<T>(destination)).ConsumeAsync(Stream(items), context, cancellationToken);

    /// <summary>Writes received messages through the sink, which settles them.</summary>
    public Task WriteMessagesAsync<T>(string destination, IAsyncEnumerable<IAcknowledgableMessage<T>> messages, PipelineContext context,
        CancellationToken cancellationToken) =>
        Track(Sink<T>(destination)).Acknowledging().ConsumeAsync(Stream(messages), context, cancellationToken);

    protected TNode Track<TNode>(TNode node)
        where TNode : class
    {
        _nodes.Add(node);
        return node;
    }

    protected IAsyncEnumerable<IAcknowledgableMessage<T>> Enumerate<TMessage, T>(SourceNode<TMessage> source, PipelineContext context,
        CancellationToken cancellationToken)
        where TMessage : IAcknowledgableMessage<T> =>
        Cast<TMessage, T>(Track(source).OpenStream(context, cancellationToken), cancellationToken);

    public static IDataStream<T> Stream<T>(IAsyncEnumerable<T> items) => new DataStream<T>(items, "round trip");

    private static async IAsyncEnumerable<IAcknowledgableMessage<T>> Cast<TMessage, T>(IAsyncEnumerable<TMessage> messages,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where TMessage : IAcknowledgableMessage<T>
    {
        await foreach (var message in messages.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return message;
        }
    }
}

/// <summary>A sink that is not a broker: it keeps what it is given.</summary>
public sealed class CollectingSink<T> : SinkNode<T>
{
    public List<T> Items { get; } = [];

    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Items.Add(item);
        }
    }
}

/// <summary>The record every scenario sends.</summary>
public sealed record Order(int Id, string CustomerName, decimal Total, DateTimeOffset PlacedAt)
{
    public static Order Create(int id) => new(id, $"customer {id}", id * 1.25m, new DateTimeOffset(2026, 9, 30, 10, 0, 0, TimeSpan.FromHours(10)).AddMinutes(id));
}

/// <summary>Collects dead letters.</summary>
public sealed class CapturingDeadLetterSink : IDeadLetterSink
{
    public List<DeadLetterEnvelope> Captured { get; } = [];

    public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        lock (Captured)
        {
            Captured.Add(envelope);
        }

        return Task.CompletedTask;
    }
}

public static class Streams
{
    /// <summary>The first <paramref name="count" /> items, or fewer if <paramref name="timeout" /> passes first.</summary>
    public static async Task<List<T>> TakeAsync<T>(this IAsyncEnumerable<T> source, int count, TimeSpan timeout,
        Func<T, Task>? onItem = null)
    {
        using var cts = new CancellationTokenSource(timeout);
        var items = new List<T>();

        try
        {
            await foreach (var item in source.WithCancellation(cts.Token).ConfigureAwait(false))
            {
                items.Add(item);

                if (onItem is not null)
                    await onItem(item).ConfigureAwait(false);

                if (items.Count >= count)
                    break;
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // The timeout ends the read with what arrived.
        }

        return items;
    }

    /// <summary>Ends after <paramref name="count" /> items, so an endless source can feed a sink that runs to completion.</summary>
    public static async IAsyncEnumerable<T> Limit<T>(this IAsyncEnumerable<T> source, int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (count <= 0)
            yield break;

        var taken = 0;

        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;

            if (++taken >= count)
                yield break;
        }
    }

    public static async IAsyncEnumerable<T> ToAsync<T>(this IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
