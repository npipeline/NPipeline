using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Connectors.RabbitMQ.Topology;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NPipeline.Connectors.RabbitMQ.Nodes;

/// <summary>
///     Consumes a RabbitMQ queue. Each message is handed on as a <see cref="RabbitMqMessage{T}" /> to be acknowledged or
///     rejected; at most <see cref="RabbitMqReadOptions.PrefetchCount" /> are unsettled at a time. When the read ends, the
///     consumer is cancelled, messages not yet handed on go back on the queue, and the channel stays open until the messages
///     handed on are settled (up to <see cref="RabbitMqReadOptions.SettleTimeout" />). Create one with
///     <see cref="RabbitMqConnector.Source{T}" />.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class RabbitMqSourceNode<T> : SourceNode<RabbitMqMessage<T>>, IAsyncDisposable
{
    internal const string ConnectorName = "rabbitmq";

    private readonly List<Task> _closing = [];
    private readonly MessageDecoder<T> _decoder;
    private readonly CancellationTokenSource _disposing = new();
    private bool _disposed;
    private readonly RabbitMqReadOptions _options;

    /// <summary>Creates a source and validates <paramref name="options" />.</summary>
    public RabbitMqSourceNode(RabbitMqReadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _decoder = new MessageDecoder<T>(ConnectorName, options.Queue, options.Serializer, options.RowErrorHandler, options.RawExcerptLength);
    }

    /// <summary>Closes the channels of finished reads now, without waiting for their messages to be settled.</summary>
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
    public override IDataStream<RabbitMqMessage<T>> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new DataStream<RabbitMqMessage<T>>(ConsumeAsync(context, cancellationToken), $"RabbitMqSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<RabbitMqMessage<T>> ConsumeAsync(PipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var logger = context.Observability.LoggerFactory.CreateLogger(typeof(RabbitMqSourceNode<T>).FullName ?? nameof(RabbitMqSourceNode<T>));
        var deadLetters = OpenDeadLetterChannel(context);
        var inFlight = new InFlightMessages();
        var channel = await _options.Connection.CreateChannelAsync(cancellationToken).ConfigureAwait(false);

        // The buffer holds what the broker has delivered and the pipeline has not yet taken; the prefetch bounds it.
        var buffer = Channel.CreateBounded<RabbitMqMessage<T>>(new BoundedChannelOptions(_options.PrefetchCount)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });

        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        string? consumerTag = null;
        long sequence = 0;

        try
        {
            await channel.BasicQosAsync(0, _options.PrefetchCount, false, cancellationToken).ConfigureAwait(false);
            await TopologyDeclarer.DeclareQueueAsync(channel, _options.Queue, _options.Topology, logger, cancellationToken).ConfigureAwait(false);

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += (_, args) => ReceiveAsync(args, channel, buffer.Writer, inFlight, deadLetters, logger, ++sequence, stop.Token);

            consumer.ShutdownAsync += (_, args) =>
            {
                if (!stop.IsCancellationRequested)
                    buffer.Writer.TryComplete(new InvalidOperationException($"The RabbitMQ channel for queue '{_options.Queue}' closed: {args.ReplyText}"));

                return Task.CompletedTask;
            };

            consumerTag = await channel.BasicConsumeAsync(_options.Queue, false, _options.ConsumerTag ?? string.Empty, false, _options.Exclusive, null,
                consumer, cancellationToken).ConfigureAwait(false);

            LogMessages.ConsumerStarted(logger, _options.Queue, _options.PrefetchCount);

            await foreach (var message in buffer.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                yield return message;
            }
        }
        finally
        {
            await stop.CancelAsync().ConfigureAwait(false);
            buffer.Writer.TryComplete();
            await StopAsync(channel, consumerTag, buffer.Reader).ConfigureAwait(false);

            lock (_closing)
            {
                _closing.Add(CloseWhenSettledAsync(channel, inFlight));
            }
        }
    }

    private async Task ReceiveAsync(BasicDeliverEventArgs args, IChannel channel, ChannelWriter<RabbitMqMessage<T>> buffer, InFlightMessages inFlight,
        DeadLetterChannel deadLetters, ILogger logger, long sequence, CancellationToken cancellationToken)
    {
        var tag = args.DeliveryTag;
        var properties = args.BasicProperties;
        var messageId = properties.MessageId ?? $"{_options.Queue}:{args.Redelivered}:{tag}";

        try
        {
            if (_options.MaxDeliveryAttempts is { } max && DeliveryAttempts(properties) is { } attempts && attempts > max)
            {
                LogMessages.PoisonMessageRejected(logger, tag, attempts, max);
                await channel.BasicRejectAsync(tag, false, cancellationToken).ConfigureAwait(false);
                ConnectorDiagnostics.RecordMessagesSettled(ConnectorName, "rejected");
                return;
            }

            if (!_decoder.TryDecode(args.Body.Span, out var body, out var error))
            {
                LogMessages.DeserializationFailed(logger, error, tag, _options.Queue);

                // Throws for Fail, which fails the read; the message stays unsettled and is delivered again.
                _ = await _decoder.HandleFailureAsync(error, args.Body, messageId, sequence, Metadata(args), deadLetters, cancellationToken).ConfigureAwait(false);
                await channel.BasicRejectAsync(tag, false, cancellationToken).ConfigureAwait(false);
                ConnectorDiagnostics.RecordMessagesSettled(ConnectorName, "rejected");
                return;
            }

            inFlight.Add();

            var settlement = new MessageSettlement(
                async ct =>
                {
                    try
                    {
                        await channel.BasicAckAsync(tag, false, ct).ConfigureAwait(false);
                        ConnectorDiagnostics.RecordMessagesSettled(ConnectorName, "acknowledged");
                    }
                    finally
                    {
                        inFlight.Remove();
                    }
                },
                async (requeue, ct) =>
                {
                    try
                    {
                        await channel.BasicRejectAsync(tag, requeue, ct).ConfigureAwait(false);
                        ConnectorDiagnostics.RecordMessagesSettled(ConnectorName, requeue ? "requeued" : "rejected");
                    }
                    finally
                    {
                        inFlight.Remove();
                    }
                });

            var message = new RabbitMqMessage<T>(body, messageId, args.Exchange, args.RoutingKey, tag, args.Redelivered, properties, settlement);

            // Waits while the pipeline is behind; the prefetch stops the broker delivering more meanwhile.
            await buffer.WriteAsync(message, cancellationToken).ConfigureAwait(false);
            ConnectorDiagnostics.RecordRowsRead(ConnectorName, ConnectorName, 1);
        }
        catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException && cancellationToken.IsCancellationRequested)
        {
            // The read ended; an unsettled message is delivered again once the channel closes.
        }
        catch (Exception ex)
        {
            buffer.TryComplete(ex);
        }
    }

    /// <summary>Stops deliveries and puts back the messages that were delivered but never handed on.</summary>
    private static async Task StopAsync(IChannel channel, string? consumerTag, ChannelReader<RabbitMqMessage<T>> unread)
    {
        try
        {
            if (consumerTag is not null && channel.IsOpen)
                await channel.BasicCancelAsync(consumerTag).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // The channel is gone, which requeues its messages anyway.
        }

        while (unread.TryRead(out var message))
        {
            try
            {
                await message.RejectAsync(true).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                // As above.
            }
        }
    }

    private async Task CloseWhenSettledAsync(IChannel channel, InFlightMessages inFlight)
    {
        try
        {
            _ = await inFlight.WhenSettledAsync(_options.SettleTimeout, _disposing.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Disposed: close now.
        }

        try
        {
            await channel.CloseAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // Already closed.
        }

        channel.Dispose();
    }

    /// <summary>
    ///     The delivery count: <c>x-delivery-count</c> (quorum queues count earlier deliveries, so this delivery is one more),
    ///     or the <c>x-death</c> count after dead-letter cycles; <c>null</c> when the queue counts neither.
    /// </summary>
    internal static int? DeliveryAttempts(IReadOnlyBasicProperties properties)
    {
        var headers = properties.Headers;

        if (headers is null)
            return null;

        if (headers.TryGetValue("x-delivery-count", out var count) && ToInt(count) is { } earlier)
            return earlier + 1;

        if (headers.TryGetValue("x-death", out var deaths) && deaths is IList<object> { Count: > 0 } list && list[0] is IDictionary<string, object> first &&
            first.TryGetValue("count", out var deathCount) && ToInt(deathCount) is { } died)
            return died + 1;

        return null;
    }

    private static int? ToInt(object? value) => value switch
    {
        long l => (int)Math.Min(l, int.MaxValue),
        int i => i,
        short s => s,
        byte b => b,
        _ => null,
    };

    private static Dictionary<string, object> Metadata(BasicDeliverEventArgs args)
    {
        var metadata = new Dictionary<string, object>
        {
            ["Exchange"] = args.Exchange,
            ["RoutingKey"] = args.RoutingKey,
            ["Redelivered"] = args.Redelivered,
        };

        foreach (var (key, value) in args.BasicProperties.Headers ?? new Dictionary<string, object?>())
        {
            if (value is not null)
                metadata[$"Header.{key}"] = value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : value;
        }

        return metadata;
    }
}
