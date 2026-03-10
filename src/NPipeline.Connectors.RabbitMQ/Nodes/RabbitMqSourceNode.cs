using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Metrics;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Connectors.RabbitMQ.Topology;
using NPipeline.Connectors.Serialization;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NPipeline.Connectors.RabbitMQ.Nodes;

/// <summary>
///     Source node that consumes messages from a RabbitMQ queue using a push-based
///     <see cref="AsyncEventingBasicConsumer" /> backed by a bounded <see cref="Channel{T}" />
///     for backpressure integration with RabbitMQ's prefetch QoS.
/// </summary>
/// <typeparam name="T">The type of messages to consume.</typeparam>
public sealed class RabbitMqSourceNode<T> : SourceNode<RabbitMqMessage<T>>
{
    private readonly IRabbitMqConnectionManager _connectionManager;
    private readonly ILogger _logger;
    private readonly IRabbitMqMetrics _metrics;
    private readonly RabbitMqSourceOptions _options;
    private readonly IMessageSerializer _serializer;
    private string? _activeConsumerTag;
    private IChannel? _channel;

    /// <summary>
    ///     Creates a new <see cref="RabbitMqSourceNode{T}" /> with full dependency injection.
    /// </summary>
    public RabbitMqSourceNode(
        RabbitMqSourceOptions options,
        IRabbitMqConnectionManager connectionManager,
        IMessageSerializer serializer,
        IRabbitMqMetrics? metrics = null,
        ILogger<RabbitMqSourceNode<T>>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _metrics = metrics ?? NullRabbitMqMetrics.Instance;
        _logger = logger ?? NullLogger<RabbitMqSourceNode<T>>.Instance;
        _options.Validate();
    }

    /// <inheritdoc />
    public override IDataStream<RabbitMqMessage<T>> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var stream = ConsumeMessagesAsync(cancellationToken);
        return new DataStream<RabbitMqMessage<T>>(stream, $"RabbitMqSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<RabbitMqMessage<T>> ConsumeMessagesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Create a dedicated channel for this consumer (not pooled)
        _channel = await _connectionManager.CreateChannelAsync(cancellationToken).ConfigureAwait(false);

        // Set QoS prefetch
        await _channel.BasicQosAsync(
            0, _options.PrefetchCount, _options.PrefetchGlobal,
            cancellationToken).ConfigureAwait(false);

        // Optionally declare topology
        await TopologyDeclarer.DeclareSourceTopologyAsync(_channel, _options, _logger, cancellationToken)
            .ConfigureAwait(false);

        // Create bounded channel for push-to-pull bridging
        var bufferChannel = Channel.CreateBounded<RabbitMqMessage<T>>(
            new BoundedChannelOptions(_options.InternalBufferCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = _options.ConsumerDispatchConcurrency <= 1,
            });

        // Set up the async consumer
        var consumer = new AsyncEventingBasicConsumer(_channel);

        consumer.ReceivedAsync += async (_, args) =>
        {
            try
            {
                var sw = Stopwatch.StartNew();

                // Check poison message (delivery attempt count)
                if (_options.MaxDeliveryAttempts.HasValue && _options.RejectOnMaxDeliveryAttempts)
                {
                    var attemptCount = GetDeliveryAttemptCount(args);

                    if (attemptCount > _options.MaxDeliveryAttempts.Value)
                    {
                        LogMessages.PoisonMessageRejected(
                            _logger, args.DeliveryTag, attemptCount, _options.MaxDeliveryAttempts.Value);

                        await _channel.BasicRejectAsync(args.DeliveryTag, false, cancellationToken)
                            .ConfigureAwait(false);

                        _metrics.RecordNack(_options.QueueName, 1, false);
                        return;
                    }
                }

                // Deserialize
                T body;

                try
                {
                    body = _serializer.Deserialize<T>(args.Body);
                }
                catch (Exception ex)
                {
                    LogMessages.DeserializationFailed(_logger, ex, args.DeliveryTag, _options.QueueName);
                    _metrics.RecordDeserializationError(_options.QueueName);

                    if (_options.ContinueOnDeserializationError)
                    {
                        await _channel.BasicRejectAsync(args.DeliveryTag, false, cancellationToken)
                            .ConfigureAwait(false);

                        return;
                    }

                    // Complete writer with error to propagate to the pipeline
                    bufferChannel.Writer.TryComplete(ex);
                    return;
                }

                var messageId = args.BasicProperties?.MessageId ?? $"{args.Exchange}-{args.RoutingKey}-{args.DeliveryTag}";
                var capturedChannel = _channel;
                var capturedDeliveryTag = args.DeliveryTag;

                var message = new RabbitMqMessage<T>(
                    body,
                    messageId,
                    args.Exchange,
                    args.RoutingKey,
                    capturedDeliveryTag,
                    args.Redelivered,
                    args.BasicProperties ?? new BasicProperties(),
                    async ct => await capturedChannel.BasicAckAsync(capturedDeliveryTag, false, ct).ConfigureAwait(false),
                    async (requeue, ct) => await capturedChannel.BasicNackAsync(capturedDeliveryTag, false, requeue, ct).ConfigureAwait(false));

                sw.Stop();
                _metrics.RecordConsumeLatency(_options.QueueName, sw.Elapsed.TotalMilliseconds);

                LogMessages.MessageConsumed(_logger, args.DeliveryTag, _options.QueueName);

                // Write to bounded channel — blocks if buffer is full (backpressure)
                await bufferChannel.Writer.WriteAsync(message, cancellationToken).ConfigureAwait(false);

                _metrics.RecordConsumed(_options.QueueName, 1);
            }
            catch (OperationCanceledException)
            {
                bufferChannel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                bufferChannel.Writer.TryComplete(ex);
            }
        };

        consumer.UnregisteredAsync += (_, _) =>
        {
            bufferChannel.Writer.TryComplete();
            return Task.CompletedTask;
        };

        consumer.ShutdownAsync += (_, args) =>
        {
            LogMessages.ConsumerShutdown(_logger, _options.QueueName, args.ReplyText ?? "unknown");

            bufferChannel.Writer.TryComplete(
                new InvalidOperationException($"Consumer channel shutdown: {args.ReplyText}"));

            return Task.CompletedTask;
        };

        // Start consuming
        _activeConsumerTag = await _channel.BasicConsumeAsync(
            _options.QueueName,
            false,
            _options.ConsumerTag ?? "",
            false,
            _options.Exclusive,
            null,
            consumer,
            cancellationToken).ConfigureAwait(false);

        LogMessages.ConsumerStarted(_logger, _options.QueueName, _options.PrefetchCount);

        // Read from the bounded channel — this is the IAsyncEnumerable surface
        await foreach (var message in bufferChannel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return message;
        }
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            // Cancel the consumer
            if (_activeConsumerTag is not null)
            {
                try
                {
                    await _channel.BasicCancelAsync(_activeConsumerTag).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort cancellation
                }
            }

            try
            {
                await _channel.CloseAsync().ConfigureAwait(false);
                _channel.Dispose();
            }
            catch
            {
                // Best-effort cleanup
            }

            _channel = null;
        }

        await base.DisposeAsync().ConfigureAwait(false);
    }

    private static int GetDeliveryAttemptCount(BasicDeliverEventArgs args)
    {
        // Try to read x-death header for delivery count
        if (args.BasicProperties?.Headers is not null &&
            args.BasicProperties.Headers.TryGetValue("x-death", out var xDeathObj) &&
            xDeathObj is IList<object> xDeathList &&
            xDeathList.Count > 0 &&
            xDeathList[0] is IDictionary<string, object> firstDeath &&
            firstDeath.TryGetValue("count", out var countObj))
        {
            return countObj switch
            {
                long l => (int)l,
                int i => i,
                _ => 1,
            };
        }

        // Fallback: if redelivered, assume at least 2nd attempt
        return args.Redelivered
            ? 2
            : 1;
    }
}
