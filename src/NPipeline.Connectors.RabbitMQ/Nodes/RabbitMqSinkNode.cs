using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Metrics;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Connectors.RabbitMQ.Reliability;
using NPipeline.Connectors.RabbitMQ.Topology;
using NPipeline.Connectors.Serialization;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;
using RabbitMQ.Client;

namespace NPipeline.Connectors.RabbitMQ.Nodes;

/// <summary>
///     Sink node that publishes messages to a RabbitMQ exchange with support for
///     publisher confirms, batching, and acknowledgment pass-through.
/// </summary>
/// <typeparam name="T">The type of messages to publish.</typeparam>
public sealed class RabbitMqSinkNode<T> : SinkNode<T>
{
    private readonly IRabbitMqConnectionManager _connectionManager;
    private readonly ILogger _logger;
    private readonly IRabbitMqMetrics _metrics;
    private readonly RabbitMqSinkOptions _options;
    private readonly NResilience.Resilience _publishPolicy;
    private readonly IMessageSerializer _serializer;
    private bool _topologyDeclared;

    /// <summary>
    ///     Creates a new <see cref="RabbitMqSinkNode{T}" /> with full dependency injection.
    /// </summary>
    public RabbitMqSinkNode(
        RabbitMqSinkOptions options,
        IRabbitMqConnectionManager connectionManager,
        IMessageSerializer serializer,
        IRabbitMqMetrics? metrics = null,
        ILogger<RabbitMqSinkNode<T>>? logger = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _connectionManager = connectionManager ?? throw new ArgumentNullException(nameof(connectionManager));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _metrics = metrics ?? NullRabbitMqMetrics.Instance;
        _logger = logger ?? NullLogger<RabbitMqSinkNode<T>>.Instance;
        _options.Validate();
        _publishPolicy = _options.Resilience.WithListener(OnResilienceEvent);
    }

    /// <inheritdoc />
    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Ensure topology is declared once
        if (!_topologyDeclared && _options.Topology is { AutoDeclare: true })
        {
            var setupChannel = await _connectionManager.GetPooledChannelAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                await TopologyDeclarer.DeclareSinkTopologyAsync(setupChannel, _options, _logger, cancellationToken)
                    .ConfigureAwait(false);

                _topologyDeclared = true;
            }
            finally
            {
                _connectionManager.ReturnChannel(setupChannel);
            }
        }

        if (_options.Batching is not null)
            await ExecuteBatchedAsync(input, cancellationToken).ConfigureAwait(false);
        else
            await ExecuteSequentialAsync(input, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteSequentialAsync(IDataStream<T> input, CancellationToken cancellationToken)
    {
        var lease = new ChannelLease(await _connectionManager.GetPooledChannelAsync(cancellationToken).ConfigureAwait(false));

        try
        {
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await PublishItemAsync(lease, item, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _connectionManager.ReturnChannel(lease.Channel);
        }
    }

    private async Task ExecuteBatchedAsync(IDataStream<T> input, CancellationToken cancellationToken)
    {
        var batchOptions = _options.Batching!;
        var batch = new List<(T Item, ReadOnlyMemory<byte> Body, IAcknowledgableMessage? SourceMsg)>(batchOptions.BatchSize);

        using var lingerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var lingerTimer = new PeriodicTimer(batchOptions.LingerTime);

        // Background linger-flush task
        var flushTask = Task.Run(async () =>
        {
            try
            {
                while (await lingerTimer.WaitForNextTickAsync(lingerCts.Token).ConfigureAwait(false))
                {
                    if (batch.Count > 0)
                        await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }, cancellationToken);

        try
        {
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var body = SerializeItem(item);
                var sourceMsg = ExtractSourceMessage(item);
                batch.Add((item, body, sourceMsg));

                if (batch.Count >= batchOptions.BatchSize)
                    await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
            }

            // Flush remaining
            if (batch.Count > 0)
                await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await lingerCts.CancelAsync().ConfigureAwait(false);
            lingerTimer.Dispose();

            try
            {
                await flushTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }
    }

    private async Task FlushBatchAsync(
        List<(T Item, ReadOnlyMemory<byte> Body, IAcknowledgableMessage? SourceMsg)> batch,
        CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
            return;

        var lease = new ChannelLease(await _connectionManager.GetPooledChannelAsync(cancellationToken).ConfigureAwait(false));

        try
        {
            var sw = Stopwatch.StartNew();

            // Each message is retried on its own. Retrying the whole batch would publish the messages before the
            // failure a second time.
            foreach (var (item, body, _) in batch)
            {
                var routingKey = ResolveRoutingKey(item);
                var properties = BuildBasicProperties(item);

                await PublishWithRetryAsync(lease, routingKey, properties, body, cancellationToken).ConfigureAwait(false);
            }

            sw.Stop();

            // Publisher confirms are handled by BasicPublishAsync when
            // PublisherConfirmationTrackingEnabled is set on the channel.
            if (_options.EnablePublisherConfirms)
                _metrics.RecordConfirmLatency(_options.ExchangeName, sw.Elapsed.TotalMilliseconds);

            // Acknowledge source messages
            foreach (var (_, _, sourceMsg) in batch)
            {
                if (sourceMsg is not null)
                    await AcknowledgeSourceMessageAsync(sourceMsg, cancellationToken).ConfigureAwait(false);
            }

            _metrics.RecordBatchPublished(_options.ExchangeName, batch.Count);
            LogMessages.BatchPublished(_logger, batch.Count, _options.ExchangeName);

            batch.Clear();
        }
        catch (Exception ex) when (!IsCancellation(ex, cancellationToken))
        {
            LogMessages.PublishFailed(_logger, ex, _options.ExchangeName, ex.Message);

            if (!_options.ContinueOnError)
                throw;

            batch.Clear();
        }
        finally
        {
            _connectionManager.ReturnChannel(lease.Channel);
        }
    }

    private async Task PublishItemAsync(ChannelLease lease, T item, CancellationToken cancellationToken)
    {
        var routingKey = _options.RoutingKey;

        try
        {
            var body = SerializeItem(item);
            routingKey = ResolveRoutingKey(item);

            // Built once, so every attempt carries the same message ID and a consumer can discard a duplicate.
            var properties = BuildBasicProperties(item);

            var sw = Stopwatch.StartNew();
            await PublishWithRetryAsync(lease, routingKey, properties, body, cancellationToken).ConfigureAwait(false);
            sw.Stop();

            _metrics.RecordPublished(_options.ExchangeName, routingKey, 1);
            _metrics.RecordPublishLatency(_options.ExchangeName, sw.Elapsed.TotalMilliseconds);

            LogMessages.MessagePublished(_logger, _options.ExchangeName, routingKey);
        }
        catch (Exception ex) when (!IsCancellation(ex, cancellationToken))
        {
            _metrics.RecordPublishError(_options.ExchangeName, routingKey);
            LogMessages.PublishFailed(_logger, ex, _options.ExchangeName, ex.Message);

            if (!_options.ContinueOnError)
                throw;

            return;
        }

        // Acknowledge the source message only once the publish has succeeded, and outside the retried call: a failed
        // acknowledgement must not publish the message again, because it has already reached the exchange.
        var sourceMsg = ExtractSourceMessage(item);

        if (sourceMsg is not null)
            await AcknowledgeSourceMessageAsync(sourceMsg, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Publishes one message, retrying as <see cref="RabbitMqSinkOptions.Resilience" /> allows. Nothing but the
    ///     publish itself runs inside the retried call.
    /// </summary>
    private ValueTask PublishWithRetryAsync(
        ChannelLease lease,
        string routingKey,
        BasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        return _publishPolicy.RunAsync(
            static (state, ct) => state.Node.PublishOnceAsync(state.Lease, state.RoutingKey, state.Properties, state.Body, ct),
            (Node: this, Lease: lease, RoutingKey: routingKey, Properties: properties, Body: body),
            cancellationToken);
    }

    private async ValueTask PublishOnceAsync(
        ChannelLease lease,
        string routingKey,
        BasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken)
    {
        var channel = await EnsureOpenChannelAsync(lease, cancellationToken).ConfigureAwait(false);
        var sw = Stopwatch.StartNew();

        await channel.BasicPublishAsync(
            _options.ExchangeName,
            routingKey,
            _options.Mandatory,
            properties,
            body,
            cancellationToken).ConfigureAwait(false);

        sw.Stop();

        // Publisher confirms are handled by BasicPublishAsync when
        // PublisherConfirmationTrackingEnabled is set on the channel.
        if (_options.EnablePublisherConfirms)
            _metrics.RecordConfirmLatency(_options.ExchangeName, sw.Elapsed.TotalMilliseconds);
    }

    /// <summary>
    ///     Replaces the leased channel when it has closed. A closed channel never reopens, so retrying a publish on it
    ///     fails the same way every time; a fresh channel from the pool reconnects if the connection was lost.
    /// </summary>
    private async ValueTask<IChannel> EnsureOpenChannelAsync(ChannelLease lease, CancellationToken cancellationToken)
    {
        if (lease.Channel.IsOpen)
            return lease.Channel;

        _connectionManager.ReturnChannel(lease.Channel);
        lease.Channel = await _connectionManager.GetPooledChannelAsync(cancellationToken).ConfigureAwait(false);
        return lease.Channel;
    }

    private void OnResilienceEvent(CallEvent callEvent)
    {
        if (callEvent.Kind != CallEventKind.Retrying)
            return;

        LogMessages.PublishRetrying(
            _logger,
            callEvent.Exception,
            _options.ExchangeName,
            (callEvent.Delay ?? TimeSpan.Zero).TotalMilliseconds,
            callEvent.AttemptNumber,
            _options.Resilience.Attempts);
    }

    private static bool IsCancellation(Exception exception, CancellationToken cancellationToken)
    {
        return exception is OperationCanceledException && cancellationToken.IsCancellationRequested;
    }

    private ReadOnlyMemory<byte> SerializeItem(T item)
    {
        // If the item is an IAcknowledgableMessage, serialize the body
        if (item is IAcknowledgableMessage ackMsg)
            return _serializer.Serialize(ackMsg.Body);

        return _serializer.Serialize(item);
    }

    private string ResolveRoutingKey(T item)
    {
        if (_options.RoutingKeySelector is not null)
        {
            if (item is IAcknowledgableMessage ackMsg)
                return _options.RoutingKeySelector(ackMsg.Body);

            return _options.RoutingKeySelector(item!);
        }

        return _options.RoutingKey;
    }

    private BasicProperties BuildBasicProperties(T item)
    {
        var properties = new BasicProperties
        {
            ContentType = _options.ContentType ?? _serializer.ContentType,
            Persistent = _options.Persistent,
            MessageId = Guid.NewGuid().ToString(),
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
        };

        if (_options.AppId is not null)
            properties.AppId = _options.AppId;

        // Forward metadata from source message if available
        if (item is IRabbitMqMessageMetadata sourceMeta)
        {
            if (sourceMeta.CorrelationId is not null)
                properties.CorrelationId = sourceMeta.CorrelationId;

            if (sourceMeta.Headers is not null)
            {
                properties.Headers ??= new Dictionary<string, object?>();

                foreach (var header in sourceMeta.Headers)
                {
                    properties.Headers[header.Key] = header.Value;
                }
            }
        }

        return properties;
    }

    private static IAcknowledgableMessage? ExtractSourceMessage(T item)
    {
        return item as IAcknowledgableMessage;
    }

    private async Task AcknowledgeSourceMessageAsync(IAcknowledgableMessage message, CancellationToken cancellationToken)
    {
        if (_options is not null &&
            ExtractAckStrategy() == AcknowledgmentStrategy.AutoOnSinkSuccess &&
            !message.IsAcknowledged)
        {
            await message.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);
            _metrics.RecordAck("source", 1);
        }
    }

    private AcknowledgmentStrategy ExtractAckStrategy()
    {
        // The source options are not available in the sink, but
        // we default to AutoOnSinkSuccess which is the most common pattern.
        return AcknowledgmentStrategy.AutoOnSinkSuccess;
    }

    /// <summary>
    ///     The channel a sequence of publishes uses, replaced when it closes. The holder keeps the current channel so
    ///     it is the one returned to the pool.
    /// </summary>
    private sealed class ChannelLease(IChannel channel)
    {
        public IChannel Channel { get; set; } = channel;
    }
}
