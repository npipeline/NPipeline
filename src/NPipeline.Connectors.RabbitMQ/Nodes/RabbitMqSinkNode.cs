using System.Diagnostics;
using System.Runtime.ExceptionServices;
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
            var setupChannel = await _connectionManager.GetPooledChannelAsync(_options.EnablePublisherConfirms, cancellationToken).ConfigureAwait(false);

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
        var lease = new ChannelLease(await _connectionManager.GetPooledChannelAsync(_options.EnablePublisherConfirms, cancellationToken).ConfigureAwait(false));

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

        // Guards the batch. Adding, the size-triggered flush, and the linger flush all hold it, so only one flush runs
        // at a time and a flush never sees the batch change under it.
        using var batchLock = new SemaphoreSlim(1, 1);
        using var lingerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var lingerTimer = new PeriodicTimer(batchOptions.LingerTime);

        // Background linger-flush task
        var flushTask = Task.Run(async () =>
        {
            try
            {
                while (await lingerTimer.WaitForNextTickAsync(lingerCts.Token).ConfigureAwait(false))
                {
                    await batchLock.WaitAsync(lingerCts.Token).ConfigureAwait(false);

                    try
                    {
                        await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        _ = batchLock.Release();
                    }
                }
            }
            catch (OperationCanceledException) when (lingerCts.IsCancellationRequested)
            {
                // Expected during shutdown
            }
        }, cancellationToken);

        try
        {
            try
            {
                await ConsumeIntoBatchesAsync().ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The pipeline is shutting down. Publish what is already batched, bounded by ShutdownFlushTimeout, so
                // messages taken from the input are not dropped; then report the cancellation.
                await lingerCts.CancelAsync().ConfigureAwait(false);
                await FlushOnShutdownAsync(batch, flushTask).ConfigureAwait(false);
                throw;
            }

            // Stop the linger flush before the final flush, so the two cannot run together.
            await lingerCts.CancelAsync().ConfigureAwait(false);
            await flushTask.ConfigureAwait(false);

            // Flush remaining
            await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await lingerCts.CancelAsync().ConfigureAwait(false);

            try
            {
                await flushTask.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Already surfaced above, or superseded by the exception leaving this block.
            }
        }

        async Task ConsumeIntoBatchesAsync()
        {
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                // A linger flush that failed has already stopped the timer; fail the node now rather than at the end.
                if (flushTask.IsFaulted)
                    await flushTask.ConfigureAwait(false);

                var body = SerializeItem(item);
                var sourceMsg = ExtractSourceMessage(item);

                await batchLock.WaitAsync(cancellationToken).ConfigureAwait(false);

                try
                {
                    batch.Add((item, body, sourceMsg));

                    if (batch.Count >= batchOptions.BatchSize)
                        await FlushBatchAsync(batch, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    _ = batchLock.Release();
                }
            }
        }
    }

    /// <summary>
    ///     Publishes what is left in the batch after the pipeline was cancelled, with its own token bounded by
    ///     <see cref="RabbitMqSinkOptions.ShutdownFlushTimeout" />. A message that cannot be published in time stays
    ///     unacknowledged, so the broker redelivers it.
    /// </summary>
    private async Task FlushOnShutdownAsync(
        List<(T Item, ReadOnlyMemory<byte> Body, IAcknowledgableMessage? SourceMsg)> batch,
        Task lingerFlush)
    {
        try
        {
            // Wait for an in-flight linger flush, so the two never publish together.
            await lingerFlush.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // The linger flush's own failure does not stop the shutdown flush.
        }

        if (batch.Count == 0)
            return;

        using var shutdownCts = new CancellationTokenSource(_options.ShutdownFlushTimeout);

        try
        {
            await FlushBatchAsync(batch, shutdownCts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Cancellation is what the caller reports; the unpublished messages are redelivered by the broker.
            LogMessages.PublishFailed(_logger, ex, _options.ExchangeName, ex.Message);
        }
    }

    /// <summary>
    ///     Publishes the batch in order, each message retried on its own, and acknowledges every source message whose
    ///     publish succeeded, even when a later message fails. The caller must hold the batch lock.
    /// </summary>
    private async Task FlushBatchAsync(
        List<(T Item, ReadOnlyMemory<byte> Body, IAcknowledgableMessage? SourceMsg)> batch,
        CancellationToken cancellationToken)
    {
        if (batch.Count == 0)
            return;

        var published = 0;
        ExceptionDispatchInfo? failure = null;
        var failedRoutingKey = _options.RoutingKey;
        var lease = new ChannelLease(await _connectionManager.GetPooledChannelAsync(_options.EnablePublisherConfirms, cancellationToken).ConfigureAwait(false));

        try
        {
            // Retrying the whole batch would publish the messages before the failure a second time.
            foreach (var (item, body, _) in batch)
            {
                try
                {
                    failedRoutingKey = _options.RoutingKey;
                    var routingKey = ResolveRoutingKey(item);
                    failedRoutingKey = routingKey;
                    var properties = BuildBasicProperties(item);

                    await PublishWithRetryAsync(lease, routingKey, properties, body, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    failure = ExceptionDispatchInfo.Capture(ex);
                    break;
                }

                published++;
            }
        }
        finally
        {
            _connectionManager.ReturnChannel(lease.Channel);
        }

        var cancelled = failure?.SourceException is { } error && IsCancellation(error, cancellationToken);

        // The messages before a failure reached the exchange. Leaving them unacknowledged would redeliver them, and the
        // next run would publish them again. On cancellation the acknowledgements get their own short deadline, since
        // the pipeline's token has already fired.
        using (var ackCts = cancelled ? new CancellationTokenSource(_options.ShutdownFlushTimeout) : null)
        {
            var ackToken = ackCts?.Token ?? cancellationToken;

            for (var i = 0; i < published; i++)
            {
                if (batch[i].SourceMsg is { } sourceMsg)
                    await AcknowledgeSourceMessageAsync(sourceMsg, ackToken).ConfigureAwait(false);
            }
        }

        if (published > 0)
        {
            _metrics.RecordBatchPublished(_options.ExchangeName, published);
            LogMessages.BatchPublished(_logger, published, _options.ExchangeName);
        }

        // Published messages leave the batch whatever happens next, so a later flush never publishes them again.
        batch.RemoveRange(0, published);

        if (cancelled)
            failure!.Throw();

        if (failure is not null)
        {
            _metrics.RecordPublishError(_options.ExchangeName, failedRoutingKey);
            LogMessages.PublishFailed(_logger, failure.SourceException, _options.ExchangeName, failure.SourceException.Message);

            // The failed message and those after it stay unacknowledged, so the broker redelivers them.
            if (!_options.ContinueOnError)
                failure.Throw();
        }

        batch.Clear();
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

        // With publisher confirms, BasicPublishAsync waits for the broker's confirm. Bound that wait: a confirm that
        // never arrives fails this attempt as a timeout, which the policy retries. Without confirms there is no wait
        // to bound; the publish completes once the message is written to the connection.
        using var confirmCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        if (_options.EnablePublisherConfirms)
            confirmCts.CancelAfter(_options.ConfirmTimeout);

        try
        {
            await channel.BasicPublishAsync(
                _options.ExchangeName,
                routingKey,
                _options.Mandatory,
                properties,
                body,
                confirmCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (confirmCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"The broker did not confirm the publish to exchange '{_options.ExchangeName}' within {_options.ConfirmTimeout}.",
                ex);
        }

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
        lease.Channel = await _connectionManager.GetPooledChannelAsync(_options.EnablePublisherConfirms, cancellationToken).ConfigureAwait(false);
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
