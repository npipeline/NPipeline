using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Metrics;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Connectors.RabbitMQ.Topology;
using NPipeline.Connectors.Serialization;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
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
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
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

    private async Task ExecuteSequentialAsync(IDataPipe<T> input, CancellationToken cancellationToken)
    {
        var channel = await _connectionManager.GetPooledChannelAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await PublishItemAsync(channel, item, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _connectionManager.ReturnChannel(channel);
        }
    }

    private async Task ExecuteBatchedAsync(IDataPipe<T> input, CancellationToken cancellationToken)
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

        var channel = await _connectionManager.GetPooledChannelAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var sw = Stopwatch.StartNew();

            foreach (var (item, body, _) in batch)
            {
                var routingKey = ResolveRoutingKey(item);
                var properties = BuildBasicProperties(item);

                await channel.BasicPublishAsync(
                    _options.ExchangeName,
                    routingKey,
                    _options.Mandatory,
                    properties,
                    body,
                    cancellationToken).ConfigureAwait(false);
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
        catch (Exception ex)
        {
            LogMessages.PublishFailed(_logger, ex, _options.ExchangeName, ex.Message);

            if (!_options.ContinueOnError)
                throw;

            batch.Clear();
        }
        finally
        {
            _connectionManager.ReturnChannel(channel);
        }
    }

    private async Task PublishItemAsync(IChannel channel, T item, CancellationToken cancellationToken)
    {
        var attempt = 0;

        while (true)
        {
            try
            {
                var sw = Stopwatch.StartNew();

                var body = SerializeItem(item);
                var routingKey = ResolveRoutingKey(item);
                var properties = BuildBasicProperties(item);

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

                _metrics.RecordPublished(_options.ExchangeName, routingKey, 1);
                _metrics.RecordPublishLatency(_options.ExchangeName, sw.Elapsed.TotalMilliseconds);

                LogMessages.MessagePublished(_logger, _options.ExchangeName, routingKey);

                // Acknowledge source message
                var sourceMsg = ExtractSourceMessage(item);

                if (sourceMsg is not null)
                    await AcknowledgeSourceMessageAsync(sourceMsg, cancellationToken).ConfigureAwait(false);

                return;
            }
            catch (Exception) when (attempt < _options.MaxRetries)
            {
                attempt++;
                var delay = _options.RetryBaseDelayMs * (int)Math.Pow(2, attempt - 1);

                LogMessages.PublishRetrying(_logger, delay, attempt, _options.MaxRetries, _options.ExchangeName);

                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt >= _options.MaxRetries)
            {
                _metrics.RecordPublishError(_options.ExchangeName, ResolveRoutingKey(item));
                LogMessages.PublishFailed(_logger, ex, _options.ExchangeName, ex.Message);

                if (!_options.ContinueOnError)
                    throw;

                return;
            }
        }
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
}
