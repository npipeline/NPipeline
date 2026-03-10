using System.Text.Json;
using System.Text.Json.Serialization;
using System.Transactions;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Connection;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.Connectors.Configuration;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Azure.ServiceBus.Nodes;

/// <summary>
///     Sink node that publishes messages to an Azure Service Bus <em>topic</em>.
///     Supports batching (up to 100 messages) and automatic source-message acknowledgment.
/// </summary>
/// <typeparam name="T">The type of data to publish.</typeparam>
/// <remarks>
///     Requires <see cref="ServiceBusConfiguration.TopicName" /> to be set.
///     For queue publishing, use <see cref="ServiceBusQueueSinkNode{T}" />.
/// </remarks>
public sealed class ServiceBusTopicSinkNode<T> : SinkNode<T>
{
    private readonly AcknowledgmentStrategy _ackStrategy;
    private readonly ServiceBusConfiguration _configuration;
    private readonly ILogger _logger;
    private readonly ServiceBusClient? _ownedClient;
    private readonly bool _ownsClient;
    private readonly ServiceBusSender _sender;
    private readonly JsonSerializerOptions _serializerOptions;

    /// <summary>
    ///     Creates a <see cref="ServiceBusTopicSinkNode{T}" /> from the supplied configuration.
    ///     A dedicated <see cref="ServiceBusClient" /> is created and owned by this node.
    /// </summary>
    public ServiceBusTopicSinkNode(ServiceBusConfiguration configuration, ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSink();

        if (string.IsNullOrWhiteSpace(_configuration.TopicName))
        {
            throw new ArgumentException("TopicName must be specified for a topic sink node.",
                nameof(configuration));
        }

        _serializerOptions = CreateSerializerOptions(_configuration);
        _ackStrategy = _configuration.AcknowledgmentStrategy;
        _logger = logger ?? NullLogger.Instance;
        _ownedClient = ServiceBusClientFactory.Create(_configuration);
        _sender = _ownedClient.CreateSender(_configuration.TopicName);
        _ownsClient = true;
    }

    /// <summary>
    ///     Creates a <see cref="ServiceBusTopicSinkNode{T}" /> with an injected sender (testing or pool).
    /// </summary>
    public ServiceBusTopicSinkNode(
        ServiceBusSender sender,
        ServiceBusConfiguration configuration,
        ILogger? logger = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSink();
        _sender = sender ?? throw new ArgumentNullException(nameof(sender));
        _serializerOptions = CreateSerializerOptions(_configuration);
        _ackStrategy = _configuration.AcknowledgmentStrategy;
        _logger = logger ?? NullLogger.Instance;
        _ownsClient = false;
    }

    /// <inheritdoc />
    public override async Task ConsumeAsync(
        IDataStream<T> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (_configuration.EnableBatchSending && _configuration.BatchSize > 1)
            await ExecuteBatchedAsync(input, cancellationToken).ConfigureAwait(false);
        else
            await ExecuteSequentialAsync(input, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteSequentialAsync(IDataStream<T> input, CancellationToken ct)
    {
        await foreach (var item in input.WithCancellation(ct))
        {
            var body = GetSerializableBody(item);
            var message = CreateOutboundMessage(body, item);

            try
            {
                ServiceBusLogMessages.SendingMessages(_logger, 1, _sender.EntityPath);
                await SendMessageWithOptionalTransactionAsync(message, ct).ConfigureAwait(false);
                ServiceBusLogMessages.MessagesSent(_logger, 1, _sender.EntityPath);
                await TryAcknowledgeAsync(item, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (_configuration.ContinueOnError)
            {
                ServiceBusLogMessages.SendFailed(_logger, ex, _sender.EntityPath);
            }
        }
    }

    private async Task ExecuteBatchedAsync(IDataStream<T> input, CancellationToken ct)
    {
        var batch = new List<(object Body, T OriginalItem)>(_configuration.BatchSize);

        async Task FlushAsync()
        {
            if (batch.Count == 0)
                return;

            await SendBatchAsync(batch, ct).ConfigureAwait(false);
            batch.Clear();
        }

        await foreach (var item in input.WithCancellation(ct))
        {
            batch.Add((GetSerializableBody(item), item));

            if (batch.Count >= _configuration.BatchSize)
                await FlushAsync().ConfigureAwait(false);
        }

        await FlushAsync().ConfigureAwait(false);
    }

    private async Task SendBatchAsync(List<(object Body, T OriginalItem)> items, CancellationToken ct)
    {
        ServiceBusLogMessages.SendingMessages(_logger, items.Count, _sender.EntityPath);
        ServiceBusMessageBatch? batch = null;

        try
        {
            batch = await _sender.CreateMessageBatchAsync(ct).ConfigureAwait(false);
            var ackList = new List<T>(items.Count);

            foreach (var (body, originalItem) in items)
            {
                var msg = CreateOutboundMessage(body, originalItem);

                if (!batch.TryAddMessage(msg))
                {
                    await SendBatchWithOptionalTransactionAsync(batch, ct).ConfigureAwait(false);
                    ServiceBusLogMessages.MessagesSent(_logger, batch.Count, _sender.EntityPath);

                    foreach (var item in ackList)
                    {
                        await TryAcknowledgeAsync(item, ct).ConfigureAwait(false);
                    }

                    ackList.Clear();

                    batch.Dispose();
                    batch = await _sender.CreateMessageBatchAsync(ct).ConfigureAwait(false);

                    if (!batch.TryAddMessage(msg))
                    {
                        ServiceBusLogMessages.MessageTooLargeForBatch(_logger, _sender.EntityPath);
                        await SendMessageWithOptionalTransactionAsync(msg, ct).ConfigureAwait(false);
                        await TryAcknowledgeAsync(originalItem, ct).ConfigureAwait(false);
                        continue;
                    }
                }

                ackList.Add(originalItem);
            }

            if (batch.Count > 0)
            {
                await SendBatchWithOptionalTransactionAsync(batch, ct).ConfigureAwait(false);
                ServiceBusLogMessages.MessagesSent(_logger, batch.Count, _sender.EntityPath);

                foreach (var item in ackList)
                {
                    await TryAcknowledgeAsync(item, ct).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex) when (_configuration.ContinueOnError)
        {
            ServiceBusLogMessages.SendFailed(_logger, ex, _sender.EntityPath);
        }
        finally
        {
            batch?.Dispose();
        }
    }

    private async Task TryAcknowledgeAsync(T item, CancellationToken ct)
    {
        if (_ackStrategy != AcknowledgmentStrategy.AutoOnSinkSuccess)
            return;

        if (item is IAcknowledgableMessage ack && !ack.IsAcknowledged)
            await ack.AcknowledgeAsync(ct).ConfigureAwait(false);
    }

    private static object GetSerializableBody(T item)
    {
        return item is IAcknowledgableMessage ack
            ? ack.Body
            : item!;
    }

    private ServiceBusMessage CreateOutboundMessage(object body, T originalItem)
    {
        var message = new ServiceBusMessage(BinaryData.FromBytes(
            JsonSerializer.SerializeToUtf8Bytes(body, _serializerOptions)))
        {
            ContentType = "application/json",
        };

        // Support outbound metadata on either the wrapped payload or the original item.
        ApplyPublishMetadata(body, message);
        ApplyPublishMetadata(originalItem, message);
        return message;
    }

    private static void ApplyPublishMetadata(object? source, ServiceBusMessage message)
    {
        if (source is not IServiceBusPublishMetadata metadata)
            return;

        if (!string.IsNullOrWhiteSpace(metadata.MessageId))
            message.MessageId = metadata.MessageId;

        if (!string.IsNullOrWhiteSpace(metadata.CorrelationId))
            message.CorrelationId = metadata.CorrelationId;

        if (!string.IsNullOrWhiteSpace(metadata.SessionId))
            message.SessionId = metadata.SessionId;

        if (!string.IsNullOrWhiteSpace(metadata.PartitionKey))
            message.PartitionKey = metadata.PartitionKey;

        if (!string.IsNullOrWhiteSpace(metadata.Subject))
            message.Subject = metadata.Subject;

        if (!string.IsNullOrWhiteSpace(metadata.ContentType))
            message.ContentType = metadata.ContentType;

        if (metadata.TimeToLive.HasValue)
            message.TimeToLive = metadata.TimeToLive.Value;

        if (metadata.ScheduledEnqueueTimeUtc.HasValue)
            message.ScheduledEnqueueTime = metadata.ScheduledEnqueueTimeUtc.Value;

        if (metadata.ApplicationProperties is { Count: > 0 })
        {
            foreach (var pair in metadata.ApplicationProperties)
            {
                message.ApplicationProperties[pair.Key] = pair.Value;
            }
        }
    }

    private async Task SendMessageWithOptionalTransactionAsync(ServiceBusMessage message, CancellationToken ct)
    {
        if (!_configuration.EnableTransactionalSends)
        {
            await _sender.SendMessageAsync(message, ct).ConfigureAwait(false);
            return;
        }

        using var scope = new TransactionScope(
            TransactionScopeOption.Required,
            TransactionScopeAsyncFlowOption.Enabled);

        await _sender.SendMessageAsync(message, ct).ConfigureAwait(false);
        scope.Complete();
    }

    private async Task SendBatchWithOptionalTransactionAsync(ServiceBusMessageBatch batch, CancellationToken ct)
    {
        if (!_configuration.EnableTransactionalSends)
        {
            await _sender.SendMessagesAsync(batch, ct).ConfigureAwait(false);
            return;
        }

        using var scope = new TransactionScope(
            TransactionScopeOption.Required,
            TransactionScopeAsyncFlowOption.Enabled);

        await _sender.SendMessagesAsync(batch, ct).ConfigureAwait(false);
        scope.Complete();
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await _sender.DisposeAsync().ConfigureAwait(false);

        if (_ownsClient && _ownedClient != null)
            await _ownedClient.DisposeAsync().ConfigureAwait(false);

        await base.DisposeAsync().ConfigureAwait(false);
    }

    private static JsonSerializerOptions CreateSerializerOptions(ServiceBusConfiguration config)
    {
        return config.JsonSerializerOptions ?? new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
    }
}
