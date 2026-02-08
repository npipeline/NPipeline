using System.Text.Json;
using Amazon;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Amazon.SQS.Model;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Models;
using NPipeline.Connectors.Configuration;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Nodes;

/// <summary>
///     Sink node that publishes messages to an SQS queue with automatic acknowledgment support.
///     Supports individual and batch acknowledgment strategies.
/// </summary>
/// <typeparam name="T">Type to serialize to JSON.</typeparam>
public sealed class SqsSinkNode<T> : SinkNode<T>
{
    private readonly AcknowledgmentStrategy _acknowledgmentStrategy;
    private readonly AcknowledgmentBatcher _batcher;
    private readonly BatchAcknowledgmentOptions _batchOptions;

    private readonly SqsConfiguration _configuration;
    private readonly List<Task> _delayedAcknowledgmentTasks = [];
    private readonly JsonSerializerOptions _serializerOptions;
    private readonly IAmazonSQS _sqsClient;
    private IPipelineLogger _logger = NullPipelineLogger.Instance;

    /// <summary>
    ///     Creates a new SqsSinkNode with the specified configuration.
    /// </summary>
    public SqsSinkNode(SqsConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSink();

        _sqsClient = CreateSqsClient(configuration);
        _serializerOptions = CreateSerializerOptions(configuration);
        _acknowledgmentStrategy = configuration.AcknowledgmentStrategy;
        _batchOptions = configuration.BatchAcknowledgment ?? new BatchAcknowledgmentOptions();
        _batcher = new AcknowledgmentBatcher(_batchOptions, _sqsClient, configuration.SourceQueueUrl, NullPipelineLogger.Instance);
    }

    /// <summary>
    ///     Creates a new SqsSinkNode with a custom SQS client.
    /// </summary>
    public SqsSinkNode(IAmazonSQS sqsClient, SqsConfiguration configuration)
    {
        _sqsClient = sqsClient ?? throw new ArgumentNullException(nameof(sqsClient));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSink();

        _serializerOptions = CreateSerializerOptions(configuration);
        _acknowledgmentStrategy = configuration.AcknowledgmentStrategy;
        _batchOptions = configuration.BatchAcknowledgment ?? new BatchAcknowledgmentOptions();
        _batcher = new AcknowledgmentBatcher(_batchOptions, _sqsClient, configuration.SourceQueueUrl, NullPipelineLogger.Instance);
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(SqsSinkNode<T>));
        _logger = logger;
        _batcher.SetLogger(logger);

        if (_configuration.EnableParallelProcessing && _configuration.MaxDegreeOfParallelism > 1)
            await ExecuteParallelAsync(input, logger, cancellationToken).ConfigureAwait(false);
        else
            await ExecuteSequentialAsync(input, logger, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteSequentialAsync(IDataPipe<T> input, IPipelineLogger logger, CancellationToken cancellationToken)
    {
        if (_configuration.BatchSize <= 1)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                await ProcessItemAsync(item, logger, cancellationToken).ConfigureAwait(false);
            }

            // Flush any remaining batched acknowledgments
            await _batcher.FlushAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        var batch = new List<OutgoingMessage>(_configuration.BatchSize);

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            batch.Add(CreateOutgoingMessage(item));

            if (batch.Count >= _configuration.BatchSize)
            {
                await SendBatchAndAcknowledgeAsync(batch, logger, cancellationToken).ConfigureAwait(false);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await SendBatchAndAcknowledgeAsync(batch, logger, cancellationToken).ConfigureAwait(false);

        // Flush any remaining batched acknowledgments
        await _batcher.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteParallelAsync(IDataPipe<T> input, IPipelineLogger logger, CancellationToken cancellationToken)
    {
        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = _configuration.MaxDegreeOfParallelism,
            CancellationToken = cancellationToken,
        };

        await Parallel.ForEachAsync(input, options, async (item, ct) => { await ProcessItemAsync(item, logger, ct).ConfigureAwait(false); })
            .ConfigureAwait(false);

        // Flush any remaining batched acknowledgments
        await _batcher.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static OutgoingMessage CreateOutgoingMessage(T item)
    {
        if (item is IAcknowledgableMessage acknowledgableMessage)
            return new OutgoingMessage(acknowledgableMessage.Body, acknowledgableMessage);

        return new OutgoingMessage(item!, null);
    }

    private async Task SendBatchAndAcknowledgeAsync(List<OutgoingMessage> batch, IPipelineLogger logger, CancellationToken cancellationToken)
    {
        if (batch.Count == 1)
        {
            var outgoing = batch[0];
            var sent = await SendMessageAsync(outgoing.Payload, logger, cancellationToken).ConfigureAwait(false);

            if (sent && outgoing.AckMessage != null)
                await HandleAcknowledgmentAsync(outgoing.AckMessage, cancellationToken).ConfigureAwait(false);

            return;
        }

        var ackMessages = await SendMessageBatchAsync(batch, logger, cancellationToken).ConfigureAwait(false);

        foreach (var ackMessage in ackMessages)
        {
            await HandleAcknowledgmentAsync(ackMessage, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessItemAsync(T item, IPipelineLogger logger, CancellationToken cancellationToken)
    {
        // Check if this is an acknowledgable message
        if (item is IAcknowledgableMessage acknowledgableMessage)
        {
            logger.Log(LogLevel.Debug, "Processing IAcknowledgableMessage, MessageType={MessageType}", item?.GetType().Name ?? "null");
            await ProcessAcknowledgableMessageAsync(acknowledgableMessage, logger, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Regular message, just send to sink queue
            logger.Log(LogLevel.Debug, "Processing regular message, MessageType={MessageType}", item?.GetType().Name ?? "null");
            await SendMessageAsync(item!, logger, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessAcknowledgableMessageAsync(IAcknowledgableMessage message, IPipelineLogger logger, CancellationToken cancellationToken)
    {
        switch (_acknowledgmentStrategy)
        {
            case AcknowledgmentStrategy.AutoOnSinkSuccess:
                // Send to sink first, then acknowledge
                // Use the original payload, not the wrapper, to avoid serialization issues
                var sendSuccess = await SendMessageAsync(message.Body, logger, cancellationToken).ConfigureAwait(false);

                if (sendSuccess)
                    await AcknowledgeMessageAsync(message, cancellationToken).ConfigureAwait(false);

                break;

            case AcknowledgmentStrategy.Manual:
                // Send to sink, but don't acknowledge (user must do it manually)
                await SendMessageAsync(message.Body, logger, cancellationToken).ConfigureAwait(false);
                break;

            case AcknowledgmentStrategy.Delayed:
                // Send to sink, then acknowledge after delay
                sendSuccess = await SendMessageAsync(message.Body, logger, cancellationToken).ConfigureAwait(false);

                if (sendSuccess)
                {
                    var delayedTask = Task.Run(async () =>
                    {
                        try
                        {
                            await Task.Delay(_configuration.AcknowledgmentDelayMs, cancellationToken).ConfigureAwait(false);
                            await AcknowledgeMessageAsync(message, cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            // Expected on cancellation
                        }
                    }, cancellationToken);

                    TrackDelayedAcknowledgmentTask(delayedTask);
                }

                break;

            case AcknowledgmentStrategy.None:
                // Send to sink, but don't acknowledge
                await SendMessageAsync(message.Body, logger, cancellationToken).ConfigureAwait(false);
                break;
        }
    }

    private async Task HandleAcknowledgmentAsync(IAcknowledgableMessage message, CancellationToken cancellationToken)
    {
        switch (_acknowledgmentStrategy)
        {
            case AcknowledgmentStrategy.AutoOnSinkSuccess:
                await AcknowledgeMessageAsync(message, cancellationToken).ConfigureAwait(false);
                break;
            case AcknowledgmentStrategy.Manual:
                break;
            case AcknowledgmentStrategy.Delayed:
                var delayedTask = Task.Run(async () =>
                {
                    try
                    {
                        await Task.Delay(_configuration.AcknowledgmentDelayMs, cancellationToken).ConfigureAwait(false);
                        await AcknowledgeMessageAsync(message, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected on cancellation
                    }
                }, cancellationToken);

                TrackDelayedAcknowledgmentTask(delayedTask);

                break;
            case AcknowledgmentStrategy.None:
                break;
        }
    }

    private async Task AcknowledgeMessageAsync(IAcknowledgableMessage message, CancellationToken cancellationToken)
    {
        if (message.IsAcknowledged)
            return;

        if (_batchOptions.EnableAutomaticBatching)
        {
            // Add to batch for later acknowledgment
            await _batcher.AddAsync(message, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Acknowledge immediately
            await message.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<bool> SendMessageAsync(object item, IPipelineLogger logger, CancellationToken cancellationToken)
    {
        try
        {
            logger.Log(LogLevel.Debug, "Sending message, ItemType={ItemType}", item.GetType().Name);
            var jsonBody = JsonSerializer.Serialize(item, _serializerOptions);

            var request = new SendMessageRequest
            {
                QueueUrl = _configuration.SinkQueueUrl,
                MessageBody = jsonBody,
                DelaySeconds = _configuration.DelaySeconds,
            };

            // Add message attributes if configured
            if (_configuration.MessageAttributes != null)
            {
                request.MessageAttributes ??= new Dictionary<string, MessageAttributeValue>();

                foreach (var attr in _configuration.MessageAttributes)
                {
                    request.MessageAttributes[attr.Key] = attr.Value;
                }
            }

            await _sqsClient.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);
            logger.Log(LogLevel.Debug, "Message sent successfully");
            return true;
        }
        catch (Exception ex) when (_configuration.ContinueOnError)
        {
            logger.Log(LogLevel.Warning, ex, "Failed to send message to SQS. Continuing due to ContinueOnError setting.");
            return false;
        }
    }

    private async Task<IReadOnlyList<IAcknowledgableMessage>> SendMessageBatchAsync(
        IReadOnlyList<OutgoingMessage> items,
        IPipelineLogger logger,
        CancellationToken cancellationToken)
    {
        var entries = new List<SendMessageBatchRequestEntry>(items.Count);
        var entryAckMap = new Dictionary<string, IAcknowledgableMessage>(items.Count);

        for (var i = 0; i < items.Count; i++)
        {
            try
            {
                var jsonBody = JsonSerializer.Serialize(items[i].Payload, _serializerOptions);
                var entryId = Guid.NewGuid().ToString();

                var entry = new SendMessageBatchRequestEntry
                {
                    Id = entryId,
                    MessageBody = jsonBody,
                    DelaySeconds = _configuration.DelaySeconds,
                };

                // Add message attributes if configured
                if (_configuration.MessageAttributes != null)
                {
                    entry.MessageAttributes ??= new Dictionary<string, MessageAttributeValue>();

                    foreach (var attr in _configuration.MessageAttributes)
                    {
                        entry.MessageAttributes[attr.Key] = attr.Value;
                    }
                }

                entries.Add(entry);

                if (items[i].AckMessage != null)
                    entryAckMap[entryId] = items[i].AckMessage!;
            }
            catch (Exception ex) when (_configuration.ContinueOnError)
            {
                logger.Log(LogLevel.Warning, ex, "Failed to serialize message for batch. Skipping.");
            }
        }

        if (entries.Count == 0)
            return [];

        var request = new SendMessageBatchRequest
        {
            QueueUrl = _configuration.SinkQueueUrl,
            Entries = entries,
        };

        try
        {
            var response = await _sqsClient.SendMessageBatchAsync(request, cancellationToken).ConfigureAwait(false);

            var failedIds = response.Failed?.Select(f => f.Id).ToHashSet() ?? [];

            if (failedIds.Count > 0)
            {
                foreach (var failed in response.Failed!)
                {
                    logger.Log(LogLevel.Warning, "Failed to send message {Id} to SQS: {Message}", failed.Id, failed.Message);
                }
            }

            var successfulAckMessages = entryAckMap
                .Where(kvp => !failedIds.Contains(kvp.Key))
                .Select(kvp => kvp.Value)
                .ToList();

            return successfulAckMessages;
        }
        catch (AmazonSQSException ex) when (_configuration.ContinueOnError)
        {
            logger.Log(LogLevel.Warning, ex, "Failed to send message batch to SQS. Continuing due to ContinueOnError setting.");
            return [];
        }
    }

    private static IAmazonSQS CreateSqsClient(SqsConfiguration configuration)
    {
        var config = new AmazonSQSConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(configuration.Region),
        };

        if (!string.IsNullOrWhiteSpace(configuration.AccessKeyId) &&
            !string.IsNullOrWhiteSpace(configuration.SecretAccessKey))
        {
            return new AmazonSQSClient(
                configuration.AccessKeyId,
                configuration.SecretAccessKey,
                config);
        }

        if (!string.IsNullOrWhiteSpace(configuration.ProfileName))
        {
            var chain = new CredentialProfileStoreChain();

            if (chain.TryGetProfile(configuration.ProfileName, out var profile))
                return new AmazonSQSClient(profile.GetAWSCredentials(chain), config);
        }

        // Use default credential chain
        return new AmazonSQSClient(config);
    }

    private static JsonSerializerOptions CreateSerializerOptions(SqsConfiguration configuration)
    {
        var options = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = configuration.PropertyNameCaseInsensitive,
            PropertyNamingPolicy = configuration.PropertyNamingPolicy switch
            {
                JsonPropertyNamingPolicy.CamelCase => JsonNamingPolicy.CamelCase,
                JsonPropertyNamingPolicy.SnakeCase => JsonNamingPolicy.SnakeCaseLower,
                JsonPropertyNamingPolicy.LowerCase => new LowerCaseNamingPolicy(),
                JsonPropertyNamingPolicy.PascalCase => new PascalCaseNamingPolicy(),
                JsonPropertyNamingPolicy.AsIs => null,
                _ => JsonNamingPolicy.CamelCase,
            },
        };

        return options;
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await _batcher.DisposeAsync().ConfigureAwait(false);
        List<Task> delayedTasks;

        lock (_delayedAcknowledgmentTasks)
        {
            delayedTasks = _delayedAcknowledgmentTasks.ToList();
        }

        try
        {
            await Task.WhenAll(delayedTasks).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.Log(LogLevel.Warning, ex, "One or more delayed acknowledgment tasks failed during disposal");
        }
        finally
        {
            lock (_delayedAcknowledgmentTasks)
            {
                _delayedAcknowledgmentTasks.RemoveAll(task => task.IsCompleted);
            }
        }

        await base.DisposeAsync().ConfigureAwait(false);
    }

    private void TrackDelayedAcknowledgmentTask(Task delayedTask)
    {
        lock (_delayedAcknowledgmentTasks)
        {
            _delayedAcknowledgmentTasks.Add(delayedTask);
            _delayedAcknowledgmentTasks.RemoveAll(task => task.IsCompleted);
        }
    }

    private sealed record OutgoingMessage(object Payload, IAcknowledgableMessage? AckMessage);

    private sealed class LowerCaseNamingPolicy : JsonNamingPolicy
    {
        public static readonly LowerCaseNamingPolicy Instance = new();

        public override string ConvertName(string name)
        {
            if (string.IsNullOrEmpty(name))
                return name;

            return name.ToLowerInvariant();
        }
    }

    private sealed class PascalCaseNamingPolicy : JsonNamingPolicy
    {
        public static readonly PascalCaseNamingPolicy Instance = new();

        public override string ConvertName(string name)
        {
            if (string.IsNullOrEmpty(name))
                return name;

            return char.ToUpperInvariant(name[0]) + name[1..];
        }
    }
}

/// <summary>
///     Handles batch acknowledgment of messages to improve performance.
/// </summary>
internal sealed class AcknowledgmentBatcher : IDisposable, IAsyncDisposable
{
    private readonly List<IAcknowledgableMessageWrapper> _batch;
    private readonly Timer _flushTimer;
    private readonly object _lock = new();
    private readonly BatchAcknowledgmentOptions _options;
    private readonly string _queueUrl;
    private readonly SemaphoreSlim _semaphore;
    private readonly IAmazonSQS _sqsClient;
    private bool _disposed;
    private IPipelineLogger _logger;

    public AcknowledgmentBatcher(
        BatchAcknowledgmentOptions options,
        IAmazonSQS sqsClient,
        string queueUrl,
        IPipelineLogger? logger = null)
    {
        _options = options;
        _sqsClient = sqsClient;
        _queueUrl = queueUrl;
        _logger = logger ?? NullPipelineLogger.Instance;
        _semaphore = new SemaphoreSlim(options.MaxConcurrentBatches, options.MaxConcurrentBatches);
        _batch = new List<IAcknowledgableMessageWrapper>(options.BatchSize);
        _flushTimer = new Timer(FlushCallback, null, options.FlushTimeoutMs, options.FlushTimeoutMs);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        try
        {
            _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
            await FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.Log(LogLevel.Error, ex, "Error flushing acknowledgment batch on dispose");
        }
        finally
        {
            _flushTimer.Dispose();
        }

        _semaphore.Dispose();
        _disposed = true;
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        try
        {
            _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
        }
        catch (ObjectDisposedException)
        {
            // Ignore if already disposed
        }
        finally
        {
            _flushTimer.Dispose();
            _semaphore.Dispose();
            _disposed = true;
        }
    }

    public void SetLogger(IPipelineLogger logger)
    {
        _logger = logger ?? NullPipelineLogger.Instance;
    }

    public async Task AddAsync(IAcknowledgableMessage message, CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        bool shouldFlush;

        try
        {
            lock (_lock)
            {
                _batch.Add(new AcknowledgableMessageWrapper(message));
                shouldFlush = _batch.Count >= _options.BatchSize;
            }
        }
        finally
        {
            _semaphore.Release();
        }

        if (shouldFlush)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await FlushBatchAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private void FlushCallback(object? state)
    {
        if (_disposed)
            return;

        _ = Task.Run(async () =>
        {
            try
            {
                if (_disposed)
                    return;

                await FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Log(LogLevel.Error, ex, "Error flushing acknowledgment batch");
            }
        });
    }

    private async Task FlushBatchAsync(CancellationToken cancellationToken)
    {
        List<IAcknowledgableMessageWrapper> messagesToAck;

        lock (_lock)
        {
            if (_batch.Count == 0)
                return;

            messagesToAck = _batch.ToList();
            _batch.Clear();
        }

        if (messagesToAck.Count == 1)
        {
            // Single message - acknowledge directly
            await messagesToAck[0].AcknowledgeAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            // Batch acknowledge
            await AcknowledgeBatchAsync(messagesToAck, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task AcknowledgeBatchAsync(IReadOnlyList<IAcknowledgableMessageWrapper> messages, CancellationToken cancellationToken)
    {
        var entries = new List<DeleteMessageBatchRequestEntry>();
        var entryLookup = new Dictionary<string, IAcknowledgableMessageWrapper>();

        foreach (var message in messages)
        {
            if (message.Metadata.TryGetValue("ReceiptHandle", out var receiptHandle) && receiptHandle is string handle)
            {
                var entry = new DeleteMessageBatchRequestEntry
                {
                    Id = message.MessageId,
                    ReceiptHandle = handle,
                };

                entries.Add(entry);
                entryLookup[entry.Id] = message;
            }
        }

        if (entries.Count == 0)
            return;

        var request = new DeleteMessageBatchRequest
        {
            QueueUrl = _queueUrl,
            Entries = entries,
        };

        var response = await _sqsClient.DeleteMessageBatchAsync(request, cancellationToken).ConfigureAwait(false);

        var failedIds = response.Failed?.Select(f => f.Id).ToHashSet() ?? [];

        foreach (var entry in entries)
        {
            if (!failedIds.Contains(entry.Id) && entryLookup.TryGetValue(entry.Id, out var wrapper))
                wrapper.MarkAcknowledged();
        }

        // Handle failed deletions
        if (response.Failed?.Count > 0)
        {
            foreach (var failed in response.Failed)
            {
                _logger.Log(LogLevel.Warning, "Failed to delete message {MessageId}: {ErrorMessage}", failed.Id, failed.Message);
            }
        }
    }

    private interface IAcknowledgableMessageWrapper
    {
        string MessageId { get; }
        IReadOnlyDictionary<string, object> Metadata { get; }
        Task AcknowledgeAsync(CancellationToken cancellationToken = default);
        void MarkAcknowledged();
    }

    private sealed class AcknowledgableMessageWrapper(IAcknowledgableMessage inner) : IAcknowledgableMessageWrapper
    {
        private readonly IAcknowledgableMessage _inner = inner;

        public string MessageId => _inner.MessageId;
        public IReadOnlyDictionary<string, object> Metadata => _inner.Metadata;

        public Task AcknowledgeAsync(CancellationToken cancellationToken = default)
        {
            return _inner.AcknowledgeAsync(cancellationToken);
        }

        public void MarkAcknowledged()
        {
            if (_inner is IAwsSqsAcknowledgableMessage awsMessage)
                awsMessage.MarkAcknowledged();
        }
    }
}
