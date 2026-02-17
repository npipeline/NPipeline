using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Kafka.Partitioning;
using NPipeline.Connectors.Kafka.Retry;
using NPipeline.Connectors.Kafka.Serialization;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Nodes;

/// <summary>
///     Sink node that produces messages to a Kafka topic with support for batching,
///     idempotence, and transactions.
/// </summary>
/// <typeparam name="T">The type of messages to produce.</typeparam>
public sealed class KafkaSinkNode<T> : SinkNode<T>
{
    // LoggerMessage delegates for performance
    private static readonly Action<ILogger, Exception?> LogTransactionFailed =
        LoggerMessage.Define(LogLevel.Error, new EventId(1, nameof(LogTransactionFailed)),
            "Transaction failed, aborting");

    private static readonly Action<ILogger, Exception?> LogTransactionAbortFailed =
        LoggerMessage.Define(LogLevel.Error, new EventId(2, nameof(LogTransactionAbortFailed)),
            "Failed to abort transaction");

    private static readonly Action<ILogger, Exception?> LogProduceFailed =
        LoggerMessage.Define(LogLevel.Error, new EventId(3, nameof(LogProduceFailed)),
            "Failed to produce message");

    private static readonly Action<ILogger, double, int, Exception?> LogProduceRetrying =
        LoggerMessage.Define<double, int>(LogLevel.Warning, new EventId(4, nameof(LogProduceRetrying)),
            "Produce failed, retrying in {Delay}ms (attempt {Attempt})");

    private static readonly Action<ILogger, Exception?> LogBatchPrepareFailed =
        LoggerMessage.Define(LogLevel.Error, new EventId(5, nameof(LogBatchPrepareFailed)),
            "Failed to prepare message for batch");

    private static readonly Action<ILogger, Exception?> LogBatchProduceFailed =
        LoggerMessage.Define(LogLevel.Error, new EventId(6, nameof(LogBatchProduceFailed)),
            "Batch produce failed");

    private static readonly Action<ILogger, Exception?> LogFlushFailed =
        LoggerMessage.Define(LogLevel.Error, new EventId(7, nameof(LogFlushFailed)),
            "Failed to flush producer");

    private readonly MessageBatcher _batcher;
    private readonly SemaphoreSlim _batchFlushSemaphore = new(1, 1);
    private readonly KafkaConfiguration _configuration;
    private readonly IKafkaMetrics _metrics;
    private readonly bool _ownsProducer;
    private readonly object _partitionCountLock = new();
    private readonly IPartitionKeyProvider<T> _partitionKeyProvider;
    private readonly IProducer<string, T> _producer;
    private readonly IRetryStrategy _retryStrategy;
    private readonly ISerializerProvider _serializer;
    private readonly object _transactionInitLock = new();
    private int? _cachedPartitionCount;
    private ILogger _logger = NullLogger.Instance;
    private bool _transactionsInitialized;

    /// <summary>
    ///     Creates a new KafkaSinkNode with the specified configuration.
    /// </summary>
    /// <param name="configuration">The Kafka configuration.</param>
    public KafkaSinkNode(KafkaConfiguration configuration)
        : this(configuration, NullKafkaMetrics.Instance, new ExponentialBackoffRetryStrategy())
    {
    }

    /// <summary>
    ///     Creates a new KafkaSinkNode with the specified configuration and metrics.
    /// </summary>
    /// <param name="configuration">The Kafka configuration.</param>
    /// <param name="metrics">The metrics recorder.</param>
    /// <param name="retryStrategy">The retry strategy for transient errors.</param>
    /// <param name="partitionKeyProvider">Optional custom partition key provider.</param>
    public KafkaSinkNode(
        KafkaConfiguration configuration,
        IKafkaMetrics metrics,
        IRetryStrategy retryStrategy,
        IPartitionKeyProvider<T>? partitionKeyProvider = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSink();

        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _retryStrategy = retryStrategy ?? throw new ArgumentNullException(nameof(retryStrategy));
        _partitionKeyProvider = partitionKeyProvider ?? CreateDefaultPartitionKeyProvider();
        _serializer = CreateSerializer(configuration, metrics);

        var producerConfig = BuildProducerConfig(configuration);

        _producer = new ProducerBuilder<string, T>(producerConfig)
            .SetValueSerializer(new MessageSerializer<T>(_serializer))
            .Build();

        _ownsProducer = true;
        _batcher = new MessageBatcher(configuration.BatchSize);
    }

    /// <summary>
    ///     Creates a new KafkaSinkNode with a custom producer.
    /// </summary>
    /// <param name="producer">The Kafka producer to use.</param>
    /// <param name="configuration">The Kafka configuration.</param>
    /// <param name="metrics">The metrics recorder.</param>
    /// <param name="retryStrategy">The retry strategy for transient errors.</param>
    /// <param name="partitionKeyProvider">Optional custom partition key provider.</param>
    public KafkaSinkNode(
        IProducer<string, T> producer,
        KafkaConfiguration configuration,
        IKafkaMetrics metrics,
        IRetryStrategy retryStrategy,
        IPartitionKeyProvider<T>? partitionKeyProvider = null)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSink();

        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _retryStrategy = retryStrategy ?? throw new ArgumentNullException(nameof(retryStrategy));
        _partitionKeyProvider = partitionKeyProvider ?? CreateDefaultPartitionKeyProvider();
        _serializer = CreateSerializer(configuration, metrics);
        _ownsProducer = false;
        _batcher = new MessageBatcher(configuration.BatchSize);
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger = context.LoggerFactory.CreateLogger(nameof(KafkaSinkNode<T>));

        // Ensure partition count is cached before processing
        _ = GetPartitionCount();

        if (_configuration.EnableTransactions)
        {
            EnsureTransactionsInitialized(cancellationToken);
            await ExecuteTransactionalAsync(input, _logger, cancellationToken).ConfigureAwait(false);
        }
        else if (_configuration.BatchSize > 1)
            await ExecuteBatchedAsync(input, _logger, cancellationToken).ConfigureAwait(false);
        else
            await ExecuteSequentialAsync(input, _logger, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteSequentialAsync(IDataPipe<T> input, ILogger logger, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            await ProcessItemAsync(item, logger, cancellationToken).ConfigureAwait(false);
        }

        // Ensure all pending messages are flushed
        Flush(cancellationToken);
    }

    private async Task ExecuteBatchedAsync(IDataPipe<T> input, ILogger logger, CancellationToken cancellationToken)
    {
        Task? flushTask = null;

        using var flushCts = _configuration.BatchLingerMs > 0
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;

        if (flushCts != null)
            flushTask = RunBatchFlushLoopAsync(logger, flushCts.Token);

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            var outgoingMessage = CreateOutgoingMessage(item);

            var shouldFlush = _batcher.Add(outgoingMessage);

            if (shouldFlush)
                await FlushBatchAsync(logger, cancellationToken).ConfigureAwait(false);
        }

        if (flushCts != null)
        {
            flushCts.Cancel();

            if (flushTask != null)
            {
                try
                {
                    await flushTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        // Flush remaining messages
        await FlushBatchAsync(logger, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteTransactionalAsync(IDataPipe<T> input, ILogger logger, CancellationToken cancellationToken)
    {
        _producer.BeginTransaction();

        // Collect offsets for exactly-once semantics (SendOffsetsToTransaction)
        var offsetsToCommit = new List<TopicPartitionOffset>();
        var consumerGroupMetadata = default(IConsumerGroupMetadata);

        try
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                // Collect offset and consumer group metadata from KafkaMessage for exactly-once semantics
                if (item is KafkaMessage<T> kafkaMessage)
                {
                    offsetsToCommit.Add(kafkaMessage.TopicPartitionOffset);
                    consumerGroupMetadata ??= kafkaMessage.ConsumerGroupMetadata;
                }

                await ProcessItemAsync(item, logger, cancellationToken).ConfigureAwait(false);
            }

            // Send offsets to transaction for exactly-once semantics
            if (offsetsToCommit.Count > 0 && consumerGroupMetadata != null)
            {
                // Group offsets by topic-partition and take the highest offset per partition
                var consolidatedOffsets = offsetsToCommit
                    .GroupBy(tpo => new { tpo.Topic, tpo.Partition })
                    .Select(g => new TopicPartitionOffset(g.Key.Topic, g.Key.Partition, g.Max(tpo => tpo.Offset)))
                    .ToList();

                _producer.SendOffsetsToTransaction(
                    consolidatedOffsets,
                    consumerGroupMetadata,
                    TimeSpan.FromSeconds(30));
            }

            // Commit the transaction
            var sw = Stopwatch.StartNew();
            _producer.CommitTransaction();
            sw.Stop();
            _metrics.RecordTransactionCommit(sw.Elapsed);
        }
        catch (Exception ex)
        {
            LogTransactionFailed(logger, ex);

            try
            {
                var sw = Stopwatch.StartNew();
                _producer.AbortTransaction();
                sw.Stop();
                _metrics.RecordTransactionAbort(sw.Elapsed);
            }
            catch (Exception abortEx)
            {
                LogTransactionAbortFailed(logger, abortEx);
            }

            throw;
        }
    }

    private async Task ProcessItemAsync(T item, ILogger logger, CancellationToken cancellationToken)
    {
        if (item is IAcknowledgableMessage acknowledgableMessage)
            await ProcessAcknowledgableMessageAsync(acknowledgableMessage, logger, cancellationToken).ConfigureAwait(false);
        else
            _ = await SendMessageAsync(item!, null, logger, cancellationToken).ConfigureAwait(false);
    }

    private async Task ProcessAcknowledgableMessageAsync(IAcknowledgableMessage message, ILogger logger, CancellationToken cancellationToken)
    {
        // Send to sink first, then acknowledge
        var sendSuccess = await SendMessageAsync(message.Body, message, logger, cancellationToken).ConfigureAwait(false);

        if (sendSuccess && _configuration.AcknowledgmentStrategy == AcknowledgmentStrategy.AutoOnSinkSuccess && !message.IsAcknowledged)
            await message.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);
    }

    private static OutgoingMessage CreateOutgoingMessage(T item)
    {
        if (item is IAcknowledgableMessage acknowledgableMessage)
            return new OutgoingMessage(acknowledgableMessage.Body, acknowledgableMessage);

        return new OutgoingMessage(item!, null);
    }

    private async Task<bool> SendMessageAsync(object item, IAcknowledgableMessage? ackMessage, ILogger logger, CancellationToken cancellationToken)
    {
        var attempt = 0;
        var partitionCount = GetPartitionCount();

        while (true)
        {
            try
            {
                var typedItem = (T)item;
                var key = _partitionKeyProvider.GetPartitionKey(typedItem);
                var partition = _partitionKeyProvider.GetPartition(typedItem, partitionCount);

                var message = new Message<string, T>
                {
                    Key = key,
                    Value = typedItem,
                    Timestamp = Timestamp.Default,
                };

                // Add headers if available from metadata
                if (ackMessage?.Metadata != null)
                {
                    message.Headers = [];

                    foreach (var kvp in ackMessage.Metadata)
                    {
                        if (kvp.Value is string stringValue)
                            message.Headers.Add(kvp.Key, Encoding.UTF8.GetBytes(stringValue));
                        else if (kvp.Value is byte[] byteValue)
                            message.Headers.Add(kvp.Key, byteValue);
                    }
                }

                var sw = Stopwatch.StartNew();

                if (partition.HasValue)
                {
                    _ = await _producer.ProduceAsync(
                        new TopicPartition(_configuration.SinkTopic, new Partition(partition.Value)),
                        message,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                    _ = await _producer.ProduceAsync(_configuration.SinkTopic, message, cancellationToken).ConfigureAwait(false);

                sw.Stop();
                _metrics.RecordProduced(_configuration.SinkTopic, 1);
                _metrics.RecordProduceLatency(_configuration.SinkTopic, sw.Elapsed);

                return true;
            }
            catch (ProduceException<string, T> ex)
            {
                attempt++;
                _metrics.RecordProduceError(_configuration.SinkTopic, ex);

                if (!_retryStrategy.ShouldRetry(ex, attempt))
                {
                    LogProduceFailed(logger, ex);

                    if (_configuration.ContinueOnError)
                        return false;

                    throw;
                }

                var delay = _retryStrategy.GetDelay(attempt);
                LogProduceRetrying(logger, delay.TotalMilliseconds, attempt, null);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
            catch (KafkaException ex)
            {
                attempt++;
                _metrics.RecordProduceError(_configuration.SinkTopic, ex);

                if (!_retryStrategy.ShouldRetry(ex, attempt))
                {
                    LogProduceFailed(logger, ex);

                    if (_configuration.ContinueOnError)
                        return false;

                    throw;
                }

                var delay = _retryStrategy.GetDelay(attempt);
                LogProduceRetrying(logger, delay.TotalMilliseconds, attempt, null);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task FlushBatchAsync(ILogger logger, CancellationToken cancellationToken)
    {
        await _batchFlushSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var messages = _batcher.Drain();

            if (messages.Count == 0)
                return;

            _metrics.RecordBatchSize(_configuration.SinkTopic, messages.Count);

            if (messages.Count == 1)
            {
                var msg = messages[0];
                var sent = await SendMessageAsync(msg.Payload, msg.AckMessage, logger, cancellationToken).ConfigureAwait(false);

                if (sent && msg.AckMessage != null && _configuration.AcknowledgmentStrategy == AcknowledgmentStrategy.AutoOnSinkSuccess &&
                    !msg.AckMessage.IsAcknowledged)
                    await msg.AckMessage.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);

                return;
            }

            // Batch produce
            var results = await ProduceBatchAsync(messages, logger, cancellationToken).ConfigureAwait(false);

            // Acknowledge successful sends
            foreach (var (success, ackMessage) in results)
            {
                if (success && ackMessage != null && _configuration.AcknowledgmentStrategy == AcknowledgmentStrategy.AutoOnSinkSuccess &&
                    !ackMessage.IsAcknowledged)
                    await ackMessage.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _ = _batchFlushSemaphore.Release();
        }
    }

    private async Task<IReadOnlyList<(bool Success, IAcknowledgableMessage? AckMessage)>> ProduceBatchAsync(
        IReadOnlyList<OutgoingMessage> messages,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var partitionCount = GetPartitionCount();
        var results = new List<(bool Success, IAcknowledgableMessage? AckMessage)>(messages.Count);
        var tasks = new List<Task<DeliveryResult<string, T>>>(messages.Count);
        var taskToMessage = new List<IAcknowledgableMessage?>(messages.Count);

        foreach (var msg in messages)
        {
            try
            {
                var typedItem = (T)msg.Payload;
                var key = _partitionKeyProvider.GetPartitionKey(typedItem);
                var partition = _partitionKeyProvider.GetPartition(typedItem, partitionCount);

                var message = new Message<string, T>
                {
                    Key = key,
                    Value = typedItem,
                };

                if (partition.HasValue)
                {
                    tasks.Add(_producer.ProduceAsync(
                        new TopicPartition(_configuration.SinkTopic, new Partition(partition.Value)),
                        message,
                        cancellationToken));

                    taskToMessage.Add(msg.AckMessage);
                }
                else
                {
                    tasks.Add(_producer.ProduceAsync(_configuration.SinkTopic, message, cancellationToken));
                    taskToMessage.Add(msg.AckMessage);
                }
            }
            catch (Exception ex)
            {
                LogBatchPrepareFailed(logger, ex);
                results.Add((false, msg.AckMessage));
            }
        }

        try
        {
            var deliveryResults = await Task.WhenAll(tasks).ConfigureAwait(false);
            _metrics.RecordProduced(_configuration.SinkTopic, deliveryResults.Length);

            for (var i = 0; i < deliveryResults.Length; i++)
            {
                var deliveryResult = deliveryResults[i];

                var ackMessage = i < taskToMessage.Count
                    ? taskToMessage[i]
                    : null;

                results.Add((deliveryResult.Status == PersistenceStatus.Persisted, ackMessage));
            }
        }
        catch (Exception ex)
        {
            LogBatchProduceFailed(logger, ex);

            if (ex is AggregateException aggregateException)
            {
                foreach (var innerEx in aggregateException.InnerExceptions)
                {
                    _metrics.RecordProduceError(_configuration.SinkTopic, innerEx);
                }
            }
            else
                _metrics.RecordProduceError(_configuration.SinkTopic, ex);

            for (var i = 0; i < tasks.Count; i++)
            {
                var task = tasks[i];

                var ackMessage = i < taskToMessage.Count
                    ? taskToMessage[i]
                    : null;

                if (task.IsCompletedSuccessfully)
                {
                    var deliveryResult = task.Result;
                    results.Add((deliveryResult.Status == PersistenceStatus.Persisted, ackMessage));
                }
                else
                {
                    if (task.Exception != null)
                        _metrics.RecordProduceError(_configuration.SinkTopic, task.Exception);

                    results.Add((false, ackMessage));
                }
            }
        }

        return results;
    }

    private void Flush(CancellationToken cancellationToken)
    {
        try
        {
            _producer.Flush(cancellationToken);
        }
        catch (Exception ex)
        {
            LogFlushFailed(_logger, ex);
        }
    }

    private static ProducerConfig BuildProducerConfig(KafkaConfiguration config)
    {
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config.BootstrapServers,
            ClientId = config.ClientId,
            EnableIdempotence = config.EnableIdempotence,
            SecurityProtocol = config.SecurityProtocol,
            SaslMechanism = config.SaslMechanism,
            SaslUsername = config.SaslUsername,
            SaslPassword = config.SaslPassword,
            LingerMs = config.LingerMs,
            BatchNumMessages = config.BatchSize,
            MessageMaxBytes = config.MessageMaxBytes,
            CompressionType = config.CompressionType,
            StatisticsIntervalMs = config.StatisticsIntervalMs,
            Acks = config.Acks,
        };

        if (config.EnableTransactions && !string.IsNullOrWhiteSpace(config.TransactionalId))
            producerConfig.TransactionalId = config.TransactionalId;

        return producerConfig;
    }

    private static ISerializerProvider CreateSerializer(KafkaConfiguration config, IKafkaMetrics metrics)
    {
        return config.SerializationFormat switch
        {
            SerializationFormat.Json => new JsonMessageSerializer(metrics),
            SerializationFormat.Avro => config.SchemaRegistry != null
                ? new AvroMessageSerializer(config.SchemaRegistry, metrics)
                : throw new InvalidOperationException(
                    "SchemaRegistry configuration is required for Avro serialization."),
            SerializationFormat.Protobuf => config.SchemaRegistry != null
                ? new ProtobufMessageSerializer(config.SchemaRegistry, metrics)
                : throw new InvalidOperationException(
                    "SchemaRegistry configuration is required for Protobuf serialization."),
            _ => new JsonMessageSerializer(metrics),
        };
    }

    private static IPartitionKeyProvider<T> CreateDefaultPartitionKeyProvider()
    {
        // Create a default partition key provider that uses ToString()
        return new DefaultPartitionKeyProvider<T>(msg => msg?.ToString() ?? string.Empty);
    }

    private void EnsureTransactionsInitialized(CancellationToken cancellationToken)
    {
        if (_transactionsInitialized)
            return;

        cancellationToken.ThrowIfCancellationRequested();

        lock (_transactionInitLock)
        {
            if (_transactionsInitialized)
                return;

            _producer.InitTransactions(TimeSpan.FromMilliseconds(_configuration.TransactionInitTimeoutMs));
            _transactionsInitialized = true;
        }
    }

    private async Task RunBatchFlushLoopAsync(ILogger logger, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await Task.Delay(_configuration.BatchLingerMs, cancellationToken).ConfigureAwait(false);
            await FlushBatchAsync(logger, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        try
        {
            _producer.Flush(CancellationToken.None);
        }
        catch (Exception ex)
        {
            LogFlushFailed(_logger, ex);
        }

        if (_ownsProducer)
            _producer.Dispose();

        _batcher.Dispose();
        _batchFlushSemaphore.Dispose();

        await base.DisposeAsync().ConfigureAwait(false);
    }

    private int GetPartitionCount()
    {
        if (_cachedPartitionCount.HasValue)
            return _cachedPartitionCount.Value;

        lock (_partitionCountLock)
        {
            if (_cachedPartitionCount.HasValue)
                return _cachedPartitionCount.Value;

            try
            {
                // Use admin client to get partition count from Kafka metadata
                using var adminClient = new AdminClientBuilder(new AdminClientConfig
                {
                    BootstrapServers = _configuration.BootstrapServers,
                    SecurityProtocol = _configuration.SecurityProtocol,
                    SaslMechanism = _configuration.SaslMechanism,
                    SaslUsername = _configuration.SaslUsername,
                    SaslPassword = _configuration.SaslPassword,
                }).Build();

                var metadata = adminClient.GetMetadata(_configuration.SinkTopic, TimeSpan.FromSeconds(30));
                var topicMetadata = metadata.Topics.FirstOrDefault(t => t.Topic == _configuration.SinkTopic);

                if (topicMetadata != null)
                {
                    _cachedPartitionCount = topicMetadata.Partitions.Count;
                    return _cachedPartitionCount.Value;
                }
            }
            catch (Exception)
            {
                // If metadata fetch fails, return 0 to indicate unknown partition count
                // The partitioner should handle this gracefully
            }

            return 0;
        }
    }

    private sealed record OutgoingMessage(object Payload, IAcknowledgableMessage? AckMessage);

    /// <summary>
    ///     Serializer that uses the ISerializerProvider.
    /// </summary>
    private sealed class MessageSerializer<TValue>(ISerializerProvider serializer) : ISerializer<TValue>
    {
        private readonly ISerializerProvider _serializer = serializer;

        public byte[] Serialize(TValue data, SerializationContext context)
        {
            return data is null
                ? []
                : _serializer.Serialize(data);
        }
    }

    /// <summary>
    ///     Helper class for batching outgoing messages.
    /// </summary>
    private sealed class MessageBatcher : IDisposable
    {
        private readonly List<OutgoingMessage> _batch;
        private readonly int _batchSize;
        private readonly object _lock = new();
        private bool _disposed;

        public MessageBatcher(int batchSize)
        {
            _batchSize = batchSize > 0
                ? batchSize
                : 1;

            _batch = new List<OutgoingMessage>(_batchSize);
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
        }

        public bool Add(OutgoingMessage message)
        {
            lock (_lock)
            {
                _batch.Add(message);
                return _batch.Count >= _batchSize;
            }
        }

        public IReadOnlyList<OutgoingMessage> Drain()
        {
            lock (_lock)
            {
                if (_batch.Count == 0)
                    return [];

                var result = new List<OutgoingMessage>(_batch);
                _batch.Clear();
                return result;
            }
        }
    }
}
