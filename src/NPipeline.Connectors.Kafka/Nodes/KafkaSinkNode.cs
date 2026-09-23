using System.Diagnostics;
using System.Runtime.ExceptionServices;
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
using NPipeline.Connectors.Kafka.Serialization;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Nodes;

/// <summary>
///     Sink node that produces messages to a Kafka topic with support for batching,
///     idempotence, and transactions.
/// </summary>
/// <remarks>
///     The node produces each message once and does not retry. librdkafka retries every produce until
///     <c>delivery.timeout.ms</c>, and the idempotent producer removes the duplicates its retries would cause, so a
///     produce error means librdkafka has already given up or the error is not retriable.
/// </remarks>
/// <typeparam name="T">The type of messages to produce.</typeparam>
public sealed class KafkaSinkNode<T> : SinkNode<T>, IAsyncDisposable
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
    private readonly IPartitionKeyProvider<T> _partitionKeyProvider;
    private readonly IProducer<string, T> _producer;
    private readonly ISerializerProvider _serializer;
    private readonly object _transactionInitLock = new();
    private int? _cachedPartitionCount;
    private ILogger _logger = NullLogger.Instance;
    private Task? _transactionInit;

    /// <summary>
    ///     Creates a new KafkaSinkNode with the specified configuration.
    /// </summary>
    /// <param name="configuration">The Kafka configuration.</param>
    public KafkaSinkNode(KafkaConfiguration configuration)
        : this(configuration, NullKafkaMetrics.Instance)
    {
    }

    /// <summary>
    ///     Creates a new KafkaSinkNode with the specified configuration and metrics.
    /// </summary>
    /// <param name="configuration">The Kafka configuration.</param>
    /// <param name="metrics">The metrics recorder.</param>
    /// <param name="partitionKeyProvider">Optional custom partition key provider.</param>
    public KafkaSinkNode(
        KafkaConfiguration configuration,
        IKafkaMetrics metrics,
        IPartitionKeyProvider<T>? partitionKeyProvider = null)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSink();

        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _partitionKeyProvider = partitionKeyProvider ?? CreateDefaultPartitionKeyProvider();
        _serializer = CreateSerializer(configuration, metrics);

        var producerConfig = BuildProducerConfig(configuration);

        _producer = new ProducerBuilder<string, T>(producerConfig)
            .SetValueSerializer(new KafkaValueSerializer<T>(_serializer))
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
    /// <param name="partitionKeyProvider">Optional custom partition key provider.</param>
    public KafkaSinkNode(
        IProducer<string, T> producer,
        KafkaConfiguration configuration,
        IKafkaMetrics metrics,
        IPartitionKeyProvider<T>? partitionKeyProvider = null)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSink();

        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _partitionKeyProvider = partitionKeyProvider ?? CreateDefaultPartitionKeyProvider();
        _serializer = CreateSerializer(configuration, metrics);
        _ownsProducer = false;
        _batcher = new MessageBatcher(configuration.BatchSize);
    }

    /// <inheritdoc />
    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger = context.Observability.LoggerFactory.CreateLogger(nameof(KafkaSinkNode<T>));

        // Read the partition count once, before producing. Bounded by MetadataTimeoutMs and the pipeline's token.
        await ResolvePartitionCountAsync(cancellationToken).ConfigureAwait(false);

        if (_configuration.EnableTransactions)
        {
            await EnsureTransactionsInitializedAsync(cancellationToken).ConfigureAwait(false);
            await ExecuteTransactionalAsync(input, _logger, cancellationToken).ConfigureAwait(false);
        }
        else if (_configuration.BatchSize > 1)
            await ExecuteBatchedAsync(input, _logger, cancellationToken).ConfigureAwait(false);
        else
            await ExecuteSequentialAsync(input, _logger, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteSequentialAsync(IDataStream<T> input, ILogger logger, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            await ProcessItemAsync(item, logger, cancellationToken).ConfigureAwait(false);
        }

        // Ensure all pending messages are flushed
        Flush(cancellationToken);
    }

    private async Task ExecuteBatchedAsync(IDataStream<T> input, ILogger logger, CancellationToken cancellationToken)
    {
        Task? flushTask = null;

        using var flushCts = _configuration.BatchLingerMs > 0
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;

        if (flushCts != null)
            flushTask = RunBatchFlushLoopAsync(logger, flushCts.Token);

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            // A linger flush that failed has stopped its loop; fail the node now rather than at the end.
            if (flushTask is { IsFaulted: true })
                await flushTask.ConfigureAwait(false);

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

    private async Task ExecuteTransactionalAsync(IDataStream<T> input, ILogger logger, CancellationToken cancellationToken)
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
                if (item is IKafkaOffsetSource offsetSource)
                {
                    offsetsToCommit.Add(offsetSource.TopicPartitionOffset);
                    consumerGroupMetadata ??= offsetSource.ConsumerGroupMetadata;
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
        var ackMessage = item as IAcknowledgableMessage;

        // Produce first, then acknowledge once the broker has the message.
        var sent = await SendMessageAsync(item, ackMessage, logger, cancellationToken).ConfigureAwait(false);

        if (sent && ackMessage != null)
            await AcknowledgeAsync(ackMessage, cancellationToken).ConfigureAwait(false);
    }

    private async Task AcknowledgeAsync(IAcknowledgableMessage message, CancellationToken cancellationToken)
    {
        if (_configuration.AcknowledgmentStrategy == AcknowledgmentStrategy.AutoOnSinkSuccess && !message.IsAcknowledged)
            await message.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);
    }

    private static OutgoingMessage CreateOutgoingMessage(T item)
    {
        return new OutgoingMessage(item, item as IAcknowledgableMessage);
    }

    /// <summary>
    ///     Builds the record for <paramref name="item" />. An acknowledgable message is produced as itself; the value
    ///     serializer writes its <see cref="IAcknowledgableMessage.Body" />, and its metadata becomes headers.
    /// </summary>
    private Message<string, T> CreateMessage(T item, IAcknowledgableMessage? ackMessage)
    {
        var message = new Message<string, T>
        {
            Key = _partitionKeyProvider.GetPartitionKey(item),
            Value = item,
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

        return message;
    }

    private Task<DeliveryResult<string, T>> ProduceAsync(T item, Message<string, T> message, CancellationToken cancellationToken)
    {
        var partition = _partitionKeyProvider.GetPartition(item, _cachedPartitionCount ?? 0);

        // One produce, with no retry above it: librdkafka has already retried anything retriable, and a new produce
        // of the same message is a new record that the idempotent producer cannot deduplicate.
        return partition.HasValue
            ? _producer.ProduceAsync(new TopicPartition(_configuration.SinkTopic, new Partition(partition.Value)), message, cancellationToken)
            : _producer.ProduceAsync(_configuration.SinkTopic, message, cancellationToken);
    }

    private async Task<bool> SendMessageAsync(T item, IAcknowledgableMessage? ackMessage, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            var message = CreateMessage(item, ackMessage);
            var sw = Stopwatch.StartNew();

            _ = await ProduceAsync(item, message, cancellationToken).ConfigureAwait(false);

            sw.Stop();
            _metrics.RecordProduced(_configuration.SinkTopic, 1);
            _metrics.RecordProduceLatency(_configuration.SinkTopic, sw.Elapsed);

            return true;
        }
        catch (KafkaException ex)
        {
            _metrics.RecordProduceError(_configuration.SinkTopic, ex);
            LogProduceFailed(logger, ex);

            if (_configuration.ContinueOnError)
                return false;

            throw;
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
                var sent = await SendMessageAsync(msg.Item, msg.AckMessage, logger, cancellationToken).ConfigureAwait(false);

                if (sent && msg.AckMessage != null)
                    await AcknowledgeAsync(msg.AckMessage, cancellationToken).ConfigureAwait(false);

                return;
            }

            // Batch produce
            var (results, failure) = await ProduceBatchAsync(messages, logger, cancellationToken).ConfigureAwait(false);

            // Acknowledge every message the broker has, even when others in the batch failed: leaving them
            // unacknowledged would redeliver them and produce them again.
            foreach (var (success, ackMessage) in results)
            {
                if (success && ackMessage != null)
                    await AcknowledgeAsync(ackMessage, cancellationToken).ConfigureAwait(false);
            }

            if (failure != null && !_configuration.ContinueOnError)
                failure.Throw();
        }
        finally
        {
            _ = _batchFlushSemaphore.Release();
        }
    }

    /// <summary>
    ///     Produces the batch concurrently and reports, per message, whether the broker has it, plus the first failure.
    ///     Every failure is recorded; whether it fails the node is the caller's decision.
    /// </summary>
    private async Task<(List<(bool Success, IAcknowledgableMessage? AckMessage)> Results, ExceptionDispatchInfo? Failure)> ProduceBatchAsync(
        IReadOnlyList<OutgoingMessage> messages,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var results = new List<(bool Success, IAcknowledgableMessage? AckMessage)>(messages.Count);
        var tasks = new List<Task<DeliveryResult<string, T>>>(messages.Count);
        var taskToMessage = new List<IAcknowledgableMessage?>(messages.Count);
        ExceptionDispatchInfo? failure = null;

        foreach (var msg in messages)
        {
            try
            {
                tasks.Add(ProduceAsync(msg.Item, CreateMessage(msg.Item, msg.AckMessage), cancellationToken));
                taskToMessage.Add(msg.AckMessage);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                LogBatchPrepareFailed(logger, ex);
                _metrics.RecordProduceError(_configuration.SinkTopic, ex);
                failure ??= ExceptionDispatchInfo.Capture(ex);
                results.Add((false, msg.AckMessage));
            }
        }

        try
        {
            _ = await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // Each task is inspected below, so every failure is recorded, not only the first.
        }

        var produced = 0;

        for (var i = 0; i < tasks.Count; i++)
        {
            var task = tasks[i];

            if (task.IsCompletedSuccessfully)
            {
                var persisted = task.Result.Status == PersistenceStatus.Persisted;
                produced += persisted ? 1 : 0;
                results.Add((persisted, taskToMessage[i]));
                continue;
            }

            var error = task.Exception?.InnerException ?? task.Exception ?? (Exception)new TaskCanceledException(task);
            _metrics.RecordProduceError(_configuration.SinkTopic, error);
            failure ??= ExceptionDispatchInfo.Capture(error);
            results.Add((false, taskToMessage[i]));
        }

        if (produced > 0)
            _metrics.RecordProduced(_configuration.SinkTopic, produced);

        if (failure != null)
            LogBatchProduceFailed(logger, failure.SourceException);

        return (results, failure);
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
            MessageTimeoutMs = config.DeliveryTimeoutMs,
            RetryBackoffMs = config.RetryBackoffMs,
            RetryBackoffMaxMs = config.RetryBackoffMaxMs,
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
        // Key by ToString(), of the body for an acknowledgable message
        return new DefaultPartitionKeyProvider<T>(msg => msg is IAcknowledgableMessage acknowledgable
            ? acknowledgable.Body?.ToString() ?? string.Empty
            : msg?.ToString() ?? string.Empty);
    }

    /// <summary>
    ///     Initializes transactions once, bounded by <see cref="KafkaConfiguration.TransactionInitTimeoutMs" />. The
    ///     pipeline's token cancels the wait, so a cluster that does not answer cannot hold up shutdown.
    /// </summary>
    private async Task EnsureTransactionsInitializedAsync(CancellationToken cancellationToken)
    {
        Task init;

        lock (_transactionInitLock)
        {
            // Reuse an initialization still in flight (never start a second one beside it); start again after a failure.
            if (_transactionInit is null || _transactionInit.IsFaulted || _transactionInit.IsCanceled)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // InitTransactions blocks and takes no token, so run it aside and stop waiting when the token fires.
                var timeout = TimeSpan.FromMilliseconds(_configuration.TransactionInitTimeoutMs);
                _transactionInit = Task.Run(() => _producer.InitTransactions(timeout), CancellationToken.None);

                // Observe an initialization abandoned by cancellation, so its eventual failure is not reported as unobserved.
                _ = _transactionInit.ContinueWith(static t => _ = t.Exception, CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }

            init = _transactionInit;
        }

        await init.WaitAsync(cancellationToken).ConfigureAwait(false);
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
    public async ValueTask DisposeAsync()
    {
        // An initialization abandoned by cancellation may still be running on the producer. Let it finish (it is bounded
        // by TransactionInitTimeoutMs) before flushing and disposing, so the producer is never disposed under it.
        if (_transactionInit is { IsCompleted: false } init)
        {
            try
            {
                await init.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Its failure was the node's to report; disposal goes ahead.
            }
        }

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
    }

    /// <summary>
    ///     Reads the sink topic's partition count from the brokers and caches it. The lookup gives up after
    ///     <see cref="KafkaConfiguration.MetadataTimeoutMs" />, and the pipeline's token cancels the wait, so an
    ///     unreachable cluster fails the node promptly instead of blocking it.
    /// </summary>
    private async Task ResolvePartitionCountAsync(CancellationToken cancellationToken)
    {
        if (_cachedPartitionCount.HasValue)
            return;

        // AdminClient.GetMetadata blocks and takes no token, so run it aside and stop waiting when the token fires.
        var lookup = Task.Run(ReadPartitionCount, CancellationToken.None);

        // Observe a lookup abandoned by cancellation, so its eventual failure is not reported as unobserved.
        _ = lookup.ContinueWith(static t => _ = t.Exception, CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        try
        {
            _cachedPartitionCount = await lookup.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (KafkaException ex)
        {
            throw new InvalidOperationException(
                $"Could not read metadata for Kafka topic '{_configuration.SinkTopic}' from '{_configuration.BootstrapServers}' " +
                $"within {_configuration.MetadataTimeoutMs} ms. Check that the brokers are reachable, or raise MetadataTimeoutMs.",
                ex);
        }
    }

    private int ReadPartitionCount()
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _configuration.BootstrapServers,
            SecurityProtocol = _configuration.SecurityProtocol,
            SaslMechanism = _configuration.SaslMechanism,
            SaslUsername = _configuration.SaslUsername,
            SaslPassword = _configuration.SaslPassword,
        }).Build();

        var metadata = adminClient.GetMetadata(_configuration.SinkTopic, TimeSpan.FromMilliseconds(_configuration.MetadataTimeoutMs));

        // A topic that does not exist yet (and may be auto-created) reports no partitions; the partitioner treats
        // zero as unknown.
        return metadata.Topics.FirstOrDefault(t => t.Topic == _configuration.SinkTopic)?.Partitions.Count ?? 0;
    }

    private sealed record OutgoingMessage(T Item, IAcknowledgableMessage? AckMessage);

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
