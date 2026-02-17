using System.Diagnostics;
using System.Runtime.CompilerServices;
using Confluent.Kafka;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Kafka.Retry;
using NPipeline.Connectors.Kafka.Serialization;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Nodes;

/// <summary>
///     Source node that consumes messages from a Kafka topic with consumer group support.
/// </summary>
/// <typeparam name="T">The type of messages to consume.</typeparam>
public sealed class KafkaSourceNode<T> : SourceNode<KafkaMessage<T>>
{
    private readonly KafkaConfiguration _configuration;
    private readonly IConsumer<string, T> _consumer;
    private readonly IKafkaMetrics _metrics;
    private readonly bool _ownsConsumer;
    private readonly IRetryStrategy _retryStrategy;
    private readonly ISerializerProvider _serializer;

    /// <summary>
    ///     Creates a new KafkaSourceNode with the specified configuration.
    /// </summary>
    /// <param name="configuration">The Kafka configuration.</param>
    public KafkaSourceNode(KafkaConfiguration configuration)
        : this(configuration, NullKafkaMetrics.Instance, new ExponentialBackoffRetryStrategy())
    {
    }

    /// <summary>
    ///     Creates a new KafkaSourceNode with the specified configuration and metrics.
    /// </summary>
    /// <param name="configuration">The Kafka configuration.</param>
    /// <param name="metrics">The metrics recorder.</param>
    /// <param name="retryStrategy">The retry strategy for transient errors.</param>
    public KafkaSourceNode(
        KafkaConfiguration configuration,
        IKafkaMetrics metrics,
        IRetryStrategy retryStrategy)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();

        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _retryStrategy = retryStrategy ?? throw new ArgumentNullException(nameof(retryStrategy));
        _serializer = CreateSerializer(configuration, metrics);

        var consumerConfig = BuildConsumerConfig(configuration);

        _consumer = new ConsumerBuilder<string, T>(consumerConfig)
            .SetValueDeserializer(new MessageDeserializer<T>(_serializer))
            .Build();

        _ownsConsumer = true;
    }

    /// <summary>
    ///     Creates a new KafkaSourceNode with a custom consumer.
    /// </summary>
    /// <param name="consumer">The Kafka consumer to use.</param>
    /// <param name="configuration">The Kafka configuration.</param>
    /// <param name="metrics">The metrics recorder.</param>
    /// <param name="retryStrategy">The retry strategy for transient errors.</param>
    public KafkaSourceNode(
        IConsumer<string, T> consumer,
        KafkaConfiguration configuration,
        IKafkaMetrics metrics,
        IRetryStrategy retryStrategy)
    {
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();

        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _retryStrategy = retryStrategy ?? throw new ArgumentNullException(nameof(retryStrategy));
        _serializer = CreateSerializer(configuration, metrics);
        _ownsConsumer = false;
    }

    /// <inheritdoc />
    public override IDataPipe<KafkaMessage<T>> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var stream = ConsumeMessagesAsync(cancellationToken);
        return new StreamingDataPipe<KafkaMessage<T>>(stream, $"KafkaSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<KafkaMessage<T>> ConsumeMessagesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Subscribe to the topic
        _consumer.Subscribe(_configuration.SourceTopic);

        var attempt = 0;
        var maxPollRecords = _configuration.MaxPollRecords;
        var pollTimeout = TimeSpan.FromMilliseconds(_configuration.PollTimeoutMs);

        while (!cancellationToken.IsCancellationRequested)
        {
            List<KafkaMessage<T>>? messagesToYield = null;

            try
            {
                // Batch consume up to MaxPollRecords messages per poll cycle
                var sw = Stopwatch.StartNew();
                var batchStartTime = sw.ElapsedMilliseconds;
                messagesToYield = new List<KafkaMessage<T>>(maxPollRecords);

                for (var i = 0; i < maxPollRecords; i++)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(pollTimeout);

                        if (consumeResult == null || consumeResult.IsPartitionEOF)
                        {
                            // No more messages available in this poll cycle
                            break;
                        }

                        // Create KafkaMessage with acknowledgment callback
                        var message = CreateKafkaMessage(consumeResult);
                        messagesToYield.Add(message);
                    }
                    catch (ConsumeException ex)
                    {
                        // Handle consume errors within the batch
                        attempt++;

                        if (!_retryStrategy.ShouldRetry(ex, attempt))
                        {
                            _metrics.RecordCommitError(_configuration.SourceTopic, ex);
                            throw;
                        }

                        var delay = _retryStrategy.GetDelay(attempt);
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                        break; // Exit batch on error
                    }
                }

                sw.Stop();
                _metrics.RecordPollLatency(_configuration.SourceTopic, sw.Elapsed);

                if (messagesToYield.Count > 0)
                    _metrics.RecordConsumed(_configuration.SourceTopic, messagesToYield.Count);

                attempt = 0; // Reset attempt counter on successful batch
            }
            catch (OperationCanceledException)
            {
                // Graceful shutdown - commit offsets and close consumer
                await ShutdownAsync().ConfigureAwait(false);
                break;
            }
            catch (KafkaException ex)
            {
                attempt++;

                if (!_retryStrategy.ShouldRetry(ex, attempt))
                {
                    _metrics.RecordCommitError(_configuration.SourceTopic, ex);
                    throw;
                }

                var delay = _retryStrategy.GetDelay(attempt);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                continue;
            }

            // Yield messages outside the try-catch block
            foreach (var message in messagesToYield)
            {
                yield return message;
            }
        }

        // Final cleanup
        await ShutdownAsync().ConfigureAwait(false);
    }

    private KafkaMessage<T> CreateKafkaMessage(ConsumeResult<string, T> consumeResult)
    {
        var timestamp = consumeResult.Message.Timestamp.UnixTimestampMs > 0
            ? DateTimeOffset.FromUnixTimeMilliseconds(consumeResult.Message.Timestamp.UnixTimestampMs).UtcDateTime
            : DateTime.UtcNow;

        // For exactly-once semantics, offsets are committed via SendOffsetsToTransaction in the sink
        // For at-least-once semantics, offsets are committed directly via the consumer
        var topicPartitionOffset = consumeResult.TopicPartitionOffset;

        Func<CancellationToken, Task>? acknowledgeCallback = null;
        IConsumerGroupMetadata? consumerGroupMetadata = null;

        if (_configuration.DeliverySemantic == DeliverySemantic.AtLeastOnce)
        {
            acknowledgeCallback = ct =>
            {
                var sw = Stopwatch.StartNew();

                try
                {
                    _consumer.Commit([topicPartitionOffset]);
                    sw.Stop();
                    _metrics.RecordCommitLatency(_configuration.SourceTopic, sw.Elapsed);
                    return Task.CompletedTask;
                }
                catch (Exception ex)
                {
                    sw.Stop();
                    _metrics.RecordCommitError(_configuration.SourceTopic, ex);
                    throw;
                }
            };
        }
        else if (_configuration.DeliverySemantic == DeliverySemantic.ExactlyOnce)
        {
            // Get consumer group metadata for SendOffsetsToTransaction
            consumerGroupMetadata = _consumer.ConsumerGroupMetadata;
        }

        return new KafkaMessage<T>(
            consumeResult.Message.Value,
            consumeResult.Topic,
            consumeResult.Partition,
            consumeResult.Offset,
            consumeResult.Message.Key ?? string.Empty,
            timestamp,
            consumeResult.Message.Headers ?? [],
            acknowledgeCallback,
            consumerGroupMetadata);
    }

    private Task ShutdownAsync()
    {
        try
        {
            if (_ownsConsumer && _configuration.EnableAutoCommit)
            {
                // Only commit on shutdown when auto-commit is enabled to avoid
                // acknowledging messages that haven't been explicitly processed.
                _consumer.Commit();
            }
        }
        catch (Exception ex)
        {
            _metrics.RecordCommitError(_configuration.SourceTopic, ex);
        }
        finally
        {
            if (_ownsConsumer)
            {
                _consumer.Close();
                _consumer.Dispose();
            }
        }

        return Task.CompletedTask;
    }

    private static ConsumerConfig BuildConsumerConfig(KafkaConfiguration config)
    {
        return new ConsumerConfig
        {
            BootstrapServers = config.BootstrapServers,
            ClientId = config.ClientId,
            GroupId = config.ConsumerGroupId,
            GroupInstanceId = config.GroupInstanceId,
            AutoOffsetReset = config.AutoOffsetReset,
            EnableAutoCommit = config.EnableAutoCommit,
            EnableAutoOffsetStore = config.EnableAutoOffsetStore,
            FetchMinBytes = config.FetchMinBytes,
            FetchMaxBytes = config.FetchMaxBytes,
            MaxPartitionFetchBytes = config.MaxPartitionFetchBytes,
            SecurityProtocol = config.SecurityProtocol,
            SaslMechanism = config.SaslMechanism,
            SaslUsername = config.SaslUsername,
            SaslPassword = config.SaslPassword,
            IsolationLevel = config.IsolationLevel,
            StatisticsIntervalMs = config.StatisticsIntervalMs,
        };
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

    /// <summary>
    ///     Deserializer that uses the ISerializerProvider.
    /// </summary>
    private sealed class MessageDeserializer<TValue>(ISerializerProvider serializer) : IDeserializer<TValue>
    {
        public TValue Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.IsEmpty)
                return default!;

            return serializer.Deserialize<TValue>(data.ToArray());
        }
    }
}
