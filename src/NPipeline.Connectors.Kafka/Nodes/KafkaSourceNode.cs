using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Kafka.Serialization;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Kafka.Nodes;

/// <summary>
///     Source node that consumes messages from a Kafka topic with consumer group support.
/// </summary>
/// <typeparam name="T">The type of messages to consume.</typeparam>
public sealed class KafkaSourceNode<T> : SourceNode<KafkaMessage<T>>
{
    private static readonly Action<ILogger, double, int, int, Exception?> LogConsumeRetrying =
        LoggerMessage.Define<double, int, int>(LogLevel.Warning, new EventId(1, nameof(LogConsumeRetrying)),
            "Consume failed, retrying in {Delay}ms (attempt {Attempt} of {Attempts})");

    private readonly KafkaConfiguration _configuration;
    private readonly IConsumer<string, T> _consumer;
    private readonly NResilience.Resilience _consumePolicy;
    private readonly IKafkaMetrics _metrics;
    private readonly bool _ownsConsumer;
    private readonly ISerializerProvider _serializer;
    private ILogger _logger = NullLogger.Instance;

    /// <summary>
    ///     Creates a new KafkaSourceNode with the specified configuration.
    /// </summary>
    /// <param name="configuration">The Kafka configuration.</param>
    public KafkaSourceNode(KafkaConfiguration configuration)
        : this(configuration, NullKafkaMetrics.Instance)
    {
    }

    /// <summary>
    ///     Creates a new KafkaSourceNode with the specified configuration and metrics.
    /// </summary>
    /// <param name="configuration">The Kafka configuration. <see cref="KafkaConfiguration.Resilience" /> sets how a failed consume is retried.</param>
    /// <param name="metrics">The metrics recorder.</param>
    public KafkaSourceNode(
        KafkaConfiguration configuration,
        IKafkaMetrics metrics)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();

        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _consumePolicy = _configuration.Resilience.WithListener(OnResilienceEvent);
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
    public KafkaSourceNode(
        IConsumer<string, T> consumer,
        KafkaConfiguration configuration,
        IKafkaMetrics metrics)
    {
        _consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.ValidateSource();

        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _consumePolicy = _configuration.Resilience.WithListener(OnResilienceEvent);
        _serializer = CreateSerializer(configuration, metrics);
        _ownsConsumer = false;
    }

    /// <inheritdoc />
    public override IDataStream<KafkaMessage<T>> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger = context.Observability.LoggerFactory.CreateLogger(nameof(KafkaSourceNode<T>));

        var stream = ConsumeMessagesAsync(cancellationToken);
        return new DataStream<KafkaMessage<T>>(stream, $"KafkaSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<KafkaMessage<T>> ConsumeMessagesAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Subscribe to the topic
        _consumer.Subscribe(_configuration.SourceTopic);

        var maxPollRecords = _configuration.MaxPollRecords;
        var pollTimeout = TimeSpan.FromMilliseconds(_configuration.PollTimeoutMs);

        while (!cancellationToken.IsCancellationRequested)
        {
            List<KafkaMessage<T>>? messagesToYield = null;
            ExceptionDispatchInfo? failure = null;

            try
            {
                // Batch consume up to MaxPollRecords messages per poll cycle
                var sw = Stopwatch.StartNew();
                messagesToYield = new List<KafkaMessage<T>>(maxPollRecords);

                for (var i = 0; i < maxPollRecords; i++)
                {
                    ConsumeResult<string, T>? consumeResult;

                    try
                    {
                        // Each consume is one call to the policy, so its attempt count applies to that consume: an error
                        // that clears restarts the count, and one that persists surfaces once the attempts are spent.
                        consumeResult = await _consumePolicy.RunAsync(
                            static (state, _) => state.Node.ConsumeOnce(state.PollTimeout),
                            (Node: this, PollTimeout: pollTimeout),
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
                    {
                        // Hand over the messages already consumed in this batch before failing, so none are lost.
                        failure = ExceptionDispatchInfo.Capture(ex);
                        break;
                    }

                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        // No more messages available in this poll cycle
                        break;
                    }

                    // Create KafkaMessage with acknowledgment callback
                    var message = CreateKafkaMessage(consumeResult);
                    messagesToYield.Add(message);
                }

                sw.Stop();
                _metrics.RecordPollLatency(_configuration.SourceTopic, sw.Elapsed);

                if (messagesToYield.Count > 0)
                    _metrics.RecordConsumed(_configuration.SourceTopic, messagesToYield.Count);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Graceful shutdown - commit offsets and close consumer
                await ShutdownAsync().ConfigureAwait(false);
                break;
            }

            // Yield messages outside the try-catch block
            foreach (var message in messagesToYield)
            {
                yield return message;
            }

            if (failure is not null)
            {
                // Leave the consumer group now rather than when the session times out, so the partitions move on.
                try
                {
                    await ShutdownAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Closing a failed consumer can fail too; the consume failure is the one to surface.
                }

                failure.Throw();
            }
        }

        // Final cleanup
        await ShutdownAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     One consume attempt. Every failure is recorded, including one the policy then retries.
    /// </summary>
    private ValueTask<ConsumeResult<string, T>?> ConsumeOnce(TimeSpan pollTimeout)
    {
        try
        {
            return ValueTask.FromResult<ConsumeResult<string, T>?>(_consumer.Consume(pollTimeout));
        }
        catch (Exception ex)
        {
            _metrics.RecordConsumeError(_configuration.SourceTopic, ex);
            throw;
        }
    }

    private void OnResilienceEvent(CallEvent callEvent)
    {
        if (callEvent.Kind != CallEventKind.Retrying)
            return;

        LogConsumeRetrying(
            _logger,
            (callEvent.Delay ?? TimeSpan.Zero).TotalMilliseconds,
            callEvent.AttemptNumber,
            _configuration.Resilience.Attempts,
            callEvent.Exception);
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

            // The real topic and component, so a schema-registry deserializer resolves the right subject.
            return serializer.Deserialize<TValue>(data.ToArray(), context);
        }
    }
}
