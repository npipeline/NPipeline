using System.Runtime.CompilerServices;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Internal;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Kafka.Nodes;

/// <summary>
///     Consumes a Kafka topic as a member of a consumer group. Each message is handed on as a <see cref="KafkaMessage{T}" />;
///     acknowledging it stores its offset, in partition order, and the consumer commits stored offsets every
///     <see cref="KafkaReadOptions.CommitInterval" />. When the read ends, the consumer stays until the messages handed on are
///     settled (up to <see cref="KafkaReadOptions.SettleTimeout" />), then commits and leaves the group, so its partitions
///     move to another member at once. Create one with <see cref="KafkaConnector.Source{T}" />.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class KafkaSourceNode<T> : SourceNode<KafkaMessage<T>>, IAsyncDisposable
{
    internal const string ConnectorName = "kafka";

    private static readonly Action<ILogger, double, int, int, Exception?> LogConsumeRetrying =
        LoggerMessage.Define<double, int, int>(LogLevel.Warning, new EventId(1, nameof(LogConsumeRetrying)),
            "Consume failed, retrying in {Delay}ms (attempt {Attempt} of {Attempts})");

    private readonly List<Task> _closing = [];
    private readonly MessageDecoder<T> _decoder;
    private readonly CancellationTokenSource _disposing = new();
    private bool _disposed;
    private readonly KafkaReadOptions _options;

    /// <summary>Creates a source and validates <paramref name="options" />.</summary>
    public KafkaSourceNode(KafkaReadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _decoder = new MessageDecoder<T>(ConnectorName, options.Topic, options.Serializer, options.RowErrorHandler, options.RawExcerptLength);
    }

    /// <summary>Closes the consumers of finished reads now, committing what was acknowledged, without waiting for the rest.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _disposing.CancelAsync().ConfigureAwait(false);

        Task[] closing;

        lock (_closing)
        {
            closing = [.. _closing];
        }

        await Task.WhenAll(closing).ConfigureAwait(false);
        _disposing.Dispose();
    }

    /// <inheritdoc />
    public override IDataStream<KafkaMessage<T>> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        ObjectDisposedException.ThrowIf(_disposed, this);
        return new DataStream<KafkaMessage<T>>(ConsumeAsync(context, cancellationToken), $"KafkaSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<KafkaMessage<T>> ConsumeAsync(PipelineContext context, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var logger = context.Observability.LoggerFactory.CreateLogger(typeof(KafkaSourceNode<T>).FullName ?? nameof(KafkaSourceNode<T>));
        var deadLetters = OpenDeadLetterChannel(context);
        var inFlight = new InFlightMessages();
        OffsetTracker? tracker = null;

        var consumer = new ConsumerBuilder<byte[]?, byte[]?>(ConsumerConfig())
            .SetPartitionsRevokedHandler((_, partitions) => tracker?.Revoked(partitions))
            .Build();

        tracker = new OffsetTracker(consumer);
        var policy = _options.Resilience.WithListener(e => OnResilienceEvent(logger, e));
        Func<IConsumerGroupMetadata> groupMetadata = () => consumer.ConsumerGroupMetadata;
        long sequence = 0;

        try
        {
            consumer.Subscribe(_options.Topic);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var result = await policy.RunAsync(static (state, _) => ValueTask.FromResult(state.Consumer.Consume(state.Timeout)),
                    (Consumer: consumer, Timeout: _options.PollTimeout), cancellationToken).ConfigureAwait(false);

                if (result is null || result.IsPartitionEOF)
                    continue;

                sequence++;
                var position = result.TopicPartitionOffset;
                var partition = result.TopicPartition;
                var offset = result.Offset.Value;
                var value = result.Message.Value;
                tracker.Delivered(partition, offset);

                T body;
                var tombstone = value is null;

                if (tombstone)
                {
                    if (_options.SkipTombstones)
                    {
                        tracker.Settled(partition, offset, true);
                        continue;
                    }

                    body = default!;
                }
                else if (!_decoder.TryDecode(value, out body!, out var error))
                {
                    // Throws for Fail, which fails the read with the offset unstored, so a restart reads the message again.
                    _ = await _decoder.HandleFailureAsync(error, value, $"{position.Topic}/{position.Partition.Value}/{offset}", offset, Metadata(result),
                        deadLetters, cancellationToken).ConfigureAwait(false);

                    tracker.Settled(partition, offset, true);
                    continue;
                }

                inFlight.Add();

                var settlement = new MessageSettlement(
                    _ =>
                    {
                        Settle(tracker, inFlight, partition, offset, true, "acknowledged");
                        return Task.CompletedTask;
                    },
                    (requeue, _) =>
                    {
                        Settle(tracker, inFlight, partition, offset, !requeue, requeue ? "requeued" : "rejected");
                        return Task.CompletedTask;
                    });

                var key = result.Message.Key is { } keyBytes ? Encoding.UTF8.GetString(keyBytes) : null;
                var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(result.Message.Timestamp.UnixTimestampMs);

                ConnectorDiagnostics.RecordRowsRead(ConnectorName, ConnectorName, 1);

                yield return new KafkaMessage<T>(body, position, key, timestamp, result.Message.Headers ?? [], tombstone, settlement, groupMetadata);
            }
        }
        finally
        {
            lock (_closing)
            {
                _closing.Add(CloseWhenSettledAsync(consumer, inFlight));
            }
        }
    }

    private static void Settle(OffsetTracker tracker, InFlightMessages inFlight, TopicPartition partition, long offset, bool advance, string outcome)
    {
        try
        {
            tracker.Settled(partition, offset, advance);
            ConnectorDiagnostics.RecordMessagesSettled(ConnectorName, outcome);
        }
        finally
        {
            inFlight.Remove();
        }
    }

    private async Task CloseWhenSettledAsync(IConsumer<byte[]?, byte[]?> consumer, InFlightMessages inFlight)
    {
        try
        {
            _ = await inFlight.WhenSettledAsync(_options.SettleTimeout, _disposing.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Disposed: close now.
        }

        try
        {
            // Commits the stored offsets and leaves the group, so its partitions are reassigned without waiting for a timeout.
            consumer.Close();
        }
        catch (KafkaException)
        {
            // The group will expire the member instead.
        }
        finally
        {
            consumer.Dispose();
        }
    }

    private ConsumerConfig ConsumerConfig()
    {
        var config = new ConsumerConfig
        {
            GroupId = _options.GroupId,
            GroupInstanceId = _options.GroupInstanceId,
            AutoOffsetReset = _options.AutoOffsetReset,

            // Offsets are stored as messages are acknowledged, and committed in the background from what is stored.
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AutoCommitIntervalMs = (int)Math.Min(_options.CommitInterval.TotalMilliseconds, int.MaxValue),
        };

        if (_options.IsolationLevel is { } isolation)
            config.IsolationLevel = isolation;

        _options.Apply(config);
        return config;
    }

    private void OnResilienceEvent(ILogger logger, CallEvent callEvent)
    {
        if (callEvent.Kind == CallEventKind.Retrying)
            LogConsumeRetrying(logger, (callEvent.Delay ?? TimeSpan.Zero).TotalMilliseconds, callEvent.AttemptNumber, _options.Resilience.Attempts, callEvent.Exception);
    }

    private static Dictionary<string, object> Metadata(ConsumeResult<byte[]?, byte[]?> result)
    {
        var metadata = new Dictionary<string, object>
        {
            ["Topic"] = result.Topic,
            ["Partition"] = result.Partition.Value,
            ["Offset"] = result.Offset.Value,
        };

        if (result.Message.Key is { } key)
            metadata["Key"] = Encoding.UTF8.GetString(key);

        foreach (var header in result.Message.Headers ?? [])
        {
            metadata[$"Header.{header.Key}"] = Encoding.UTF8.GetString(header.GetValueBytes());
        }

        return metadata;
    }
}
