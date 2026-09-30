using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Models;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Kafka.Nodes;

/// <summary>
///     Produces to a Kafka topic. Messages go in batches of <see cref="KafkaWriteOptions{T}.BatchSize" />, whose deliveries
///     are awaited together; written through <c>Acknowledging()</c>, each received message is acknowledged once the broker
///     has its body. With a <see cref="KafkaWriteOptions{T}.TransactionalId" />, each batch is one transaction that also
///     commits the offsets of the Kafka messages it came from, so reading and writing are exactly-once. Create one with
///     <see cref="KafkaConnector.Sink{T}" />.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class KafkaSinkNode<T> : SinkNode<T>, IMessageSink<T>, IAsyncDisposable
{
    private static readonly Action<ILogger, string, Exception?> LogProduceFailed =
        LoggerMessage.Define<string>(LogLevel.Error, new EventId(3, nameof(LogProduceFailed)), "Failed to produce a message to {Topic}");

    private static readonly Action<ILogger, string, Exception?> LogTransactionAborted =
        LoggerMessage.Define<string>(LogLevel.Error, new EventId(1, nameof(LogTransactionAborted)), "Transaction on {Topic} failed and was aborted");

    private readonly KafkaWriteOptions<T> _options;
    private readonly IProducer<byte[]?, byte[]> _producer;
    private readonly object _initLock = new();
    private Task? _initialized;
    private ILogger _logger = Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance;

    /// <summary>Creates a sink and validates <paramref name="options" />.</summary>
    public KafkaSinkNode(KafkaWriteOptions<T> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _producer = new ProducerBuilder<byte[]?, byte[]>(ProducerConfig()).Build();
    }

    /// <summary>Waits for messages still being delivered, then disposes the producer.</summary>
    public ValueTask DisposeAsync()
    {
        try
        {
            _ = _producer.Flush(TimeSpan.FromSeconds(10));
        }
        catch (KafkaException)
        {
            // Undelivered messages were reported to the write that produced them.
        }

        _producer.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public override Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken) =>
        WriteAsync(input, static item => item, static _ => null, context, cancellationToken);

    /// <inheritdoc />
    public Task ConsumeMessagesAsync(IDataStream<IAcknowledgableMessage<T>> input, PipelineContext context, CancellationToken cancellationToken) =>
        WriteAsync(input, static message => message.Body, static message => message, context, cancellationToken);

    private async Task WriteAsync<TItem>(IDataStream<TItem> input, Func<TItem, T> body, Func<TItem, IAcknowledgableMessage?> source,
        PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);

        _logger = context.Observability.LoggerFactory.CreateLogger(typeof(KafkaSinkNode<T>).FullName ?? nameof(KafkaSinkNode<T>));
        var deadLetters = OpenDeadLetterChannel(context);

        if (_options.TransactionalId is not null)
            await InitializeTransactionsAsync(cancellationToken).ConfigureAwait(false);

        (Outgoing[] Batch, Task<DeliveryResult<byte[]?, byte[]>>[] Deliveries)? previous = null;

        await foreach (var batch in input.BatchAsync(_options.BatchSize, _options.BatchLinger, cancellationToken).ConfigureAwait(false))
        {
            var outgoing = new Outgoing[batch.Count];
            var i = 0;

            foreach (var item in batch)
            {
                var value = body(item);
                var from = source(item);
                outgoing[i++] = new Outgoing(value, from, Message(value, from));
            }

            if (_options.TransactionalId is not null)
            {
                await WriteTransactionAsync(outgoing, cancellationToken).ConfigureAwait(false);
                continue;
            }

            // This batch is produced while the previous one's deliveries are awaited and settled, so batches overlap on the
            // wire and settlement still follows input order.
            var deliveries = Produce(outgoing, cancellationToken);

            if (previous is { } earlier)
                await SettleBatchAsync(earlier.Batch, earlier.Deliveries, deadLetters, cancellationToken).ConfigureAwait(false);

            previous = (outgoing, deliveries);
        }

        if (previous is { } last)
            await SettleBatchAsync(last.Batch, last.Deliveries, deadLetters, cancellationToken).ConfigureAwait(false);
    }

    private async Task SettleBatchAsync(Outgoing[] batch, Task<DeliveryResult<byte[]?, byte[]>>[] deliveries, DeadLetterChannel deadLetters,
        CancellationToken cancellationToken)
    {
        var errors = await DeliveredAsync(deliveries, cancellationToken).ConfigureAwait(false);
        var produced = 0;
        var acknowledgements = new List<Task>();

        // Settling a Kafka message stores its offset only once every earlier one of its partition is settled, so
        // acknowledging the messages after a failure never commits past it.
        for (var i = 0; i < batch.Length; i++)
        {
            if (errors[i] is not null)
                continue;

            produced++;

            if (batch[i].Source is { } from)
                acknowledgements.Add(from.AcknowledgeAsync(cancellationToken));
        }

        // Settled together: a broker that settles each message with its own request (Service Bus) takes one round trip, not one per message.
        await Task.WhenAll(acknowledgements).ConfigureAwait(false);
        ConnectorDiagnostics.RecordRowsWritten(KafkaSourceNode<T>.ConnectorName, KafkaSourceNode<T>.ConnectorName, produced);

        for (var i = 0; i < batch.Length; i++)
        {
            if (errors[i] is not { } error)
                continue;

            LogProduceFailed(_logger, _options.Topic, error);

            switch (_options.FailedMessages)
            {
                case FailedMessageAction.Requeue:
                    // A plain body has no source message to requeue, so its failure fails the write.
                    if (batch[i].Source is not { } requeue)
                        throw error;

                    await requeue.RejectAsync(true, cancellationToken).ConfigureAwait(false);
                    break;
                case FailedMessageAction.DeadLetter:
                    await deadLetters.SendAsync(batch[i].Body!, error, cancellationToken).ConfigureAwait(false);

                    if (batch[i].Source is { } handled)
                        await handled.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);

                    break;
                default:
                    throw error;
            }
        }
    }

    private async Task WriteTransactionAsync(Outgoing[] batch, CancellationToken cancellationToken)
    {
        _producer.BeginTransaction();

        try
        {
            var errors = await DeliveredAsync(Produce(batch, cancellationToken), cancellationToken).ConfigureAwait(false);

            if (Array.Find(errors, e => e is not null) is { } error)
                throw error;

            // The next offset to read for each partition the batch's Kafka messages came from, committed with the batch.
            var received = batch.Select(o => o.Source).OfType<IKafkaReceived>().ToList();

            if (received.Count > 0 && received[0].GroupMetadata() is { } group)
            {
                var offsets = received
                    .GroupBy(r => r.Position.TopicPartition)
                    .Select(g => new TopicPartitionOffset(g.Key, new Offset(g.Max(r => r.Position.Offset.Value) + 1)))
                    .ToList();

                _producer.SendOffsetsToTransaction(offsets, group, _options.TransactionTimeout);
            }

            _producer.CommitTransaction(_options.TransactionTimeout);
        }
        catch (Exception ex)
        {
            LogTransactionAborted(_logger, _options.Topic, ex);

            try
            {
                _producer.AbortTransaction(_options.TransactionTimeout);
            }
            catch (KafkaException)
            {
                // A producer that cannot abort is fenced or broken; the original failure is the one to report.
            }

            throw;
        }

        ConnectorDiagnostics.RecordRowsWritten(KafkaSourceNode<T>.ConnectorName, KafkaSourceNode<T>.ConnectorName, batch.Length);

        foreach (var outgoing in batch)
        {
            if (outgoing.Source is { } from)
                await from.AcknowledgeAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Starts producing the batch; librdkafka batches and sends the messages.</summary>
    private Task<DeliveryResult<byte[]?, byte[]>>[] Produce(Outgoing[] batch, CancellationToken cancellationToken)
    {
        var deliveries = new Task<DeliveryResult<byte[]?, byte[]>>[batch.Length];

        for (var i = 0; i < batch.Length; i++)
        {
            deliveries[i] = _producer.ProduceAsync(_options.Topic, batch[i].Message, cancellationToken);
        }

        return deliveries;
    }

    /// <summary>Waits for every delivery; returns each message's failure, or <c>null</c>.</summary>
    private static async Task<Exception?[]> DeliveredAsync(Task<DeliveryResult<byte[]?, byte[]>>[] deliveries, CancellationToken cancellationToken)
    {
        try
        {
            _ = await Task.WhenAll(deliveries).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // Each delivery is inspected below.
        }

        var errors = new Exception?[deliveries.Length];

        for (var i = 0; i < deliveries.Length; i++)
        {
            var delivery = deliveries[i];

            errors[i] = delivery.IsCompletedSuccessfully
                ? delivery.Result.Status == PersistenceStatus.Persisted
                    ? null
                    : new KafkaException(new Error(ErrorCode.Local_MsgTimedOut, $"The broker may not have the message (status {delivery.Result.Status})."))
                : delivery.Exception?.InnerException ?? new OperationCanceledException(cancellationToken);
        }

        return errors;
    }

    private Message<byte[]?, byte[]> Message(T value, IAcknowledgableMessage? source)
    {
        var received = source as IKafkaReceived;
        var key = _options.KeySelector is { } selector ? selector(value) : received?.Key;

        var message = new Message<byte[]?, byte[]>
        {
            Key = key is null ? null : Encoding.UTF8.GetBytes(key),
            Value = _options.Serializer.Serialize(value, new MessageContext(_options.Topic)),
        };

        if (_options.CopyHeaders && received is not null)
        {
            message.Headers = [];

            foreach (var header in received.Headers)
            {
                message.Headers.Add(header.Key, header.GetValueBytes());
            }
        }

        return message;
    }

    private Task InitializeTransactionsAsync(CancellationToken cancellationToken)
    {
        lock (_initLock)
        {
            // InitTransactions blocks and takes no token, so it runs aside; a failed one is started again next time.
            if (_initialized is null || _initialized.IsFaulted || _initialized.IsCanceled)
                _initialized = Task.Run(() => _producer.InitTransactions(_options.TransactionTimeout), CancellationToken.None);

            return _initialized.WaitAsync(cancellationToken);
        }
    }

    private ProducerConfig ProducerConfig()
    {
        var config = new ProducerConfig
        {
            Acks = _options.Acks,
            EnableIdempotence = _options.EnableIdempotence,
            LingerMs = _options.Linger.TotalMilliseconds,
            MessageTimeoutMs = (int)Math.Min(_options.DeliveryTimeout.TotalMilliseconds, int.MaxValue),
            TransactionalId = _options.TransactionalId,
        };

        if (_options.Compression is { } compression)
            config.CompressionType = compression;

        if (_options.TransactionalId is not null)
        {
            config.TransactionTimeoutMs = (int)Math.Min(_options.TransactionTimeout.TotalMilliseconds * 2, int.MaxValue);

            // librdkafka requires a message to time out within its transaction.
            config.MessageTimeoutMs = Math.Min(config.MessageTimeoutMs ?? int.MaxValue, config.TransactionTimeoutMs.Value);
        }

        _options.Apply(config);
        return config;
    }

    private sealed record Outgoing(T Body, IAcknowledgableMessage? Source, Message<byte[]?, byte[]> Message);
}
