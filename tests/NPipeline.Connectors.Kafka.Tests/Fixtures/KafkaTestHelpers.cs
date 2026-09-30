using System.Runtime.CompilerServices;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Connectors.Kafka.Tests.Fixtures;

/// <summary>Topic, stream and verification helpers for tests against the container's broker.</summary>
internal static class KafkaTestHelpers
{
    /// <summary>Creates a topic with a unique name that starts with <paramref name="prefix" />.</summary>
    public static async Task<string> CreateTopicAsync(this KafkaTestContainerFixture fixture, string prefix, int partitions = 1)
    {
        var topic = $"{prefix}-{Guid.NewGuid():N}";
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = fixture.BootstrapServers }).Build();
        await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }]);
        return topic;
    }

    /// <summary>Produces raw values with a plain Confluent producer, so a test can feed the source what no sink would write.</summary>
    public static async Task ProduceRawAsync(this KafkaTestContainerFixture fixture, string topic, params (string? Key, string? Value)[] records)
    {
        using var producer = new ProducerBuilder<string?, string?>(new ProducerConfig { BootstrapServers = fixture.BootstrapServers }).Build();

        foreach (var (key, value) in records)
        {
            _ = await producer.ProduceAsync(topic, new Message<string?, string?> { Key = key, Value = value });
        }
    }

    /// <summary>Reads up to <paramref name="count" /> records from the start of <paramref name="topic" /> with a plain Confluent consumer.</summary>
    public static List<ConsumeResult<string?, string?>> ConsumeRaw(this KafkaTestContainerFixture fixture, string topic, int count, TimeSpan timeout,
        IsolationLevel isolation = IsolationLevel.ReadUncommitted)
    {
        using var consumer = new ConsumerBuilder<string?, string?>(new ConsumerConfig
        {
            BootstrapServers = fixture.BootstrapServers,
            GroupId = $"verify-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            IsolationLevel = isolation,
        }).Build();

        consumer.Subscribe(topic);
        var records = new List<ConsumeResult<string?, string?>>();
        var deadline = DateTime.UtcNow + timeout;

        while (records.Count < count && DateTime.UtcNow < deadline)
        {
            if (consumer.Consume(TimeSpan.FromMilliseconds(250)) is { IsPartitionEOF: false } record)
                records.Add(record);
        }

        consumer.Close();
        return records;
    }

    /// <summary>The group's committed offset for each partition of <paramref name="topic" />, or <c>-1001</c> (unset) where there is none.</summary>
    public static long[] Committed(this KafkaTestContainerFixture fixture, string topic, string groupId, int partitions = 1)
    {
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig { BootstrapServers = fixture.BootstrapServers, GroupId = groupId }).Build();

        return consumer.Committed(Enumerable.Range(0, partitions).Select(p => new TopicPartition(topic, p)), TimeSpan.FromSeconds(10))
            .OrderBy(o => o.Partition.Value)
            .Select(o => o.Offset.Value)
            .ToArray();
    }

    /// <summary>The first <paramref name="count" /> items, or fewer if <paramref name="timeout" /> passes first.</summary>
    public static async Task<List<T>> TakeAsync<T>(this IAsyncEnumerable<T> source, int count, TimeSpan timeout, Func<T, Task>? onItem = null)
    {
        using var cts = new CancellationTokenSource(timeout);
        var items = new List<T>();

        try
        {
            await foreach (var item in source.WithCancellation(cts.Token).ConfigureAwait(false))
            {
                items.Add(item);

                if (onItem is not null)
                    await onItem(item).ConfigureAwait(false);

                if (items.Count >= count)
                    break;
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // The timeout ends the read with what arrived.
        }

        return items;
    }

    /// <summary>A stream that ends after <paramref name="count" /> items, so an endless source can feed a sink that runs to completion.</summary>
    public static IDataStream<TOut> Limit<TIn, TOut>(this IAsyncEnumerable<TIn> source, int count)
        where TIn : TOut =>
        new DataStream<TOut>(LimitAsync<TIn, TOut>(source, count), "limited");

    private static async IAsyncEnumerable<TOut> LimitAsync<TIn, TOut>(IAsyncEnumerable<TIn> source, int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where TIn : TOut
    {
        var taken = 0;

        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;

            // Stop at once: waiting for another item from an endless source would never end.
            if (++taken >= count)
                yield break;
        }
    }
}
