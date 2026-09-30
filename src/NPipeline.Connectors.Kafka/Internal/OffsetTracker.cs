using Confluent.Kafka;

namespace NPipeline.Connectors.Kafka.Internal;

/// <summary>
///     Stores a consumer's offsets as its messages are settled, in order: a partition's stored offset is the lowest offset
///     handed on and not yet settled, or one past the last message handed on, so a commit never passes a message that is
///     still being handled. A message rejected with requeue is never settled here, so commits stop at it and a restart
///     reads it again.
/// </summary>
internal sealed class OffsetTracker(IConsumer<byte[]?, byte[]?> consumer)
{
    private readonly object _lock = new();
    private readonly Dictionary<TopicPartition, PartitionState> _partitions = [];

    /// <summary>Counts a message the consumer returned.</summary>
    public void Delivered(TopicPartition partition, long offset)
    {
        lock (_lock)
        {
            if (!_partitions.TryGetValue(partition, out var state))
                _partitions[partition] = state = new PartitionState();

            _ = state.Pending.Add(offset);
            state.Next = Math.Max(state.Next, offset + 1);
        }
    }

    /// <summary>Settles a message: with <paramref name="advance" /> the offset may be committed past it; without, commits stop at it.</summary>
    public void Settled(TopicPartition partition, long offset, bool advance)
    {
        long store;

        lock (_lock)
        {
            if (!_partitions.TryGetValue(partition, out var state) || !advance)
                return;

            _ = state.Pending.Remove(offset);
            store = state.Pending.Count > 0 ? state.Pending.Min : state.Next;

            if (store <= state.Stored)
                return;

            state.Stored = store;
        }

        try
        {
            // The offset to store is the next one to read.
            consumer.StoreOffset(new TopicPartitionOffset(partition, new Offset(store)));
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_State)
        {
            // The partition was revoked; its new owner reads from the last commit, so the message may be read again.
        }
    }

    /// <summary>Forgets partitions the group took away; their messages still in flight are read again by the new owner.</summary>
    public void Revoked(IEnumerable<TopicPartitionOffset> partitions)
    {
        lock (_lock)
        {
            foreach (var partition in partitions)
            {
                _ = _partitions.Remove(partition.TopicPartition);
            }
        }
    }

    private sealed class PartitionState
    {
        public SortedSet<long> Pending { get; } = [];

        public long Next { get; set; } = -1;

        public long Stored { get; set; } = -1;
    }
}
