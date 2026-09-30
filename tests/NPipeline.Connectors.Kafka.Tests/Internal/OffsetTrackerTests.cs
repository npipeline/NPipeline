using Confluent.Kafka;
using FakeItEasy;
using NPipeline.Connectors.Kafka.Internal;

namespace NPipeline.Connectors.Kafka.Tests.Internal;

/// <summary>
///     Unit tests for <see cref="OffsetTracker" />: a partition's stored offset never passes a message still being handled.
/// </summary>
public sealed class OffsetTrackerTests
{
    private static readonly TopicPartition Partition0 = new("orders", 0);
    private static readonly TopicPartition Partition1 = new("orders", 1);

    private readonly IConsumer<byte[]?, byte[]?> _consumer = A.Fake<IConsumer<byte[]?, byte[]?>>();
    private readonly List<TopicPartitionOffset> _stored = [];
    private readonly OffsetTracker _tracker;

    public OffsetTrackerTests()
    {
        A.CallTo(() => _consumer.StoreOffset(A<TopicPartitionOffset>._)).Invokes((TopicPartitionOffset offset) => _stored.Add(offset));
        _tracker = new OffsetTracker(_consumer);
    }

    [Fact]
    public void AcknowledgingInOrder_StoresTheNextOffsetEachTime()
    {
        Deliver(Partition0, 0, 1, 2);

        _tracker.Settled(Partition0, 0, true);
        _tracker.Settled(Partition0, 1, true);
        _tracker.Settled(Partition0, 2, true);

        Offsets(Partition0).Should().Equal(1, 2, 3);
    }

    [Fact]
    public void AcknowledgingOutOfOrder_StoresOnlyTheSettledPrefix()
    {
        Deliver(Partition0, 10, 11, 12);

        _tracker.Settled(Partition0, 12, true);
        _tracker.Settled(Partition0, 11, true);

        Offsets(Partition0).Should().OnlyContain(o => o <= 10, "offset 10 is still being handled");

        _tracker.Settled(Partition0, 10, true);

        Offsets(Partition0).Last().Should().Be(13, "everything up to 12 is settled, so the next to read is 13");
    }

    [Fact]
    public void Requeue_HoldsCommitsAtTheMessage()
    {
        Deliver(Partition0, 0, 1, 2);

        _tracker.Settled(Partition0, 0, true);
        _tracker.Settled(Partition0, 1, false);
        _tracker.Settled(Partition0, 2, true);

        Offsets(Partition0).Should().Equal(1);
    }

    [Fact]
    public void RejectWithoutRequeue_LetsCommitsMovePast()
    {
        Deliver(Partition0, 0, 1);

        _tracker.Settled(Partition0, 0, true);
        _tracker.Settled(Partition0, 1, true);

        Offsets(Partition0).Should().Equal(1, 2);
    }

    [Fact]
    public void Partitions_AreTrackedSeparately()
    {
        Deliver(Partition0, 0, 1);
        Deliver(Partition1, 5);

        _tracker.Settled(Partition1, 5, true);
        _tracker.Settled(Partition0, 1, true);

        Offsets(Partition1).Should().Equal(6);
        Offsets(Partition0).Should().OnlyContain(o => o == 0, "offset 0 of partition 0 is still being handled");
    }

    [Fact]
    public void AnOffsetIsNeverStoredTwice_OrMovedBack()
    {
        Deliver(Partition0, 0, 1, 2);

        _tracker.Settled(Partition0, 2, true);
        _tracker.Settled(Partition0, 1, true);
        _tracker.Settled(Partition0, 0, true);

        Offsets(Partition0).Should().BeInAscendingOrder().And.OnlyHaveUniqueItems();
        Offsets(Partition0).Last().Should().Be(3);
    }

    [Fact]
    public void RevokedPartition_IsForgotten()
    {
        Deliver(Partition0, 0);

        _tracker.Revoked([new TopicPartitionOffset(Partition0, Offset.Unset)]);
        _tracker.Settled(Partition0, 0, true);

        _stored.Should().BeEmpty("the partition's new owner reads from the last commit");
    }

    [Fact]
    public void StoreRejectedBecauseThePartitionWasRevoked_IsIgnored()
    {
        A.CallTo(() => _consumer.StoreOffset(A<TopicPartitionOffset>._)).Throws(new KafkaException(ErrorCode.Local_State));
        Deliver(Partition0, 0);

        var act = () => _tracker.Settled(Partition0, 0, true);

        act.Should().NotThrow();
    }

    [Fact]
    public void OtherStoreFailures_Surface()
    {
        A.CallTo(() => _consumer.StoreOffset(A<TopicPartitionOffset>._)).Throws(new KafkaException(ErrorCode.Local_Fatal));
        Deliver(Partition0, 0);

        var act = () => _tracker.Settled(Partition0, 0, true);

        act.Should().Throw<KafkaException>();
    }

    private void Deliver(TopicPartition partition, params long[] offsets)
    {
        foreach (var offset in offsets)
        {
            _tracker.Delivered(partition, offset);
        }
    }

    private List<long> Offsets(TopicPartition partition) =>
        _stored.Where(s => s.TopicPartition == partition).Select(s => s.Offset.Value).ToList();
}
