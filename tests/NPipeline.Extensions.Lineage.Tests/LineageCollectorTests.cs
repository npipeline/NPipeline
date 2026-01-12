using FluentAssertions;
using NPipeline.Configuration;
using NPipeline.Lineage;
using Xunit;

namespace NPipeline.Extensions.Lineage.Tests;

public class LineageCollectorTests
{
    [Fact]
    public void CreateLineagePacket_WithValidItemAndSourceNodeId_ShouldCreatePacketWithUniqueGuid()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var sourceNodeId = "source1";

        // Act
        var packet1 = collector.CreateLineagePacket(item, sourceNodeId);
        var packet2 = collector.CreateLineagePacket(item, sourceNodeId);

        // Assert
        packet1.Should().NotBeNull();
        packet2.Should().NotBeNull();
        packet1.LineageId.Should().NotBeEmpty();
        packet2.LineageId.Should().NotBeEmpty();
        packet1.LineageId.Should().NotBe(packet2.LineageId);
        packet1.Data.Should().Be(item);
        packet2.Data.Should().Be(item);
        packet1.TraversalPath.Should().HaveCount(1);
        packet1.TraversalPath[0].Should().Be(sourceNodeId);
    }

    [Fact]
    public void CreateLineagePacket_WithNullSourceNodeId_ShouldThrowArgumentNullException()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => collector.CreateLineagePacket(item, null!));
    }

    [Fact]
    public void CreateLineagePacket_WithDifferentDataTypes_ShouldCreatePackets()
    {
        // Arrange
        var collector = new LineageCollector();

        // Act
        var stringPacket = collector.CreateLineagePacket("string", "source1");
        var intPacket = collector.CreateLineagePacket(42, "source2");
        var objectPacket = collector.CreateLineagePacket(new TestData { Id = 1, Name = "Test" }, "source3");

        // Assert
        stringPacket.Data.Should().Be("string");
        intPacket.Data.Should().Be(42);
        objectPacket.Data.Should().BeOfType<TestData>();
        ((TestData)objectPacket.Data!).Id.Should().Be(1);
    }

    [Fact]
    public void RecordHop_WithValidLineageIdAndHop_ShouldRecordHop()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");
        var hop = new LineageHop(
            "node1",
            HopDecisionFlags.Emitted,
            ObservedCardinality.One,
            1,
            1,
            null,
            false);

        // Act
        collector.RecordHop(packet.LineageId, hop);
        var lineageInfo = collector.GetLineageInfo(packet.LineageId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageHops.Should().HaveCount(1);
        lineageInfo.LineageHops[0].NodeId.Should().Be("node1");
        lineageInfo.LineageHops[0].Outcome.Should().Be(HopDecisionFlags.Emitted);
        lineageInfo.TraversalPath.Should().HaveCount(2);
        lineageInfo.TraversalPath.Should().Contain("node1");
    }

    [Fact]
    public void RecordHop_WithMultipleHops_ShouldRecordAllHops()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");
        var hop1 = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);
        var hop2 = new LineageHop("node2", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);
        var hop3 = new LineageHop("node3", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);

        // Act
        collector.RecordHop(packet.LineageId, hop1);
        collector.RecordHop(packet.LineageId, hop2);
        collector.RecordHop(packet.LineageId, hop3);
        var lineageInfo = collector.GetLineageInfo(packet.LineageId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageHops.Should().HaveCount(3);
        lineageInfo.TraversalPath.Should().HaveCount(4);
        lineageInfo.TraversalPath.Should().ContainInOrder("source1", "node1", "node2", "node3");
    }

    [Fact]
    public void RecordHop_WithDuplicateNodeIds_ShouldNotDuplicateInTraversalPath()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");
        var hop1 = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);
        var hop2 = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);

        // Act
        collector.RecordHop(packet.LineageId, hop1);
        collector.RecordHop(packet.LineageId, hop2);
        var lineageInfo = collector.GetLineageInfo(packet.LineageId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.TraversalPath.Should().HaveCount(2);
        lineageInfo.TraversalPath.Should().ContainInOrder("source1", "node1");
        lineageInfo.LineageHops.Should().HaveCount(2);
    }

    [Fact]
    public void RecordHop_WithUnknownLineageId_ShouldNotThrow()
    {
        // Arrange
        var collector = new LineageCollector();
        var unknownLineageId = Guid.NewGuid();
        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);

        // Act & Assert
        var exception = Record.Exception(() => collector.RecordHop(unknownLineageId, hop));
        exception.Should().BeNull();
    }

    [Theory]
    [InlineData(HopDecisionFlags.Emitted)]
    [InlineData(HopDecisionFlags.FilteredOut)]
    [InlineData(HopDecisionFlags.Joined)]
    [InlineData(HopDecisionFlags.Aggregated)]
    [InlineData(HopDecisionFlags.Retried)]
    [InlineData(HopDecisionFlags.Error)]
    [InlineData(HopDecisionFlags.DeadLettered)]
    [InlineData(HopDecisionFlags.Emitted | HopDecisionFlags.Retried)]
    public void RecordHop_WithVariousOutcomes_ShouldRecordCorrectly(HopDecisionFlags outcome)
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");
        var hop = new LineageHop("node1", outcome, ObservedCardinality.One, 1, 1, null, false);

        // Act
        collector.RecordHop(packet.LineageId, hop);
        var lineageInfo = collector.GetLineageInfo(packet.LineageId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageHops[0].Outcome.Should().Be(outcome);
    }

    [Fact]
    public void ShouldCollectLineage_WithNullOptions_ShouldReturnTrue()
    {
        // Arrange
        var collector = new LineageCollector();
        var lineageId = Guid.NewGuid();

        // Act
        var result = collector.ShouldCollectLineage(lineageId, null);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void ShouldCollectLineage_WithSampleEveryOne_ShouldReturnTrue()
    {
        // Arrange
        var collector = new LineageCollector();
        var lineageId = Guid.NewGuid();
        var options = new LineageOptions(SampleEvery: 1);

        // Act
        var result = collector.ShouldCollectLineage(lineageId, options);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void ShouldCollectLineage_WithDeterministicSampling_ShouldBeDeterministic()
    {
        // Arrange
        var collector = new LineageCollector();
        var lineageId = Guid.NewGuid();
        var options = new LineageOptions(SampleEvery: 10, DeterministicSampling: true);

        // Act
        var result1 = collector.ShouldCollectLineage(lineageId, options);
        var result2 = collector.ShouldCollectLineage(lineageId, options);
        var result3 = collector.ShouldCollectLineage(lineageId, options);

        // Assert
        result1.Should().Be(result2);
        result2.Should().Be(result3);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(100)]
    public void ShouldCollectLineage_WithDeterministicSampling_ShouldSampleCorrectly(int sampleEvery)
    {
        // Arrange
        var collector = new LineageCollector();
        var options = new LineageOptions(SampleEvery: sampleEvery, DeterministicSampling: true);
        var sampledCount = 0;
        var totalItems = sampleEvery * 10;

        // Act
        for (var i = 0; i < totalItems; i++)
        {
            var lineageId = Guid.NewGuid();
            if (collector.ShouldCollectLineage(lineageId, options))
            {
                sampledCount++;
            }
        }

        // Assert
        sampledCount.Should().BeGreaterThan(0);
        sampledCount.Should().BeLessThan(totalItems);
        // Approximately 1/sampleEvery items should be sampled
        var expectedSamples = totalItems / sampleEvery;
        var tolerance = totalItems / sampleEvery;
        sampledCount.Should().BeInRange(expectedSamples - tolerance, expectedSamples + tolerance);
    }

    [Fact]
    public void ShouldCollectLineage_WithNonDeterministicSampling_ShouldBeRandom()
    {
        // Arrange
        var collector = new LineageCollector();
        var options = new LineageOptions(SampleEvery: 10, DeterministicSampling: false);
        var results = new List<bool>();

        // Act
        for (var i = 0; i < 100; i++)
        {
            var lineageId = Guid.NewGuid();
            results.Add(collector.ShouldCollectLineage(lineageId, options));
        }

        // Assert
        results.Should().Contain(true);
        results.Should().Contain(false);
    }

    [Fact]
    public void GetLineageInfo_WithValidLineageId_ShouldReturnLineageInfo()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");
        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);
        collector.RecordHop(packet.LineageId, hop);

        // Act
        var lineageInfo = collector.GetLineageInfo(packet.LineageId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageId.Should().Be(packet.LineageId);
        lineageInfo.Data.Should().Be(item);
        lineageInfo.TraversalPath.Should().HaveCount(2);
        lineageInfo.LineageHops.Should().HaveCount(1);
    }

    [Fact]
    public void GetLineageInfo_WithUnknownLineageId_ShouldReturnNull()
    {
        // Arrange
        var collector = new LineageCollector();
        var unknownLineageId = Guid.NewGuid();

        // Act
        var lineageInfo = collector.GetLineageInfo(unknownLineageId);

        // Assert
        lineageInfo.Should().BeNull();
    }

    [Fact]
    public void GetAllLineageInfo_WithMultipleItems_ShouldReturnAllLineageInfo()
    {
        // Arrange
        var collector = new LineageCollector();
        var packet1 = collector.CreateLineagePacket("item1", "source1");
        var packet2 = collector.CreateLineagePacket("item2", "source1");
        var packet3 = collector.CreateLineagePacket("item3", "source1");
        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);

        collector.RecordHop(packet1.LineageId, hop);
        collector.RecordHop(packet2.LineageId, hop);
        collector.RecordHop(packet3.LineageId, hop);

        // Act
        var allLineageInfo = collector.GetAllLineageInfo();

        // Assert
        allLineageInfo.Should().HaveCount(3);
        allLineageInfo.Should().Contain(info => info.LineageId == packet1.LineageId);
        allLineageInfo.Should().Contain(info => info.LineageId == packet2.LineageId);
        allLineageInfo.Should().Contain(info => info.LineageId == packet3.LineageId);
    }

    [Fact]
    public void GetAllLineageInfo_WithNoItems_ShouldReturnEmptyList()
    {
        // Arrange
        var collector = new LineageCollector();

        // Act
        var allLineageInfo = collector.GetAllLineageInfo();

        // Assert
        allLineageInfo.Should().BeEmpty();
    }

    [Fact]
    public void Clear_ShouldRemoveAllLineageInfo()
    {
        // Arrange
        var collector = new LineageCollector();
        var packet1 = collector.CreateLineagePacket("item1", "source1");
        var packet2 = collector.CreateLineagePacket("item2", "source1");
        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);
        collector.RecordHop(packet1.LineageId, hop);
        collector.RecordHop(packet2.LineageId, hop);

        // Act
        collector.Clear();
        var allLineageInfo = collector.GetAllLineageInfo();

        // Assert
        allLineageInfo.Should().BeEmpty();
        collector.GetLineageInfo(packet1.LineageId).Should().BeNull();
        collector.GetLineageInfo(packet2.LineageId).Should().BeNull();
    }

    [Fact]
    public async Task ConcurrentAccess_ShouldBeThreadSafe()
    {
        // Arrange
        var collector = new LineageCollector();
        var tasks = new List<Task>();
        var itemCount = 100;
        var threads = 10;

        // Act
        for (var t = 0; t < threads; t++)
        {
            var threadId = t;
            var task = Task.Run(() =>
            {
                for (var i = 0; i < itemCount; i++)
                {
                    var item = $"item-{threadId}-{i}";
                    var packet = collector.CreateLineagePacket(item, $"source-{threadId}");
                    var hop = new LineageHop(
                        $"node-{threadId}",
                        HopDecisionFlags.Emitted,
                        ObservedCardinality.One,
                        1,
                        1,
                        null,
                        false);
                    collector.RecordHop(packet.LineageId, hop);
                }
            });
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);

        // Assert
        var allLineageInfo = collector.GetAllLineageInfo();
        allLineageInfo.Should().HaveCount(threads * itemCount);
    }

    [Fact]
    public async Task ConcurrentRecordHop_ShouldNotLoseHops()
    {
        // Arrange
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test", "source1");
        var hopCount = 100;
        var threads = 10;

        // Act
        var tasks = Enumerable.Range(0, threads).Select(threadId =>
            Task.Run(() =>
            {
                for (var i = 0; i < hopCount; i++)
                {
                    var hop = new LineageHop(
                        $"node-{threadId}-{i}",
                        HopDecisionFlags.Emitted,
                        ObservedCardinality.One,
                        1,
                        1,
                        null,
                        false);
                    collector.RecordHop(packet.LineageId, hop);
                }
            })
        );

        await Task.WhenAll(tasks);

        // Assert
        var lineageInfo = collector.GetLineageInfo(packet.LineageId);
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageHops.Should().HaveCount(threads * hopCount);
    }

    [Fact]
    public void CreateLineagePacket_WithNullItem_ShouldCreatePacket()
    {
        // Arrange
        var collector = new LineageCollector();
        string? item = null;

        // Act
        var packet = collector.CreateLineagePacket(item!, "source1");

        // Assert
        packet.Should().NotBeNull();
        packet.Data.Should().BeNull();
        packet.LineageId.Should().NotBeEmpty();
    }

    [Fact]
    public void GetLineageInfo_AfterMultipleOperations_ShouldReturnCorrectData()
    {
        // Arrange
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test", "source1");
        var hop1 = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false);
        var hop2 = new LineageHop("node2", HopDecisionFlags.FilteredOut, ObservedCardinality.Zero, 1, 0, null, false);
        var hop3 = new LineageHop("node3", HopDecisionFlags.Joined, ObservedCardinality.Many, 2, 3, new[] { 0, 1 }, false);

        // Act
        collector.RecordHop(packet.LineageId, hop1);
        collector.RecordHop(packet.LineageId, hop2);
        collector.RecordHop(packet.LineageId, hop3);
        var lineageInfo = collector.GetLineageInfo(packet.LineageId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageHops.Should().HaveCount(3);
        lineageInfo.LineageHops[0].NodeId.Should().Be("node1");
        lineageInfo.LineageHops[0].Outcome.Should().Be(HopDecisionFlags.Emitted);
        lineageInfo.LineageHops[1].NodeId.Should().Be("node2");
        lineageInfo.LineageHops[1].Outcome.Should().Be(HopDecisionFlags.FilteredOut);
        lineageInfo.LineageHops[2].NodeId.Should().Be("node3");
        lineageInfo.LineageHops[2].Outcome.Should().Be(HopDecisionFlags.Joined);
        lineageInfo.LineageHops[2].AncestryInputIndices.Should().BeEquivalentTo(new[] { 0, 1 });
    }

    private sealed class TestData
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}