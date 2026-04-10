using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Extensions.Lineage.Tests;

public class LineageCollectorTests
{
    private static readonly Guid s_pipelineId = Guid.Parse("11111111-1111-1111-1111-111111111111");

    private static string QualifiedPathNode(string nodeId) => $"{s_pipelineId:N}::{nodeId}";

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
        packet1.CorrelationId.Should().NotBeEmpty();
        packet2.CorrelationId.Should().NotBeEmpty();
        packet1.CorrelationId.Should().NotBe(packet2.CorrelationId);
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
        objectPacket.Data!.Id.Should().Be(1);
    }

    [Fact]
    public void RecordHop_WithValidCorrelationIdAndHop_ShouldRecordHop()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");

        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);

        // Act
        collector.RecordHop(packet.CorrelationId, hop);
        var lineageInfo = collector.GetLineageInfo(packet.CorrelationId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageHops.Should().HaveCount(1);
        lineageInfo.LineageHops[0].NodeId.Should().Be("node1");
        lineageInfo.LineageHops[0].Outcome.Should().Be(HopDecisionFlags.Emitted);
        lineageInfo.TraversalPath.Should().HaveCount(2);
        lineageInfo.TraversalPath.Should().Contain(QualifiedPathNode("node1"));
    }

    [Fact]
    public void RecordHop_WithMultipleHops_ShouldRecordAllHops()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");
        var hop1 = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);
        var hop2 = new LineageHop("node2", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);
        var hop3 = new LineageHop("node3", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);

        // Act
        collector.RecordHop(packet.CorrelationId, hop1);
        collector.RecordHop(packet.CorrelationId, hop2);
        collector.RecordHop(packet.CorrelationId, hop3);
        var lineageInfo = collector.GetLineageInfo(packet.CorrelationId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageHops.Should().HaveCount(3);
        lineageInfo.TraversalPath.Should().HaveCount(4);
        lineageInfo.TraversalPath.Should().ContainInOrder(
            "source1",
            QualifiedPathNode("node1"),
            QualifiedPathNode("node2"),
            QualifiedPathNode("node3"));
    }

    [Fact]
    public void RecordHop_WithDuplicateNodeIds_ShouldNotDuplicateInTraversalPath()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");
        var hop1 = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);
        var hop2 = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);

        // Act
        collector.RecordHop(packet.CorrelationId, hop1);
        collector.RecordHop(packet.CorrelationId, hop2);
        var lineageInfo = collector.GetLineageInfo(packet.CorrelationId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.TraversalPath.Should().HaveCount(2);
        lineageInfo.TraversalPath.Should().ContainInOrder("source1", QualifiedPathNode("node1"));
        lineageInfo.LineageHops.Should().HaveCount(2);
    }

    [Fact]
    public void RecordHop_WithUnknownCorrelationId_ShouldNotThrow()
    {
        // Arrange
        var collector = new LineageCollector();
        var unknownCorrelationId = Guid.NewGuid();
        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);

        // Act & Assert
        var exception = Record.Exception(() => collector.RecordHop(unknownCorrelationId, hop));
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
        var hop = new LineageHop("node1", outcome, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);

        // Act
        collector.RecordHop(packet.CorrelationId, hop);
        var lineageInfo = collector.GetLineageInfo(packet.CorrelationId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.LineageHops[0].Outcome.Should().Be(outcome);
    }

    [Fact]
    public void ShouldCollectLineage_WithNullOptions_ShouldReturnTrue()
    {
        // Arrange
        var collector = new LineageCollector();
        var correlationId = Guid.NewGuid();

        // Act
        var result = collector.ShouldCollectLineage(correlationId, null);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void ShouldCollectLineage_WithSampleEveryOne_ShouldReturnTrue()
    {
        // Arrange
        var collector = new LineageCollector();
        var correlationId = Guid.NewGuid();
        var options = new LineageOptions(SampleEvery: 1);

        // Act
        var result = collector.ShouldCollectLineage(correlationId, options);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void ShouldCollectLineage_WithDeterministicSampling_ShouldBeDeterministic()
    {
        // Arrange
        var collector = new LineageCollector();
        var correlationId = Guid.NewGuid();
        var options = new LineageOptions(SampleEvery: 10, DeterministicSampling: true);

        // Act
        var result1 = collector.ShouldCollectLineage(correlationId, options);
        var result2 = collector.ShouldCollectLineage(correlationId, options);
        var result3 = collector.ShouldCollectLineage(correlationId, options);

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

        // Use a fixed seed to generate reproducible GUIDs for deterministic testing
        var random = new Random(42); // Fixed seed for reproducibility
        var guidBytes = new byte[16];

        // Act
        for (var i = 0; i < totalItems; i++)
        {
            random.NextBytes(guidBytes);
            var correlationId = new Guid(guidBytes);

            if (collector.ShouldCollectLineage(correlationId, options))
                sampledCount++;
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
            var correlationId = Guid.NewGuid();
            results.Add(collector.ShouldCollectLineage(correlationId, options));
        }

        // Assert
        results.Should().Contain(true);
        results.Should().Contain(false);
    }

    [Fact]
    public void GetLineageInfo_WithValidCorrelationId_ShouldReturnLineageInfo()
    {
        // Arrange
        var collector = new LineageCollector();
        var item = "test data";
        var packet = collector.CreateLineagePacket(item, "source1");
        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);
        collector.RecordHop(packet.CorrelationId, hop);

        // Act
        var lineageInfo = collector.GetLineageInfo(packet.CorrelationId);

        // Assert
        lineageInfo.Should().NotBeNull();
        lineageInfo!.CorrelationId.Should().Be(packet.CorrelationId);
        lineageInfo.Data.Should().Be(item);
        lineageInfo.TraversalPath.Should().HaveCount(2);
        lineageInfo.LineageHops.Should().HaveCount(1);
    }

    [Fact]
    public void GetLineageInfo_WithUnknownCorrelationId_ShouldReturnNull()
    {
        // Arrange
        var collector = new LineageCollector();
        var unknownCorrelationId = Guid.NewGuid();

        // Act
        var lineageInfo = collector.GetLineageInfo(unknownCorrelationId);

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
        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);

        collector.RecordHop(packet1.CorrelationId, hop);
        collector.RecordHop(packet2.CorrelationId, hop);
        collector.RecordHop(packet3.CorrelationId, hop);

        // Act
        var allLineageInfo = collector.GetAllLineageInfo();

        // Assert
        allLineageInfo.Should().HaveCount(3);
        allLineageInfo.Should().Contain(info => info.CorrelationId == packet1.CorrelationId);
        allLineageInfo.Should().Contain(info => info.CorrelationId == packet2.CorrelationId);
        allLineageInfo.Should().Contain(info => info.CorrelationId == packet3.CorrelationId);
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
        var hop = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);
        collector.RecordHop(packet1.CorrelationId, hop);
        collector.RecordHop(packet2.CorrelationId, hop);

        // Act
        collector.Clear();
        var allLineageInfo = collector.GetAllLineageInfo();

        // Assert
        allLineageInfo.Should().BeEmpty();
        collector.GetLineageInfo(packet1.CorrelationId).Should().BeNull();
        collector.GetLineageInfo(packet2.CorrelationId).Should().BeNull();
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

                    var hop = new LineageHop($"node-{threadId}", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);

                    collector.RecordHop(packet.CorrelationId, hop);
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
                    var hop = new LineageHop($"node-{threadId}-{i}", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);

                    collector.RecordHop(packet.CorrelationId, hop);
                }
            })
        );

        await Task.WhenAll(tasks);

        // Assert
        var lineageInfo = collector.GetLineageInfo(packet.CorrelationId);
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
        packet.CorrelationId.Should().NotBeEmpty();
    }

    [Fact]
    public void GetLineageInfo_AfterMultipleOperations_ShouldReturnCorrectData()
    {
        // Arrange
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test", "source1");
        var hop1 = new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false, s_pipelineId);
        var hop2 = new LineageHop("node2", HopDecisionFlags.FilteredOut, ObservedCardinality.Zero, 1, 0, null, false, s_pipelineId);
        var hop3 = new LineageHop("node3", HopDecisionFlags.Joined, ObservedCardinality.Many, 2, 3, new[] { 0, 1 }, false, s_pipelineId);

        // Act
        collector.RecordHop(packet.CorrelationId, hop1);
        collector.RecordHop(packet.CorrelationId, hop2);
        collector.RecordHop(packet.CorrelationId, hop3);
        var lineageInfo = collector.GetLineageInfo(packet.CorrelationId);

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
