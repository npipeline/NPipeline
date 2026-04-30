using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Extensions.Lineage.Tests;

public class LineageCollectorTests
{
    private static readonly Guid s_pipelineId = Guid.Parse("11111111-1111-1111-1111-111111111111");

    private static string QualifiedPathNode(string nodeId) => $"{s_pipelineId:N}::{nodeId}";

    private static LineageRecord BuildRecord(
        Guid correlationId,
        string nodeId,
        IReadOnlyList<string> traversalPath,
        LineageOutcomeReason outcomeReason = LineageOutcomeReason.Emitted,
        bool isTerminal = false,
        ObservedCardinality cardinality = ObservedCardinality.One,
        int? inputContributorCount = 1,
        int? outputEmissionCount = 1,
        IReadOnlyList<int>? contributorInputIndices = null,
        int? retryCount = null,
        object? data = null)
    {
        return new LineageRecord(
            correlationId,
            nodeId,
            s_pipelineId,
            outcomeReason,
            isTerminal,
            traversalPath,
            TimestampUtc: DateTimeOffset.UtcNow,
            RetryCount: retryCount,
            ContributorInputIndices: contributorInputIndices,
            InputContributorCount: inputContributorCount,
            OutputEmissionCount: outputEmissionCount,
            Cardinality: cardinality,
            Data: data);
    }

    [Fact]
    public void CreateLineagePacket_WithValidItemAndSourceNodeId_ShouldCreatePacketWithUniqueGuid()
    {
        var collector = new LineageCollector();
        var item = "test data";

        var packet1 = collector.CreateLineagePacket(item, "source1");
        var packet2 = collector.CreateLineagePacket(item, "source1");

        packet1.Should().NotBeNull();
        packet2.Should().NotBeNull();
        packet1.CorrelationId.Should().NotBeEmpty();
        packet2.CorrelationId.Should().NotBeEmpty();
        packet1.CorrelationId.Should().NotBe(packet2.CorrelationId);
        packet1.Data.Should().Be(item);
        packet2.Data.Should().Be(item);
        packet1.TraversalPath.Should().HaveCount(1);
        packet1.TraversalPath[0].Should().Be("source1");
    }

    [Fact]
    public void CreateLineagePacket_WithNullSourceNodeId_ShouldThrowArgumentNullException()
    {
        var collector = new LineageCollector();
        var item = "test data";

        Assert.Throws<ArgumentNullException>(() => collector.CreateLineagePacket(item, null!));
    }

    [Fact]
    public void CreateLineagePacket_WithDifferentDataTypes_ShouldCreatePackets()
    {
        var collector = new LineageCollector();

        var stringPacket = collector.CreateLineagePacket("string", "source1");
        var intPacket = collector.CreateLineagePacket(42, "source2");
        var objectPacket = collector.CreateLineagePacket(new TestData { Id = 1, Name = "Test" }, "source3");

        stringPacket.Data.Should().Be("string");
        intPacket.Data.Should().Be(42);
        objectPacket.Data.Should().BeOfType<TestData>();
        objectPacket.Data!.Id.Should().Be(1);
    }

    [Fact]
    public void Record_WithValidCorrelationIdAndEvent_ShouldRecordEvent()
    {
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test data", "source1");

        collector.Record(BuildRecord(packet.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")]));

        var history = collector.GetCorrelationHistory(packet.CorrelationId);

        history.Should().HaveCount(1);
        history[0].NodeId.Should().Be("node1");
        history[0].OutcomeReason.Should().Be(LineageOutcomeReason.Emitted);
        history[0].TraversalPath.Should().HaveCount(2);
        history[0].TraversalPath.Should().Contain(QualifiedPathNode("node1"));
    }

    [Fact]
    public void Record_WithMultipleEvents_ShouldRecordAllEventsAndPathOrder()
    {
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test data", "source1");

        collector.Record(BuildRecord(packet.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")]));
        collector.Record(BuildRecord(packet.CorrelationId, "node2", ["source1", QualifiedPathNode("node1"), QualifiedPathNode("node2")]));
        collector.Record(BuildRecord(packet.CorrelationId, "node3",
            ["source1", QualifiedPathNode("node1"), QualifiedPathNode("node2"), QualifiedPathNode("node3")]));

        var history = collector.GetCorrelationHistory(packet.CorrelationId);

        history.Should().HaveCount(3);
        history[^1].TraversalPath.Should().ContainInOrder(
            "source1",
            QualifiedPathNode("node1"),
            QualifiedPathNode("node2"),
            QualifiedPathNode("node3"));
    }

    [Fact]
    public void Record_WithDuplicateNodeIds_ShouldNotDuplicateTraversalPathSegments()
    {
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test data", "source1");

        collector.Record(BuildRecord(packet.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")]));
        collector.Record(BuildRecord(packet.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")]));

        var history = collector.GetCorrelationHistory(packet.CorrelationId);

        history.Should().HaveCount(2);
        history[^1].TraversalPath.Should().HaveCount(2);
        history[^1].TraversalPath.Should().ContainInOrder("source1", QualifiedPathNode("node1"));
    }

    [Fact]
    public void Record_WithUnknownCorrelationId_ShouldNotThrow()
    {
        var collector = new LineageCollector();
        var correlationId = Guid.NewGuid();

        var exception = Record.Exception(() =>
            collector.Record(BuildRecord(correlationId, "node1", [QualifiedPathNode("node1")])));

        exception.Should().BeNull();
        collector.GetCorrelationHistory(correlationId).Should().HaveCount(1);
    }

    [Theory]
    [InlineData(LineageOutcomeReason.Emitted, false)]
    [InlineData(LineageOutcomeReason.FilteredOut, true)]
    [InlineData(LineageOutcomeReason.Joined, false)]
    [InlineData(LineageOutcomeReason.Aggregated, false)]
    [InlineData(LineageOutcomeReason.Error, true)]
    [InlineData(LineageOutcomeReason.DeadLettered, true)]
    [InlineData(LineageOutcomeReason.DroppedByBackpressure, true)]
    [InlineData(LineageOutcomeReason.ConsumedWithoutEmission, true)]
    public void Record_WithVariousOutcomes_ShouldRecordCorrectly(LineageOutcomeReason outcomeReason, bool terminal)
    {
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test data", "source1");

        collector.Record(BuildRecord(
            packet.CorrelationId,
            "node1",
            ["source1", QualifiedPathNode("node1")],
            outcomeReason,
            terminal));

        var history = collector.GetCorrelationHistory(packet.CorrelationId);

        history.Should().HaveCount(1);
        history[0].OutcomeReason.Should().Be(outcomeReason);
        collector.GetTerminalReason(packet.CorrelationId).Should().Be(terminal ? outcomeReason : null);
    }

    [Fact]
    public void ShouldCollectLineage_WithNullOptions_ShouldReturnTrue()
    {
        var collector = new LineageCollector();

        collector.ShouldCollectLineage(Guid.NewGuid(), null).Should().BeTrue();
    }

    [Fact]
    public void ShouldCollectLineage_WithSampleEveryOne_ShouldReturnTrue()
    {
        var collector = new LineageCollector();
        var options = new LineageOptions(SampleEvery: 1);

        collector.ShouldCollectLineage(Guid.NewGuid(), options).Should().BeTrue();
    }

    [Fact]
    public void ShouldCollectLineage_WithDeterministicSampling_ShouldBeDeterministic()
    {
        var collector = new LineageCollector();
        var correlationId = Guid.NewGuid();
        var options = new LineageOptions(SampleEvery: 10, DeterministicSampling: true);

        var result1 = collector.ShouldCollectLineage(correlationId, options);
        var result2 = collector.ShouldCollectLineage(correlationId, options);
        var result3 = collector.ShouldCollectLineage(correlationId, options);

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
        var collector = new LineageCollector();
        var options = new LineageOptions(SampleEvery: sampleEvery, DeterministicSampling: true);
        var sampledCount = 0;
        var totalItems = sampleEvery * 10;

        var random = new Random(42);
        var guidBytes = new byte[16];

        for (var i = 0; i < totalItems; i++)
        {
            random.NextBytes(guidBytes);
            var correlationId = new Guid(guidBytes);

            if (collector.ShouldCollectLineage(correlationId, options))
                sampledCount++;
        }

        sampledCount.Should().BeGreaterThan(0);
        sampledCount.Should().BeLessThan(totalItems);

        var expectedSamples = totalItems / sampleEvery;
        var tolerance = totalItems / sampleEvery;

        sampledCount.Should().BeInRange(expectedSamples - tolerance, expectedSamples + tolerance);
    }

    [Fact]
    public void ShouldCollectLineage_WithNonDeterministicSampling_ShouldBeRandom()
    {
        var collector = new LineageCollector();
        var options = new LineageOptions(SampleEvery: 10, DeterministicSampling: false);
        var results = new List<bool>();

        for (var i = 0; i < 100; i++)
        {
            var correlationId = Guid.NewGuid();
            results.Add(collector.ShouldCollectLineage(correlationId, options));
        }

        results.Should().Contain(true);
        results.Should().Contain(false);
    }

    [Fact]
    public void GetCorrelationHistory_WithUnknownCorrelationId_ShouldReturnEmptyList()
    {
        var collector = new LineageCollector();

        collector.GetCorrelationHistory(Guid.NewGuid()).Should().BeEmpty();
    }

    [Fact]
    public void GetAllRecords_WithMultipleItems_ShouldReturnAllRecords()
    {
        var collector = new LineageCollector();
        var packet1 = collector.CreateLineagePacket("item1", "source1");
        var packet2 = collector.CreateLineagePacket("item2", "source1");
        var packet3 = collector.CreateLineagePacket("item3", "source1");

        collector.Record(BuildRecord(packet1.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")], data: "item1"));
        collector.Record(BuildRecord(packet2.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")], data: "item2"));
        collector.Record(BuildRecord(packet3.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")], data: "item3"));

        var allRecords = collector.GetAllRecords();

        allRecords.Should().HaveCount(3);
        allRecords.Should().Contain(record => record.CorrelationId == packet1.CorrelationId);
        allRecords.Should().Contain(record => record.CorrelationId == packet2.CorrelationId);
        allRecords.Should().Contain(record => record.CorrelationId == packet3.CorrelationId);
    }

    [Fact]
    public void GetAllRecords_WithNoItems_ShouldReturnEmptyList()
    {
        var collector = new LineageCollector();

        collector.GetAllRecords().Should().BeEmpty();
    }

    [Fact]
    public void Clear_ShouldRemoveAllLineageData()
    {
        var collector = new LineageCollector();
        var packet1 = collector.CreateLineagePacket("item1", "source1");
        var packet2 = collector.CreateLineagePacket("item2", "source1");

        collector.Record(BuildRecord(packet1.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")], data: "item1"));
        collector.Record(BuildRecord(packet2.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")], data: "item2"));

        collector.Clear();

        collector.GetAllRecords().Should().BeEmpty();
        collector.GetCorrelationHistory(packet1.CorrelationId).Should().BeEmpty();
        collector.GetCorrelationHistory(packet2.CorrelationId).Should().BeEmpty();
    }

    [Fact]
    public async Task ConcurrentAccess_ShouldBeThreadSafe()
    {
        var collector = new LineageCollector();
        var tasks = new List<Task>();
        var itemCount = 100;
        var threads = 10;

        for (var t = 0; t < threads; t++)
        {
            var threadId = t;

            tasks.Add(Task.Run(() =>
            {
                for (var i = 0; i < itemCount; i++)
                {
                    var packet = collector.CreateLineagePacket($"item-{threadId}-{i}", $"source-{threadId}");
                    collector.Record(BuildRecord(
                        packet.CorrelationId,
                        $"node-{threadId}",
                        [$"source-{threadId}", QualifiedPathNode($"node-{threadId}")],
                        data: $"item-{threadId}-{i}"));
                }
            }));
        }

        await Task.WhenAll(tasks);

        collector.GetAllRecords().Should().HaveCount(threads * itemCount);
    }

    [Fact]
    public async Task ConcurrentRecord_ShouldNotLoseEvents()
    {
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test", "source1");
        var eventCount = 100;
        var threads = 10;

        var tasks = Enumerable.Range(0, threads).Select(threadId =>
            Task.Run(() =>
            {
                for (var i = 0; i < eventCount; i++)
                {
                    collector.Record(BuildRecord(
                        packet.CorrelationId,
                        $"node-{threadId}-{i}",
                        ["source1", QualifiedPathNode($"node-{threadId}-{i}")]));
                }
            })
        );

        await Task.WhenAll(tasks);

        collector.GetCorrelationHistory(packet.CorrelationId).Should().HaveCount(threads * eventCount);
    }

    [Fact]
    public void CreateLineagePacket_WithNullItem_ShouldCreatePacket()
    {
        var collector = new LineageCollector();
        string? item = null;

        var packet = collector.CreateLineagePacket(item!, "source1");

        packet.Should().NotBeNull();
        packet.Data.Should().BeNull();
        packet.CorrelationId.Should().NotBeEmpty();
    }

    [Fact]
    public void GetCorrelationHistory_AfterMultipleOperations_ShouldReturnCorrectData()
    {
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test", "source1");

        collector.Record(BuildRecord(packet.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")],
            LineageOutcomeReason.Emitted, false, ObservedCardinality.One, 1, 1, data: "test"));

        collector.Record(BuildRecord(packet.CorrelationId, "node2",
            ["source1", QualifiedPathNode("node1"), QualifiedPathNode("node2")],
            LineageOutcomeReason.FilteredOut, false, ObservedCardinality.Zero, 1, 0, data: "test"));

        collector.Record(BuildRecord(packet.CorrelationId, "node3",
            ["source1", QualifiedPathNode("node1"), QualifiedPathNode("node2"), QualifiedPathNode("node3")],
            LineageOutcomeReason.Joined, false, ObservedCardinality.Many, 2, 3, [0, 1], data: "test"));

        var history = collector.GetCorrelationHistory(packet.CorrelationId);

        history.Should().HaveCount(3);
        history[0].NodeId.Should().Be("node1");
        history[0].OutcomeReason.Should().Be(LineageOutcomeReason.Emitted);
        history[1].NodeId.Should().Be("node2");
        history[1].OutcomeReason.Should().Be(LineageOutcomeReason.FilteredOut);
        history[2].NodeId.Should().Be("node3");
        history[2].OutcomeReason.Should().Be(LineageOutcomeReason.Joined);
        history[2].ContributorInputIndices.Should().BeEquivalentTo([0, 1]);
    }

    [Fact]
    public void GetTerminalReason_WithMultipleTerminalEvents_ShouldReturnLatestTerminalReason()
    {
        var collector = new LineageCollector();
        var packet = collector.CreateLineagePacket("test", "source1");

        collector.Record(BuildRecord(packet.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")],
            LineageOutcomeReason.FilteredOut, isTerminal: true));

        collector.Record(BuildRecord(packet.CorrelationId, "node2", ["source1", QualifiedPathNode("node1"), QualifiedPathNode("node2")],
            LineageOutcomeReason.DeadLettered, isTerminal: true));

        collector.GetTerminalReason(packet.CorrelationId).Should().Be(LineageOutcomeReason.DeadLettered);
    }

    [Fact]
    public void GetUnresolvedCorrelations_ShouldReturnOnlyNonTerminalCorrelations()
    {
        var collector = new LineageCollector();
        var unresolved = collector.CreateLineagePacket("unresolved", "source1");
        var resolved = collector.CreateLineagePacket("resolved", "source1");

        collector.Record(BuildRecord(unresolved.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")],
            LineageOutcomeReason.Emitted, isTerminal: false));

        collector.Record(BuildRecord(resolved.CorrelationId, "node1", ["source1", QualifiedPathNode("node1")],
            LineageOutcomeReason.DeadLettered, isTerminal: true));

        var unresolvedCorrelations = collector.GetUnresolvedCorrelations();

        unresolvedCorrelations.Should().Contain(unresolved.CorrelationId);
        unresolvedCorrelations.Should().NotContain(resolved.CorrelationId);
    }

    private sealed class TestData
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
