using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;

namespace NPipeline.Extensions.Lineage.Tests.Integration;

/// <summary>
///     Integration tests for the lineage extension DI registration and component resolution.
/// </summary>
public class LineageIntegrationTests
{
    private static readonly Guid s_pipelineId = Guid.Parse("11111111-1111-1111-1111-111111111111");

    private static string QualifiedPathNode(string nodeId) => $"{s_pipelineId:N}::{nodeId}";

    private static LineageRecord BuildRecord(Guid correlationId, string nodeId, IReadOnlyList<string> traversalPath, object? data = null,
        LineageOutcomeReason outcomeReason = LineageOutcomeReason.Emitted, bool isTerminal = false)
    {
        return new LineageRecord(
            correlationId,
            nodeId,
            s_pipelineId,
            outcomeReason,
            isTerminal,
            traversalPath,
            Data: data,
            Cardinality: ObservedCardinality.One);
    }

    [Fact]
    public void AddNPipelineLineage_ShouldRegisterAllRequiredServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();

        // Assert
        serviceProvider.GetService<ILineageCollector>().Should().NotBeNull();
        serviceProvider.GetService<IPipelineLineageSink>().Should().NotBeNull();
        serviceProvider.GetService<ILineageFactory>().Should().NotBeNull();
        serviceProvider.GetService<IPipelineLineageSinkProvider>().Should().NotBeNull();
    }

    [Fact]
    public void LineageCollector_ShouldTrackItemsCorrectly()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var collector = serviceProvider.GetRequiredService<ILineageCollector>();

        // Act - simulate pipeline flow
        var packet1 = collector.CreateLineagePacket("item1", "source-node");
        var packet2 = collector.CreateLineagePacket("item2", "source-node");
        var packet3 = collector.CreateLineagePacket("item3", "source-node");

        collector.Record(BuildRecord(packet1.CorrelationId, "transform-node", ["source-node", QualifiedPathNode("transform-node")], "item1"));
        collector.Record(BuildRecord(packet2.CorrelationId, "transform-node", ["source-node", QualifiedPathNode("transform-node")], "item2"));
        collector.Record(BuildRecord(packet3.CorrelationId, "transform-node", ["source-node", QualifiedPathNode("transform-node")], "item3"));

        // Assert
        var allRecords = collector.GetAllRecords();
        allRecords.Should().HaveCount(3);
        allRecords.Select(r => r.CorrelationId).Distinct().Should().HaveCount(3);
        allRecords.Should().OnlyContain(r => r.TraversalPath.Contains("source-node"));
        allRecords.Should().OnlyContain(r => r.TraversalPath.Contains(QualifiedPathNode("transform-node")));
    }

    [Fact]
    public void LineageCollector_WithSampling_ShouldSampleDeterministically()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var collector = serviceProvider.GetRequiredService<ILineageCollector>();
        var options = new LineageOptions(SampleEvery: 10, DeterministicSampling: true);

        // Act - sample 100 items
        var sampledCount = 0;

        for (var i = 0; i < 100; i++)
        {
            var packet = collector.CreateLineagePacket(i, "source");

            if (collector.ShouldCollectLineage(packet.CorrelationId, options))
                sampledCount++;
        }

        // Assert - with deterministic sampling, should be roughly 10% (±some tolerance)
        sampledCount.Should().BeGreaterThan(0);
        sampledCount.Should().BeLessThan(100);
    }

    [Fact]
    public void LineageCollector_Clear_ShouldRemoveAllData()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var collector = serviceProvider.GetRequiredService<ILineageCollector>();

        // Add some data
        var packet = collector.CreateLineagePacket("test", "source");
        collector.Record(BuildRecord(packet.CorrelationId, "node1", ["source", QualifiedPathNode("node1")], "test"));

        // Act
        collector.Clear();

        // Assert
        collector.GetAllRecords().Should().BeEmpty();
        collector.GetCorrelationHistory(packet.CorrelationId).Should().BeEmpty();
    }

    [Fact]
    public void LineageCollector_GetCorrelationHistory_ShouldReturnCorrectItemTimeline()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var collector = serviceProvider.GetRequiredService<ILineageCollector>();

        // Act
        var packet1 = collector.CreateLineagePacket("item1", "source");
        var packet2 = collector.CreateLineagePacket("item2", "source");

        collector.Record(BuildRecord(packet1.CorrelationId, "node1", ["source", QualifiedPathNode("node1")], "item1"));

        // Assert
        var lineage1 = collector.GetCorrelationHistory(packet1.CorrelationId);
        var lineage2 = collector.GetCorrelationHistory(packet2.CorrelationId);

        lineage1.Should().HaveCount(1);
        lineage1[0].CorrelationId.Should().Be(packet1.CorrelationId);
        lineage1[0].Data.Should().Be("item1");

        lineage2.Should().BeEmpty();
    }

    [Fact]
    public void DiLineageFactory_ShouldResolveRegisteredCollector()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var factory = serviceProvider.GetRequiredService<ILineageFactory>();

        // Act
        var collector = factory.ResolveLineageCollector();

        // Assert
        collector.Should().NotBeNull();
        collector.Should().BeOfType<LineageCollector>();
    }

    [Fact]
    public void DiLineageFactory_ShouldResolveRegisteredSinkProvider()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var factory = serviceProvider.GetRequiredService<ILineageFactory>();

        // Act
        var provider = factory.ResolvePipelineLineageSinkProvider();

        // Assert
        provider.Should().NotBeNull();
        provider.Should().BeOfType<DefaultPipelineLineageSinkProvider>();
    }

    [Fact]
    public void LineageCollector_MultipleHops_ShouldPreserveOrder()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var collector = serviceProvider.GetRequiredService<ILineageCollector>();

        var packet = collector.CreateLineagePacket("test", "source");

        // Act
        collector.Record(BuildRecord(packet.CorrelationId, "node1", ["source", QualifiedPathNode("node1")], "test"));
        collector.Record(BuildRecord(packet.CorrelationId, "node2", ["source", QualifiedPathNode("node1"), QualifiedPathNode("node2")], "test"));
        collector.Record(BuildRecord(packet.CorrelationId, "node3", ["source", QualifiedPathNode("node1"), QualifiedPathNode("node2"), QualifiedPathNode("node3")], "test"));

        // Assert
        var lineage = collector.GetCorrelationHistory(packet.CorrelationId);
        lineage.Should().HaveCount(3);
        lineage[^1].TraversalPath.Should().ContainInOrder(
            "source",
            QualifiedPathNode("node1"),
            QualifiedPathNode("node2"),
            QualifiedPathNode("node3"));
        lineage.Select(r => r.NodeId).Should().ContainInOrder("node1", "node2", "node3");
    }

    [Fact]
    public async Task LoggingPipelineLineageSink_ShouldRecordWithoutErrors()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var sink = serviceProvider.GetRequiredService<IPipelineLineageSink>();

        var report = new PipelineLineageReport("TestPipeline", Guid.NewGuid(), [new NodeLineageInfo("source", "SourceNode", null, "string")], [], s_pipelineId);

        // Act
        var exception = await Record.ExceptionAsync(() => sink.RecordAsync(report, CancellationToken.None));

        // Assert
        exception.Should().BeNull();
    }

    [Fact]
    public void ScopedServices_ShouldBeIndependentPerScope()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();

        // Act
        using var scope1 = serviceProvider.CreateScope();
        using var scope2 = serviceProvider.CreateScope();

        var collector1 = scope1.ServiceProvider.GetRequiredService<ILineageCollector>();
        var collector2 = scope2.ServiceProvider.GetRequiredService<ILineageCollector>();

        // Add data to collector1 only
        var packet = collector1.CreateLineagePacket("test", "source");
        collector1.Record(BuildRecord(packet.CorrelationId, "node1", ["source", QualifiedPathNode("node1")], "test"));

        // Assert - collectors should be independent
        collector1.Should().NotBeSameAs(collector2);
        collector1.GetAllRecords().Should().HaveCount(1);
        collector2.GetAllRecords().Should().BeEmpty();
    }

    [Fact]
    public void CustomSinkRegistration_ShouldUseCustomSink()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage<TestPipelineLineageSink>();
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var sink = serviceProvider.GetService<IPipelineLineageSink>();

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>();
    }

    [Fact]
    public void FactoryDelegateRegistration_ShouldUseFactoryProvidedSink()
    {
        // Arrange
        var factoryCalled = false;
        var services = new ServiceCollection();

        services.AddNPipelineLineage(_ =>
        {
            factoryCalled = true;
            return new TestPipelineLineageSink();
        });

        var serviceProvider = services.BuildServiceProvider();

        // Act
        var sink = serviceProvider.GetService<IPipelineLineageSink>();

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<TestPipelineLineageSink>();
        factoryCalled.Should().BeTrue();
    }

    // Test sink for custom registration testing
    private sealed class TestPipelineLineageSink : IPipelineLineageSink
    {
        public Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
