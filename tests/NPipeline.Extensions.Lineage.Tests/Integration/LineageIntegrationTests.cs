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

        collector.RecordHop(packet1.LineageId, new LineageHop("transform-node", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));
        collector.RecordHop(packet2.LineageId, new LineageHop("transform-node", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));
        collector.RecordHop(packet3.LineageId, new LineageHop("transform-node", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));

        // Assert
        var allLineage = collector.GetAllLineageInfo();
        allLineage.Should().HaveCount(3);
        allLineage.Should().OnlyContain(l => l.TraversalPath.Contains("source-node"));
        allLineage.Should().OnlyContain(l => l.TraversalPath.Contains("transform-node"));
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

            if (collector.ShouldCollectLineage(packet.LineageId, options))
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
        collector.RecordHop(packet.LineageId, new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));

        // Act
        collector.Clear();

        // Assert
        collector.GetAllLineageInfo().Should().BeEmpty();
        collector.GetLineageInfo(packet.LineageId).Should().BeNull();
    }

    [Fact]
    public void LineageCollector_GetLineageInfo_ShouldReturnCorrectItem()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var collector = serviceProvider.GetRequiredService<ILineageCollector>();

        // Act
        var packet1 = collector.CreateLineagePacket("item1", "source");
        var packet2 = collector.CreateLineagePacket("item2", "source");

        collector.RecordHop(packet1.LineageId, new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));

        // Assert
        var lineage1 = collector.GetLineageInfo(packet1.LineageId);
        var lineage2 = collector.GetLineageInfo(packet2.LineageId);

        lineage1.Should().NotBeNull();
        lineage1!.LineageId.Should().Be(packet1.LineageId);
        lineage1.Data.Should().Be("item1");
        lineage1.LineageHops.Should().HaveCount(1);

        lineage2.Should().NotBeNull();
        lineage2!.LineageId.Should().Be(packet2.LineageId);
        lineage2.Data.Should().Be("item2");
        lineage2.LineageHops.Should().BeEmpty();
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
        collector.RecordHop(packet.LineageId, new LineageHop("node1", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));
        collector.RecordHop(packet.LineageId, new LineageHop("node2", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));
        collector.RecordHop(packet.LineageId, new LineageHop("node3", HopDecisionFlags.Emitted, ObservedCardinality.One, 1, 1, null, false));

        // Assert
        var lineage = collector.GetLineageInfo(packet.LineageId);
        lineage.Should().NotBeNull();
        lineage!.TraversalPath.Should().ContainInOrder("source", "node1", "node2", "node3");
        lineage.LineageHops.Should().HaveCount(3);
        lineage.LineageHops[0].NodeId.Should().Be("node1");
        lineage.LineageHops[1].NodeId.Should().Be("node2");
        lineage.LineageHops[2].NodeId.Should().Be("node3");
    }

    [Fact]
    public async Task LoggingPipelineLineageSink_ShouldRecordWithoutErrors()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddNPipelineLineage();
        var serviceProvider = services.BuildServiceProvider();
        var sink = serviceProvider.GetRequiredService<IPipelineLineageSink>();

        var report = new PipelineLineageReport(
            "TestPipeline",
            Guid.NewGuid(),
            [new NodeLineageInfo("source", "SourceNode", null, "string")],
            []
        );

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

        // Assert - collectors should be independent
        collector1.Should().NotBeSameAs(collector2);
        collector1.GetAllLineageInfo().Should().HaveCount(1);
        collector2.GetAllLineageInfo().Should().BeEmpty();
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
