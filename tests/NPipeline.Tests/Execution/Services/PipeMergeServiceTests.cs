using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;

namespace NPipeline.Tests.Execution.Services;

public sealed class PipeMergeServiceTests
{
    [Fact]
    public async Task MergeAsync_NoInputStreams_ThrowsActionableError()
    {
        var service = new PipeMergeService(new MergeStrategySelector());

        var nodeDefinition = new NodeDefinition(
            "aggregate",
            "orders",
            typeof(object),
            NodeKind.Aggregate,
            typeof(int),
            typeof(int));

        Func<Task> act = () => service.MergeAsync(nodeDefinition, new NullNode(), []);

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        _ = thrown.Which.Message.Should().Contain($"[{ErrorCodes.NodeMissingInputConnection}]");
        _ = thrown.Which.Message.Should().Contain("aggregate");
        _ = thrown.Which.Message.Should().Contain("Connect an upstream node");
    }

    [Fact]
    public async Task MergeAsync_NonJoin_UsesRuntimeStreamType_WhenNodeInputTypeIsPayloadType()
    {
        var service = new PipeMergeService(new MergeStrategySelector());
        var node = new NullNode();

        var nodeDefinition = new NodeDefinition(
            "sink",
            "sink",
            typeof(object),
            NodeKind.Sink,
            typeof(int),
            MergeStrategy: MergeType.Interleave);

        IDataStream[] inputPipes =
        [
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>(
                [
                    CreatePacket(1),
                    CreatePacket(2),
                ],
                "left"),
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<LineagePacket<int>>(
                [
                    CreatePacket(3),
                    CreatePacket(4),
                ],
                "right"),
        ];

        await using var merged = await service.MergeAsync(nodeDefinition, node, inputPipes);

        _ = merged.GetDataType().Should().Be<LineagePacket<int>>();

        var typed = (IDataStream<LineagePacket<int>>)merged;
        List<int> values = [];

        await foreach (var packet in typed)
        {
            values.Add(packet.Data);
        }

        _ = values.Should().BeEquivalentTo([1, 2, 3, 4]);
    }

    [Fact]
    public async Task MergeAsync_NonJoin_Throws_WhenRuntimeInputTypesDiffer()
    {
        var service = new PipeMergeService(new MergeStrategySelector());
        var node = new NullNode();

        var nodeDefinition = new NodeDefinition(
            "sink",
            "sink",
            typeof(object),
            NodeKind.Sink,
            typeof(int),
            MergeStrategy: MergeType.Interleave);

        IDataStream[] inputPipes =
        [
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1], "left"),
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<long>([2], "right"),
        ];

        var act = async () =>
        {
            await using var merged = await service.MergeAsync(nodeDefinition, node, inputPipes);
        };

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        _ = thrown.Which.Message.Should().Contain("multiple runtime input stream types");
        _ = thrown.Which.Message.Should().Contain("System.Int32");
        _ = thrown.Which.Message.Should().Contain("System.Int64");
    }

    private static LineagePacket<int> CreatePacket(int value) => new(value, Guid.NewGuid(), ImmutableArray<string>.Empty);

    private sealed class NullNode : INode
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
