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
    public async Task MergeAsync_NonJoin_UsesRuntimeStreamType_WhenNodeInputTypeIsPayloadType()
    {
        var service = new PipeMergeService(new MergeStrategySelector());
        var node = new NullNode();

        var nodeDefinition = new NodeDefinition(
            Id: "sink",
            Name: "sink",
            NodeType: typeof(object),
            Kind: NodeKind.Sink,
            InputType: typeof(int),
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
            Id: "sink",
            Name: "sink",
            NodeType: typeof(object),
            Kind: NodeKind.Sink,
            InputType: typeof(int),
            MergeStrategy: MergeType.Interleave);

        IDataStream[] inputPipes =
        [
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1], "left"),
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<long>([2], "right"),
        ];

        Func<Task> act = async () =>
        {
            await using var merged = await service.MergeAsync(nodeDefinition, node, inputPipes);
        };

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        _ = thrown.Which.Message.Should().Contain("multiple runtime input stream types");
        _ = thrown.Which.Message.Should().Contain("System.Int32");
        _ = thrown.Which.Message.Should().Contain("System.Int64");
    }

    private static LineagePacket<int> CreatePacket(int value)
    {
        return new LineagePacket<int>(value, Guid.NewGuid(), ImmutableList<string>.Empty);
    }

    private sealed class NullNode : INode
    {
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
