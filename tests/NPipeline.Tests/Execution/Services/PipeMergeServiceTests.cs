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
    public void MergeAsync_NoInputStreams_ReturnsAFaultedTaskRatherThanThrowing()
    {
        var service = new PipeMergeService(new MergeStrategySelector());
        var nodeDefinition = new NodeDefinition("aggregate", "orders", typeof(object), NodeKind.Aggregate, typeof(int), typeof(int));

        var task = service.MergeAsync(nodeDefinition, new NullNode(), [], null);

        _ = task.IsFaulted.Should().BeTrue();
        _ = task.Exception!.InnerException.Should().BeOfType<InvalidOperationException>();
    }

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

        Func<Task> act = () => service.MergeAsync(nodeDefinition, new NullNode(), [], null);

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

        await using var merged = await service.MergeAsync(nodeDefinition, node, inputPipes, null);

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
            await using var merged = await service.MergeAsync(nodeDefinition, node, inputPipes, null);
        };

        var thrown = await act.Should().ThrowAsync<InvalidOperationException>();
        _ = thrown.Which.Message.Should().Contain("multiple runtime input stream types");
        _ = thrown.Which.Message.Should().Contain("System.Int32");
        _ = thrown.Which.Message.Should().Contain("System.Int64");
    }

    /// <summary>
    ///     The delegate cache must hold only type-shaped code. A selector registered later must be consulted for the
    ///     same (type, merge type) pair, instead of the first scope's selector being kept alive and reused.
    /// </summary>
    [Fact]
    public async Task MergeAsync_UsesTheCurrentScopeStrategySelector_EvenAfterAnotherScopeWarmedTheCache()
    {
        // A private type, so no other test can warm the cache for this pair first.
        var nodeDefinition = new NodeDefinition(
            "sink",
            "sink",
            typeof(object),
            NodeKind.Sink,
            typeof(SelectorCacheProbe),
            MergeStrategy: MergeType.Interleave);

        IDataStream[] inputPipes =
        [
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<SelectorCacheProbe>([new()], "left"),
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<SelectorCacheProbe>([new()], "right"),
        ];

        var warmingService = new PipeMergeService(new MergeStrategySelector());
        await using (await warmingService.MergeAsync(nodeDefinition, new NullNode(), inputPipes, null))
        {
        }

        var recordingSelector = new RecordingSelector();
        var service = new PipeMergeService(recordingSelector);
        await using (await service.MergeAsync(nodeDefinition, new NullNode(), inputPipes, null))
        {
        }

        recordingSelector.Calls.Should().Be(1, "the current scope's selector must be consulted, not the cached one");
    }

    /// <summary>
    ///     Merging built-in strategies only constructs a lazy stream, so the task returned by <see cref="IPipeMergeService.MergeAsync" />
    ///     must already be complete rather than queueing a thread-pool hop per merge.
    /// </summary>
    [Fact]
    public async Task MergeAsync_BuiltInInterleave_ReturnsAnAlreadyCompletedTask()
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
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([2], "right"),
        ];

        var task = service.MergeAsync(nodeDefinition, node, inputPipes, null);

        task.IsCompleted.Should().BeTrue("no work is queued to the thread pool for a built-in strategy");

        await using var merged = await task;
        _ = merged.GetDataType().Should().Be<int>();
    }

    private sealed record SelectorCacheProbe;

    /// <summary>
    ///     A selector that counts <see cref="IMergeStrategySelector.GetStrategy" /> calls, to detect a cached delegate
    ///     holding onto an earlier scope's selector.
    /// </summary>
    private sealed class RecordingSelector : IMergeStrategySelector
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public object GetStrategy(Type dataType, MergeType mergeType)
        {
            _ = Interlocked.Increment(ref _calls);
            return new MergeStrategySelector().GetStrategy(dataType, mergeType);
        }
    }

    private static LineagePacket<int> CreatePacket(int value) => new(value, Guid.NewGuid(), ImmutableArray<string>.Empty);

    private sealed class NullNode : INode
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
