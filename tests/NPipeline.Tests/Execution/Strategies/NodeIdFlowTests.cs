using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Execution.Strategies;

/// <summary>
///     The node id used to reach both execution strategies and node authors through one mutable field on the shared
///     context, so concurrent workers interleaved their writes to it and retry options, error attribution and
///     diagnostics could all be resolved against another node's id.
///     <para>
///         Strategies now take the id as a parameter, and a node asks for its own with
///         <see cref="PipelineNodeEnvironmentContext.GetNodeId" />, which resolves it from the instance rather than
///         from a field that only one node at a time can be right about.
///     </para>
/// </summary>
public sealed class NodeIdFlowTests
{
    [Fact]
    public async Task AStrategy_ReceivesItsOwnNodeId()
    {
        var recorder = new RecordingStrategy();
        var node = new PassthroughNode();
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);

        await using var input = new DataStream<int>(new[] { 1, 2, 3 }.ToAsyncEnumerable(), "input");
        await using var output = await recorder.ExecuteAsync(input, node, context, "my-node", CancellationToken.None);

        recorder.ObservedNodeId.Should().Be("my-node");
    }

    [Fact]
    public async Task ANodeAsksForItsOwnId_AndGetsIt()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        var node = new PassthroughNode();

        context.NodeEnvironment.RegisterNode("my-node", node);

        context.NodeEnvironment.GetNodeId(node).Should().Be("my-node");
        context.NodeEnvironment.TryGetNodeId(node, out var resolved).Should().BeTrue();
        resolved.Should().Be("my-node");
    }

    [Fact]
    public async Task ANodeThatIsNotPartOfTheRun_IsToldSo_RatherThanGivenSomeoneElsesId()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        context.NodeEnvironment.RegisterNode("some-other-node", new PassthroughNode());

        var stranger = new PassthroughNode();

        context.NodeEnvironment.TryGetNodeId(stranger, out _).Should().BeFalse();

        var act = () => context.NodeEnvironment.GetNodeId(stranger);
        act.Should().Throw<InvalidOperationException>().WithMessage("*NP0423*");
    }

    [Fact]
    public async Task OneInstanceWiredInTwice_ReportsTheAmbiguity_RatherThanPickingOne()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        var shared = new PassthroughNode();

        context.NodeEnvironment.RegisterNode("first", shared);
        context.NodeEnvironment.RegisterNode("second", shared);

        context.NodeEnvironment.TryGetNodeId(shared, out _).Should().BeFalse("neither id is the answer");
    }

    /// <summary>
    ///     The case the old shared field could not serve: two sinks draining at the same time, each needing its own id.
    /// </summary>
    [Fact]
    public async Task TerminalsDrainingConcurrently_EachResolveTheirOwnId()
    {
        IdRecorder.Reset();

        await using var context = new PipelineContext(PipelineContextConfiguration.Default);
        await PipelineRunner.Create().RunAsync(new FanOutPipeline(), context, CancellationToken.None);

        IdRecorder.Observed.Should().NotBeEmpty();
        IdRecorder.Observed.Should().OnlyContain(pair => pair.Expected == pair.Actual,
            "a sink asking for its own id must never be answered with a sibling's");
    }

    private static class IdRecorder
    {
        public static ConcurrentBag<(string Expected, string Actual)> Observed { get; private set; } = [];

        public static void Reset()
        {
            Observed = [];
        }
    }

    private sealed class RecordingStrategy : IExecutionStrategy
    {
        public string? ObservedNodeId { get; private set; }

        public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
            string nodeId, CancellationToken cancellationToken)
        {
            ObservedNodeId = nodeId;
            return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(Array.Empty<TOut>().ToAsyncEnumerable(), "empty"));
        }
    }

    private sealed class PassthroughNode : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(item);
        }
    }

    private sealed class CountingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new DataStream<int>(Produce(cancellationToken), "source");
        }

        private static async IAsyncEnumerable<int> Produce(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (var i = 0; i < 200; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
                yield return i;
            }
        }
    }

    /// <summary>
    ///     A sink that knows which id it was built for and checks the runtime agrees, on every item, while its sibling
    ///     drains alongside it.
    /// </summary>
    private sealed class DrainingSink(string expectedNodeId) : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                IdRecorder.Observed.Add((expectedNodeId, context.NodeEnvironment.GetNodeId(this)));
            }
        }
    }

    private sealed class FanOutPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<CountingSource, int>("source");

            var first = builder.AddSink<DrainingSink, int>("first");
            _ = builder.AddPreconfiguredNodeInstance(first.Id, new DrainingSink(first.Id));

            var second = builder.AddSink<DrainingSink, int>("second");
            _ = builder.AddPreconfiguredNodeInstance(second.Id, new DrainingSink(second.Id));

            _ = builder.Connect(source, first);
            _ = builder.Connect(source, second);
            _ = builder.WithBranchOptions("source", new BranchOptions(16));
        }
    }
}
