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
///     The node id used to reach execution strategies through <see cref="PipelineContext.CurrentNodeId" />, one mutable
///     field on a context shared by every node in the run. Concurrent workers interleaved their writes to it, so retry
///     options, error attribution and diagnostics could all be resolved against another node's id. It is now a
///     parameter on <see cref="IExecutionStrategy" />, and the field is frozen while nodes run concurrently.
/// </summary>
public sealed class NodeIdFlowTests
{
    [Fact]
    public async Task Strategy_ReceivesItsOwnNodeId_NotWhateverTheSharedFieldHolds()
    {
        var recorder = new RecordingStrategy();
        var node = new PassthroughNode { ExecutionStrategy = recorder };
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);

        // Whatever is in the shared field must not reach the strategy.
        using (context.ScopedNode("some-other-node"))
        {
            await using var input = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1, 2, 3], "input");
            await using var output = await recorder.ExecuteAsync(input, node, context, "my-node", CancellationToken.None);

            recorder.ObservedNodeId.Should().Be("my-node");
        }
    }

    [Fact]
    public async Task ScopedNode_IsInertWhileNodesRunConcurrently()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);

        using (context.ScopedNode("sequential-node"))
        {
            context.CurrentNodeId.Should().Be("sequential-node");

            context.NodeEnvironment.NodesRunConcurrently = true;

            using (context.ScopedNode("concurrent-node"))
            {
                context.CurrentNodeId.Should().Be("sequential-node", "a frozen field shows a stale id, never another node's");
            }

            context.CurrentNodeId.Should().Be("sequential-node", "an inert scope must not clobber the field on disposal either");

            context.NodeEnvironment.NodesRunConcurrently = false;
        }
    }

    /// <summary>
    ///     The failure the freeze prevents: without it, two workers entering and leaving overlapping scopes leave the
    ///     field pointing at whichever finished last, and every later read is wrong.
    /// </summary>
    [Fact]
    public async Task ConcurrentScopes_DoNotCorruptTheSharedField()
    {
        await using var context = new PipelineContext(PipelineContextConfiguration.Default);

        using (context.ScopedNode("owner"))
        {
            context.NodeEnvironment.NodesRunConcurrently = true;

            using Barrier gate = new(4);

            await Task.WhenAll(Enumerable.Range(0, 4).Select(worker => Task.Run(() =>
            {
                gate.SignalAndWait();

                for (var i = 0; i < 200; i++)
                {
                    using var scope = context.ScopedNode($"worker-{worker}");
                    context.CurrentNodeId.Should().Be("owner");
                }
            })));

            context.NodeEnvironment.NodesRunConcurrently = false;
            context.CurrentNodeId.Should().Be("owner");
        }
    }

    /// <summary>
    ///     End to end: two sinks below a fan-out drain concurrently, and neither leaves the shared field pointing at
    ///     itself afterwards.
    /// </summary>
    [Fact]
    public async Task ConcurrentTerminalDrain_LeavesTheSharedFieldUncorrupted()
    {
        var context = new PipelineContext(PipelineContextConfiguration.Default);

        await PipelineRunner.Create().RunAsync<FanOutPipeline>(context, CancellationToken.None);

        context.NodeEnvironment.NodesRunConcurrently.Should().BeFalse("the freeze must be lifted once the drain completes");
        context.CurrentNodeId.Should().NotBe("first", "a terminal must not leave its own id behind");
        context.CurrentNodeId.Should().NotBe("second");

        await context.DisposeAsync();
    }

    private sealed class RecordingStrategy : IExecutionStrategy
    {
        public string? ObservedNodeId { get; private set; }

        public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
            string nodeId, CancellationToken cancellationToken)
        {
            ObservedNodeId = nodeId;
            return Task.FromResult<IDataStream<TOut>>(new NPipeline.DataFlow.DataStreams.InMemoryDataStream<TOut>([], "empty"));
        }
    }

    private sealed class PassthroughNode : TransformNode<int, int>
    {
        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
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

    private sealed class DrainingSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // The shared field must never name a sibling terminal while both are draining.
                context.CurrentNodeId.Should().NotBe("first").And.NotBe("second");
            }
        }
    }

    private sealed class FanOutPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<CountingSource, int>("source");
            _ = builder.Connect(source, builder.AddSink<DrainingSink, int>("first"));
            _ = builder.Connect(source, builder.AddSink<DrainingSink, int>("second"));
            _ = builder.WithBranchOptions("source", new BranchOptions(16));
        }
    }
}
