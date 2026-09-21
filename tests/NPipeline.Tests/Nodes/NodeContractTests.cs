using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes;

/// <summary>
///     Guards the node contract after lifecycle and execution strategy were taken off it.
///     <para>
///         A node used to be forced to carry a <c>DisposeAsync</c> it had nothing to do with, and the framework used to
///         write the configured execution strategy onto the node instance during setup. Mutating a user object at setup
///         makes a node unsafe to share or reuse and leaves the configuration in two places — the
///         <see cref="NPipeline.Graph.NodeDefinition" /> and the instance — that can disagree.
///     </para>
/// </summary>
public sealed class NodeContractTests
{
    [Fact]
    public void INode_DoesNotRequireDisposal()
    {
        typeof(IAsyncDisposable).IsAssignableFrom(typeof(INode))
            .Should().BeFalse("a node declares its own lifecycle; it does not inherit one");
    }

    [Fact]
    public void TransformNodes_CarryNoExecutionStrategyOfTheirOwn()
    {
        typeof(ITransformNode).GetProperty("ExecutionStrategy").Should().BeNull();
        typeof(IStreamTransformNode).GetProperty("ExecutionStrategy").Should().BeNull();

        // The provider property a node may opt into is read-only, so the framework cannot write to it.
        var provider = typeof(IExecutionStrategyProvider).GetProperty(nameof(IExecutionStrategyProvider.DefaultExecutionStrategy));
        provider.Should().NotBeNull();
        provider!.CanWrite.Should().BeFalse();
    }

    [Fact]
    public async Task ConfiguredStrategy_RunsTheNode_WithoutMutatingIt()
    {
        var node = new PassthroughNode();
        var configured = new RecordingStrategy();

        await RunAsync(builder =>
        {
            var handle = builder.AddTransform<PassthroughNode, int, int>("transform");
            _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
            _ = handle.WithExecutionStrategy(builder, configured);
            return handle;
        });

        configured.Executions.Should().Be(1, "the strategy on the node definition is the one that runs");
        node.DefaultExecutionStrategy.Should().BeSameAs(PassthroughNode.NodeDefault, "the run must not write back to the node");
        PassthroughNode.NodeDefault.Executions.Should().Be(0);
    }

    [Fact]
    public async Task NodeDefault_RunsWhenTheGraphConfiguresNoStrategy()
    {
        var node = new PassthroughNode();
        PassthroughNode.NodeDefault.Reset();

        await RunAsync(builder =>
        {
            var handle = builder.AddTransform<PassthroughNode, int, int>("transform");
            _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
            return handle;
        });

        PassthroughNode.NodeDefault.Executions.Should().Be(1, "a node's own default applies when the graph configures none");
    }

    [Fact]
    public async Task SequentialExecution_IsTheFallbackForAPlainNode()
    {
        var sink = new CollectingSink();

        await RunAsync(builder => builder.AddTransform<DoublingNode, int, int>("transform"), sink);

        sink.Items.Should().Equal([2, 4, 6], "a node with no strategy anywhere still runs, one item at a time");
    }

    [Fact]
    public async Task ANodeThatOwnsNothing_IsNeverDisposed_AndANodeThatDoesIsDisposedOnce()
    {
        var disposable = new DisposableTransform();
        var sink = new CollectingSink();

        await RunAsync(builder =>
        {
            var handle = builder.AddTransform<DisposableTransform, int, int>("transform");
            _ = builder.AddPreconfiguredNodeInstance(handle.Id, disposable);
            return handle;
        }, sink);

        disposable.DisposeCount.Should().Be(1, "a node that opts into IAsyncDisposable is still disposed by the run");
        sink.Items.Should().Equal([1, 2, 3]);
    }

    [Fact]
    public async Task ASynchronouslyDisposableNode_IsDisposedToo()
    {
        var disposable = new SyncDisposableTransform();

        await RunAsync(builder =>
        {
            var handle = builder.AddTransform<SyncDisposableTransform, int, int>("transform");
            _ = builder.AddPreconfiguredNodeInstance(handle.Id, disposable);
            return handle;
        });

        disposable.DisposeCount.Should().Be(1);
    }

    private static async Task RunAsync(
        Func<PipelineBuilder, TransformNodeHandle<int, int>> addTransform,
        CollectingSink? sink = null)
    {
        sink ??= new CollectingSink();
        var runner = PipelineRunner.Create();
        await using var context = PipelineContext.CreateDefault();
        await runner.RunAsync(new InlinePipeline(addTransform, sink), context, CancellationToken.None);
    }

    private sealed class InlinePipeline(
        Func<PipelineBuilder, TransformNodeHandle<int, int>> addTransform,
        CollectingSink sink) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<NumbersSource, int>("source");
            var transform = addTransform(builder);
            var sinkHandle = builder.AddSink<CollectingSink, int>("sink");
            _ = builder.AddPreconfiguredNodeInstance(sinkHandle.Id, sink);

            builder.Connect(source, transform);
            builder.Connect(transform, sinkHandle);
        }
    }

    private sealed class NumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1, 2, 3], "numbers");
        }
    }

    private sealed class CollectingSink : SinkNode<int>
    {
        public List<int> Items { get; } = [];

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                Items.Add(item);
            }
        }
    }

    private sealed class DoublingNode : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(item * 2);
        }
    }

    private sealed class PassthroughNode : TransformNode<int, int>, IExecutionStrategyProvider
    {
        public static RecordingStrategy NodeDefault { get; } = new();

        public IExecutionStrategy DefaultExecutionStrategy => NodeDefault;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(item);
        }
    }

    private sealed class DisposableTransform : TransformNode<int, int>, IAsyncDisposable
    {
        public int DisposeCount { get; private set; }

        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return ValueTask.CompletedTask;
        }

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(item);
        }
    }

    private sealed class SyncDisposableTransform : TransformNode<int, int>, IDisposable
    {
        public int DisposeCount { get; private set; }

        public void Dispose()
        {
            DisposeCount++;
        }

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(item);
        }
    }

    /// <summary>
    ///     A strategy that records how often it ran and otherwise delegates to sequential execution.
    /// </summary>
    private sealed class RecordingStrategy : IExecutionStrategy
    {
        private readonly SequentialExecutionStrategy _inner = new();

        public int Executions { get; private set; }

        public void Reset()
        {
            Executions = 0;
        }

        public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
            IDataStream<TIn> input,
            ITransformNode<TIn, TOut> node,
            PipelineContext context,
            string nodeId,
            CancellationToken cancellationToken)
        {
            Executions++;
            return _inner.ExecuteAsync(input, node, context, nodeId, cancellationToken);
        }
    }
}
