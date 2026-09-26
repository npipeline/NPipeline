using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Execution.Plans;
using NPipeline.Execution.Services;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.State;

namespace NPipeline.Tests.Core.Execution;

[Collection(ProcessWideCounterGroup.Name)]
public sealed class NodeInstantiationServiceTests
{
    [Fact]
    public async Task RegisterStatefulNodes_StatefulNode_RegistersNode()
    {
        await using var context = PipelineContext.CreateDefault();
        var registry = new RecordingStatefulRegistry();
        var node = new StatefulNode();
        context.StatefulRegistry = registry;

        new NodeInstantiationService().RegisterStatefulNodes(new Dictionary<string, INode> { ["stateful"] = node }, context);

        Assert.True(registry.TryGetNode("stateful", out var registered));
        Assert.Same(node, registered);
    }

    [Fact]
    public async Task RegisterStatefulNodes_SimilarlyNamedInterface_DoesNotRegisterNode()
    {
        await using var context = PipelineContext.CreateDefault();
        var registry = new RecordingStatefulRegistry();
        context.StatefulRegistry = registry;

        new NodeInstantiationService().RegisterStatefulNodes(
            new Dictionary<string, INode> { ["impostor"] = new SimilarlyNamedStatefulNode() },
            context);

        Assert.Empty(registry.GetRegisteredNodes());
    }

    [Fact]
    public async Task RegisterStatefulNodes_RegistryFailure_Propagates()
    {
        await using var context = PipelineContext.CreateDefault();
        context.StatefulRegistry = new RecordingStatefulRegistry { ThrowOnRegister = true };

        var action = () => new NodeInstantiationService().RegisterStatefulNodes(
            new Dictionary<string, INode> { ["stateful"] = new StatefulNode() },
            context);

        var exception = Assert.Throws<InvalidOperationException>(action);
        Assert.Equal("Registration failed.", exception.Message);
    }

    [Fact]
    public void BuildPlans_StreamTransformWithNonStreamStrategy_ThrowsClearError()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<TestSourceNode, int>("source");
        var stream = builder.AddStreamTransform<NonStreamStrategyPassthroughNode, int, int>("stream");
        builder.WithExecutionStrategy(stream, new SequentialExecutionStrategy());
        var sink = builder.AddSink<TestSinkNode, int>("sink");
        builder.Connect(source, stream).Connect(stream, sink);

        var graph = builder.Build().Graph;

        var service = new NodeInstantiationService();
        var nodeInstances = service.InstantiateNodes(graph, new DefaultNodeFactory(), new OwnedNodeInstances());

        var ex = Assert.Throws<InvalidOperationException>(() => service.BuildPlans(graph, nodeInstances));
        Assert.Contains("cannot run a stream transform", ex.Message, StringComparison.Ordinal);
        Assert.Contains("IStreamExecutionStrategy", ex.Message, StringComparison.Ordinal);
        Assert.Contains("stream", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildPlans_SameGraphShape_ReusesCompiledDelegates()
    {
        var service = new NodeInstantiationService();

        static Dictionary<string, NodeExecutionPlan> Build(NodeInstantiationService service)
        {
            var builder = new PipelineBuilder().WithoutExtendedValidation();
            var source = builder.AddSource<UniqueSourceNode, UniqueIn>("source");
            var transform = builder.AddTransform<UniqueTransformNode, UniqueIn, UniqueOut>("transform");
            var sink = builder.AddSink<UniqueSinkNode, UniqueOut>("sink");
            builder.Connect(source, transform).Connect(transform, sink);

            var graph = builder.Build().Graph;
            var nodeInstances = service.InstantiateNodes(graph, new DefaultNodeFactory(), new OwnedNodeInstances());
            return service.BuildPlans(graph, nodeInstances);
        }

        _ = Build(service);
        var afterFirst = Volatile.Read(ref NodeInstantiationService.CompilationCount);

        _ = Build(service);
        var afterSecond = Volatile.Read(ref NodeInstantiationService.CompilationCount);

        Assert.Equal(afterFirst, afterSecond);
    }

    private sealed record UniqueIn(int Value);

    private sealed record UniqueOut(int Value);

    private sealed class UniqueSourceNode : SourceNode<UniqueIn>
    {
        public override IDataStream<UniqueIn> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<UniqueIn>(Array.Empty<UniqueIn>().ToAsyncEnumerable(), "unique-source");
    }

    private sealed class UniqueTransformNode : TransformNode<UniqueIn, UniqueOut>
    {
        public override ValueTask<UniqueOut> TransformAsync(UniqueIn item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new UniqueOut(item.Value));
    }

    private sealed class UniqueSinkNode : SinkNode<UniqueOut>
    {
        public override Task ConsumeAsync(IDataStream<UniqueOut> input, PipelineContext context, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class NonStreamStrategyPassthroughNode : IStreamTransformNode<int, int>
    {
        public async IAsyncEnumerable<int> TransformAsync(
            IAsyncEnumerable<int> items,
            PipelineContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var item in items.WithCancellation(cancellationToken))
            {
                yield return item;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TestSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<int>(Array.Empty<int>().ToAsyncEnumerable(), "test-source");
    }

    private sealed class TestSinkNode : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class StatefulNode : IStatefulNode;

    private interface IStatefulNodeLookalike : INode;

    private sealed class SimilarlyNamedStatefulNode : IStatefulNodeLookalike;

    private sealed class RecordingStatefulRegistry : IStatefulRegistry
    {
        private readonly Dictionary<string, object> _nodes = [];

        public bool ThrowOnRegister { get; init; }

        public void Register(string nodeId, object nodeInstance)
        {
            if (ThrowOnRegister)
                throw new InvalidOperationException("Registration failed.");

            _nodes.Add(nodeId, nodeInstance);
        }

        public void Unregister(string nodeId)
        {
            _nodes.Remove(nodeId);
        }

        public IReadOnlyDictionary<string, object> GetRegisteredNodes() => _nodes;

        public bool TryGetNode(string nodeId, out object? nodeInstance) => _nodes.TryGetValue(nodeId, out nodeInstance);
    }
}
