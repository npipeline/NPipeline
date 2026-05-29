using System.Collections.Immutable;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Configuration;
using NPipeline.Observability.Metrics;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Observability.Tests;

public sealed class ObservabilitySurfaceTests
{
    [Fact]
    public void PipelineBegin_RecordsStart()
    {
        var surface = new ObservabilitySurface();
        var ctx = PipelineContext.Default;
        var act = surface.BeginPipeline<TestDefinition>(ctx);
        Assert.NotNull(act);
    }

    [Fact]
    public async Task PipelineComplete_EmitsBranchMetrics()
    {
        var surface = new ObservabilitySurface();
        var ctx = PipelineContext.Default;
        var metricsKey = ExecutionAnnotationKeys.BranchMetricsForNode("node1");
        ctx.Items[metricsKey] = new BranchMetrics();
        var act = surface.BeginPipeline<TestDefinition>(ctx);

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        await surface.CompletePipeline<TestDefinition>(ctx, graph, act);
    }

    [Fact]
    public async Task PipelineFail_RecordsException()
    {
        var surface = new ObservabilitySurface();
        var ctx = PipelineContext.Default;
        var act = surface.BeginPipeline<TestDefinition>(ctx);
        await surface.FailPipeline<TestDefinition>(ctx, new InvalidOperationException("boom"), act);
    }

    [Fact]
    public void NodeSuccess_EmitsObserverEvents()
    {
        var surface = new ObservabilitySurface();
        var observer = new CollectObserver();
        var ctx = PipelineContext.Default;
        ctx.ExecutionObserver = observer;

        var def = new NodeDefinition(
            new NodeIdentity("n1", "n1"),
            new NodeTypeSystem(typeof(DummyNode), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(),
            new NodeMergeConfig(),
            new NodeLineageConfig());

        var inst = new DummyNode();

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        var scope = surface.BeginNode(ctx, graph, def, inst);
        var completed = surface.CompleteNodeSuccess(ctx, scope);
        Assert.Single(observer.Started);
        Assert.Single(observer.Completed);
        Assert.True(completed.Success);
    }

    [Fact]
    public void NodeFailure_EmitsFailureEvent()
    {
        var surface = new ObservabilitySurface();
        var observer = new CollectObserver();
        var ctx = PipelineContext.Default;
        ctx.ExecutionObserver = observer;

        var def = new NodeDefinition(
            new NodeIdentity("n1", "n1"),
            new NodeTypeSystem(typeof(DummyNode), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(),
            new NodeMergeConfig(),
            new NodeLineageConfig());

        var inst = new DummyNode();

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        var scope = surface.BeginNode(ctx, graph, def, inst);
        var completed = surface.CompleteNodeFailure(ctx, scope, new Exception("fail"));
        Assert.Single(observer.Started);
        Assert.Single(observer.Completed);
        Assert.False(completed.Success);
        Assert.NotNull(completed.Error);
    }

    [Fact]
    public void NodeSuccess_WithAutoObservabilityScope_EmitsDataflowCompletionOnScopeDispose()
    {
        var collector = new ObservabilityCollector(new FixedCollectorFactory(null!));
        var factory = new FixedCollectorFactory(collector);
        var surface = new ObservabilitySurface();
        var observer = new CollectObserver();
        var ctx = new PipelineContext(PipelineContextConfiguration.WithFactories(observabilityFactory: factory));
        ctx.ExecutionObserver = observer;

        var def = new NodeDefinition(
            new NodeIdentity("n1", "n1"),
            new NodeTypeSystem(typeof(DummyNode), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(),
            new NodeMergeConfig(),
            new NodeLineageConfig());

        var inst = new DummyNode();

        var annotations = ImmutableDictionary<string, object>.Empty
            .Add("NPipeline.Observability.Options:n1", ObservabilityOptions.Default);

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithNodeExecutionAnnotations(annotations)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        var scope = surface.BeginNode(ctx, graph, def, inst);
        var completed = surface.CompleteNodeSuccess(ctx, scope);

        Assert.Single(observer.Started);
        Assert.Single(observer.Completed);
        Assert.Empty(observer.DataflowCompleted);
        Assert.True(completed.Success);

        using (var streamScope = ctx.NodeExecutionScopeRegistry.BeginNodeScope("n1"))
        {
            streamScope.IncrementProcessed();
        }

        Assert.Single(observer.DataflowCompleted);
        Assert.Equal("n1", observer.DataflowCompleted[0].NodeId);
        Assert.True(observer.DataflowCompleted[0].Success);
    }

    private sealed class TestDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
        }
    }

    private sealed class DummyNode : ISourceNode<object>
    {
        public IDataStream<object> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<object>(new List<object> { 1, 2, 3 }, "dummy");
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CollectObserver : IExecutionObserver
    {
        public List<NodeExecutionStarted> Started { get; } = [];
        public List<NodeExecutionCompleted> Completed { get; } = [];
        public List<NodeDataflowCompleted> DataflowCompleted { get; } = [];
        public List<NodeRetryEvent> Retries { get; } = [];
        public List<QueueDropEvent> Drops { get; } = [];
        public List<QueueMetricsEvent> Metrics { get; } = [];

        public void OnNodeStarted(NodeExecutionStarted e)
        {
            Started.Add(e);
        }

        public void OnNodeCompleted(NodeExecutionCompleted e)
        {
            Completed.Add(e);
        }

        public void OnNodeDataflowCompleted(NodeDataflowCompleted e)
        {
            DataflowCompleted.Add(e);
        }

        public void OnRetry(NodeRetryEvent e)
        {
            Retries.Add(e);
        }

        public void OnDrop(QueueDropEvent e)
        {
            Drops.Add(e);
        }

        public void OnQueueMetrics(QueueMetricsEvent e)
        {
            Metrics.Add(e);
        }
    }

    private sealed class FixedCollectorFactory(IObservabilityCollector? collector) : IObservabilityFactory
    {
        private readonly IObservabilityCollector? _collector = collector;

        public IObservabilityCollector ResolveObservabilityCollector()
        {
            return _collector ?? throw new NotSupportedException();
        }

        public IMetricsSink? ResolveMetricsSink()
        {
            throw new NotSupportedException();
        }

        public IPipelineMetricsSink? ResolvePipelineMetricsSink()
        {
            throw new NotSupportedException();
        }
    }
}
