using System.Collections.Immutable;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Observability;

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
    public void PipelineComplete_EmitsBranchMetrics()
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

        surface.CompletePipeline<TestDefinition>(ctx, graph, act);
    }

    [Fact]
    public void PipelineFail_RecordsException()
    {
        var surface = new ObservabilitySurface();
        var ctx = PipelineContext.Default;
        var act = surface.BeginPipeline<TestDefinition>(ctx);
        surface.FailPipeline<TestDefinition>(ctx, new InvalidOperationException("boom"), act);
    }

    [Fact]
    public void NodeSuccess_EmitsObserverEvents()
    {
        var surface = new ObservabilitySurface();
        var observer = new CollectObserver();
        var ctx = PipelineContext.Default;
        ctx.ExecutionObserver = observer;

        var def = new NodeDefinition(
            "n1",
            "n1",
            typeof(DummyNode),
            NodeKind.Source,
            null,
            null,
            null,
            typeof(object));

        var inst = new DummyNode();
        var scope = surface.BeginNode(ctx, def, inst);
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
            "n1",
            "n1",
            typeof(DummyNode),
            NodeKind.Source,
            null,
            null,
            null,
            typeof(object));

        var inst = new DummyNode();
        var scope = surface.BeginNode(ctx, def, inst);
        var completed = surface.CompleteNodeFailure(ctx, scope, new Exception("fail"));
        Assert.Single(observer.Started);
        Assert.Single(observer.Completed);
        Assert.False(completed.Success);
        Assert.NotNull(completed.Error);
    }

    private sealed class TestDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
        }
    }

    private sealed class DummyNode : ISourceNode<object>
    {
        public IDataPipe<object> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataPipes.ListDataPipe<object>(new List<object> { 1, 2, 3 }, "dummy");
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
}
