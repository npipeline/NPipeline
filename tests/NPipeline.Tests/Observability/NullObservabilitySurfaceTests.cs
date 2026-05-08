using System.Collections.Immutable;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Observability;

public sealed class NullObservabilitySurfaceTests
{
    [Fact]
    public void NullObservabilitySurface_BeginPipeline_ReturnsActivity()
    {
        var surface = NullObservabilitySurface.Instance;
        var ctx = PipelineContext.Default;
        var act = surface.BeginPipeline<TestDefinition>(ctx);
        Assert.NotNull(act);
    }

    [Fact]
    public async Task NullObservabilitySurface_CompletePipeline_IsNoOp()
    {
        var surface = NullObservabilitySurface.Instance;
        var ctx = PipelineContext.Default;
        var act = surface.BeginPipeline<TestDefinition>(ctx);
        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();
        await surface.CompletePipeline<TestDefinition>(ctx, graph, act);
    }

    [Fact]
    public async Task NullObservabilitySurface_FailPipeline_IsNoOp()
    {
        var surface = NullObservabilitySurface.Instance;
        var ctx = PipelineContext.Default;
        var act = surface.BeginPipeline<TestDefinition>(ctx);
        await surface.FailPipeline<TestDefinition>(ctx, new InvalidOperationException("boom"), act);
    }

    [Fact]
    public void NullObservabilitySurface_BeginNode_ReturnsScopeWithNullAutoObservability()
    {
        var surface = NullObservabilitySurface.Instance;
        var ctx = PipelineContext.Default;
        var def = new NodeDefinition(
            new NodeIdentity("n1", "n1"),
            new NodeTypeSystem(typeof(object), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(), new NodeMergeConfig(), new NodeLineageConfig());
        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();
        var scope = surface.BeginNode(ctx, graph, def, new NullNode());
        Assert.Null(scope.AutoObservabilityScope);
    }

    [Fact]
    public void NullObservabilitySurface_CompleteNodeSuccess_ReturnsCompleted()
    {
        var surface = NullObservabilitySurface.Instance;
        var ctx = PipelineContext.Default;
        var def = new NodeDefinition(
            new NodeIdentity("n1", "n1"),
            new NodeTypeSystem(typeof(object), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(), new NodeMergeConfig(), new NodeLineageConfig());
        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();
        var scope = surface.BeginNode(ctx, graph, def, new NullNode());
        var completed = surface.CompleteNodeSuccess(ctx, scope);
        Assert.True(completed.Success);
    }

    [Fact]
    public void NullObservabilitySurface_CompleteNodeFailure_ReturnsFailed()
    {
        var surface = NullObservabilitySurface.Instance;
        var ctx = PipelineContext.Default;
        var def = new NodeDefinition(
            new NodeIdentity("n1", "n1"),
            new NodeTypeSystem(typeof(object), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(), new NodeMergeConfig(), new NodeLineageConfig());
        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();
        var scope = surface.BeginNode(ctx, graph, def, new NullNode());
        var completed = surface.CompleteNodeFailure(ctx, scope, new Exception("fail"));
        Assert.False(completed.Success);
        Assert.NotNull(completed.Error);
    }

    private sealed class TestDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) { }
    }

    private sealed class NullNode : INode
    {
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
