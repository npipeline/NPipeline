using System.Collections.Immutable;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Execution.Caching;
using NPipeline.Execution.Plans;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Execution;

public sealed class PipelineRunnerLineageOverrideTests
{
    [Fact]
    public async Task RunAsync_ItemLevelLineageOverrideTrue_EnablesLineageWithStudioDefaults()
    {
        // Arrange
        var pipelineFactory = A.Fake<IPipelineFactory>();
        var nodeFactory = A.Fake<INodeFactory>();
        var executionCoordinator = A.Fake<IPipelineExecutionCoordinator>();
        var infrastructureService = A.Fake<IPipelineInfrastructureService>();
        var observabilitySurface = new ObservabilitySurface();

        var baseGraph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableArray<NodeDefinition>.Empty)
            .WithEdges(ImmutableArray<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithItemLevelLineageEnabled(false)
            .Build();

        _ = A.CallTo(() => pipelineFactory.Create<TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(baseGraph));

        PipelineGraph? observedGraph = null;
        _ = A.CallTo(() => executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Invokes((PipelineGraph graph, INodeFactory _) => observedGraph = graph)
            .Returns(new Dictionary<string, INode>());

        _ = A.CallTo(() => executionCoordinator.BuildPlansWithCache(
                typeof(TestPipelineDefinition),
                A<PipelineGraph>._,
                A<Dictionary<string, INode>>._,
                A<IPipelineExecutionPlanCache>._))
            .Returns(new Dictionary<string, NodeExecutionPlan>());

        _ = A.CallTo(() => executionCoordinator.BuildInputLookup(A<PipelineGraph>._))
            .Returns(A.Fake<ILookup<string, Edge>>());

        _ = A.CallTo(() => executionCoordinator.TopologicalSort(A<PipelineGraph>._))
            .Returns([]);

        var runner = new PipelineRunnerBuilder()
            .WithPipelineFactory(pipelineFactory)
            .WithNodeFactory(nodeFactory)
            .WithExecutionCoordinator(executionCoordinator)
            .WithInfrastructureService(infrastructureService)
            .WithObservabilitySurface(observabilitySurface)
            .Build();

        var context = PipelineContext.Default;
        context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = true;

        // Act
        await runner.RunAsync<TestPipelineDefinition>(context);

        // Assert
        _ = observedGraph.Should().NotBeNull();
        _ = observedGraph!.Lineage.ItemLevelLineageEnabled.Should().BeTrue();
        _ = observedGraph.Lineage.LineageOptions.Should().NotBeNull();
        _ = observedGraph.Lineage.LineageOptions!.SampleEvery.Should().Be(1);
        _ = observedGraph.Lineage.LineageOptions.RedactData.Should().BeFalse();
    }

    [Fact]
    public async Task RunAsync_ItemLevelLineageOverrideFalse_DisablesConfiguredLineage()
    {
        // Arrange
        var pipelineFactory = A.Fake<IPipelineFactory>();
        var nodeFactory = A.Fake<INodeFactory>();
        var executionCoordinator = A.Fake<IPipelineExecutionCoordinator>();
        var infrastructureService = A.Fake<IPipelineInfrastructureService>();
        var observabilitySurface = new ObservabilitySurface();

        var baseGraph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableArray<NodeDefinition>.Empty)
            .WithEdges(ImmutableArray<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithItemLevelLineageEnabled(true)
            .WithLineageOptions(new LineageOptions(SampleEvery: 3, RedactData: true))
            .Build();

        _ = A.CallTo(() => pipelineFactory.Create<TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(baseGraph));

        PipelineGraph? observedGraph = null;
        _ = A.CallTo(() => executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Invokes((PipelineGraph graph, INodeFactory _) => observedGraph = graph)
            .Returns(new Dictionary<string, INode>());

        _ = A.CallTo(() => executionCoordinator.BuildPlansWithCache(
                typeof(TestPipelineDefinition),
                A<PipelineGraph>._,
                A<Dictionary<string, INode>>._,
                A<IPipelineExecutionPlanCache>._))
            .Returns(new Dictionary<string, NodeExecutionPlan>());

        _ = A.CallTo(() => executionCoordinator.BuildInputLookup(A<PipelineGraph>._))
            .Returns(A.Fake<ILookup<string, Edge>>());

        _ = A.CallTo(() => executionCoordinator.TopologicalSort(A<PipelineGraph>._))
            .Returns([]);

        var runner = new PipelineRunnerBuilder()
            .WithPipelineFactory(pipelineFactory)
            .WithNodeFactory(nodeFactory)
            .WithExecutionCoordinator(executionCoordinator)
            .WithInfrastructureService(infrastructureService)
            .WithObservabilitySurface(observabilitySurface)
            .Build();

        var context = PipelineContext.Default;
        context.Properties[PipelineContextKeys.ItemLevelLineageEnabledOverride] = false;

        // Act
        await runner.RunAsync<TestPipelineDefinition>(context);

        // Assert
        _ = observedGraph.Should().NotBeNull();
        _ = observedGraph!.Lineage.ItemLevelLineageEnabled.Should().BeFalse();
        _ = observedGraph.Lineage.LineageOptions.Should().NotBeNull();
        _ = observedGraph.Lineage.LineageOptions!.SampleEvery.Should().Be(3);
        _ = observedGraph.Lineage.LineageOptions.RedactData.Should().BeTrue();
    }

    private sealed class TestPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            // No-op definition used by runner tests.
        }
    }
}
