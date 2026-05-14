using System.Collections.Immutable;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Caching;
using NPipeline.Execution.Orchestration;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;
using NPipeline.State;

namespace NPipeline.Tests.Core.Execution;

public sealed class PipelineExecutionSetupStageTests
{
    [Fact]
    public async Task PrepareAsync_UsesRuntimeBoundGraphAndCopiesRuntimeBindings()
    {
        // Arrange
        var nodeFactory = A.Fake<INodeFactory>();
        var nodeInstantiationService = A.Fake<INodeInstantiationService>();
        var runtimeBinder = A.Fake<IRuntimePipelineBinder>();

        var baseGraph = BuildGraph("base-node");
        var boundGraph = BuildGraph("bound-node");
        var nodeInstances = new Dictionary<string, INode> { ["bound-node"] = A.Fake<INode>() };
        var plans = new Dictionary<string, NodeExecutionPlan>();

        var deadLetterSink = A.Fake<IDeadLetterSink>();
        var lineageSink = A.Fake<ILineageSink>();
        var pipelineLineageSink = A.Fake<IPipelineLineageSink>();
        var resiliencePolicy = A.Fake<IResiliencePolicy>();

        _ = A.CallTo(() => runtimeBinder.BindAsync(baseGraph, A<PipelineContext>._))
            .Returns(new RuntimePipelineBindingResult(
                boundGraph,
                deadLetterSink,
                lineageSink,
                pipelineLineageSink,
                resiliencePolicy));

        _ = A.CallTo(() => nodeInstantiationService.InstantiateNodes(boundGraph, nodeFactory))
            .Returns(nodeInstances);

        _ = A.CallTo(() => nodeInstantiationService.BuildPlans(
            A<PipelineGraph>._,
            A<IReadOnlyDictionary<string, INode>>._))
            .Returns(plans);

        var stage = new PipelineExecutionSetupStage(
            nodeFactory,
            nodeInstantiationService,
            NullPipelineExecutionPlanCache.Instance,
            runtimeBinder);

        var context = PipelineContext.Default;

        // Act
        var result = await stage.PrepareAsync(typeof(PipelineExecutionSetupStageTests), baseGraph, context, CancellationToken.None);

        // Assert
        _ = result.Graph.Nodes.Should().HaveCount(1);
        _ = result.Graph.Nodes[0].Id.Should().Be("bound-node");
        _ = result.Graph.NodeDefinitionMap.Should().ContainKey("bound-node");
        _ = result.NodeInstances.Should().BeSameAs(nodeInstances);
        _ = result.ExecutionPlans.Should().BeEmpty();
        _ = result.PipelineLineageSink.Should().BeSameAs(pipelineLineageSink);

        _ = context.DeadLetterSink.Should().BeSameAs(deadLetterSink);
        _ = context.LineageSink.Should().BeSameAs(lineageSink);
        _ = context.PipelineLineageSink.Should().BeSameAs(pipelineLineageSink);
        _ = context.ResiliencePolicy.Should().BeSameAs(resiliencePolicy);

        _ = A.CallTo(() => nodeInstantiationService.InstantiateNodes(boundGraph, nodeFactory))
            .MustHaveHappenedOnceExactly();

        _ = A.CallTo(() => nodeInstantiationService.BuildPlans(
            A<PipelineGraph>._,
            A<IReadOnlyDictionary<string, INode>>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task PrepareAsync_ProjectsGlobalAnnotationsAndContextServices()
    {
        // Arrange
        var nodeFactory = A.Fake<INodeFactory>();
        var nodeInstantiationService = A.Fake<INodeInstantiationService>();
        var runtimeBinder = A.Fake<IRuntimePipelineBinder>();

        var executionObserver = A.Fake<IExecutionObserver>();
        var stateManager = A.Fake<IPipelineStateManager>();
        var statefulRegistry = A.Fake<IStatefulRegistry>();

        var graph = BuildGraph(
            "node-a",
            new Dictionary<string, object>
            {
                [ExecutionAnnotationKeys.GlobalExecutionObserver] = executionObserver,
                [$"{ExecutionAnnotationKeys.GlobalAnnotationPrefix}NPipeline.StateManager"] = stateManager,
            });

        var nodeInstances = new Dictionary<string, INode> { ["node-a"] = A.Fake<INode>() };
        var plans = new Dictionary<string, NodeExecutionPlan>();

        _ = A.CallTo(() => runtimeBinder.BindAsync(graph, A<PipelineContext>._))
            .Returns(new RuntimePipelineBindingResult(graph, null, null, null, DefaultResiliencePolicy.Instance));

        _ = A.CallTo(() => nodeInstantiationService.InstantiateNodes(graph, nodeFactory))
            .Returns(nodeInstances);

        _ = A.CallTo(() => nodeInstantiationService.BuildPlans(
                graph,
                A<IReadOnlyDictionary<string, INode>>._))
            .Returns(plans);

        var stage = new PipelineExecutionSetupStage(
            nodeFactory,
            nodeInstantiationService,
            NullPipelineExecutionPlanCache.Instance,
            runtimeBinder);

        var context = PipelineContext.Default;
        context.Properties["NPipeline.Global.NPipeline.State.StatefulRegistry"] = statefulRegistry;

        // Act
        _ = await stage.PrepareAsync(typeof(PipelineExecutionSetupStageTests), graph, context, CancellationToken.None);

        // Assert
        _ = context.ExecutionObserver.Should().BeSameAs(executionObserver);
        _ = context.StateManager.Should().BeSameAs(stateManager);
        _ = context.StatefulRegistry.Should().BeSameAs(statefulRegistry);
        _ = context.Properties.Should().ContainKey(ExecutionAnnotationKeys.ExecutionObserverProperty);
        _ = context.Properties[ExecutionAnnotationKeys.ExecutionObserverProperty].Should().BeSameAs(executionObserver);

        _ = A.CallTo(() => nodeInstantiationService.RegisterStatefulNodes(nodeInstances, context))
            .MustHaveHappenedOnceExactly();
    }

    private static PipelineGraph BuildGraph(string nodeId, Dictionary<string, object>? annotations = null)
    {
        return PipelineGraphBuilder.Create()
            .WithNodes([CreateSourceNodeDefinition(nodeId)])
            .WithEdges([])
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithNodeExecutionAnnotations(annotations)
            .Build();
    }

    private static NodeDefinition CreateSourceNodeDefinition(string nodeId)
    {
        return new NodeDefinition(
            new NodeIdentity(nodeId, nodeId),
            new NodeTypeSystem(typeof(FakeSourceNode), NodeKind.Source, null, typeof(object)),
            new NodeExecutionConfig(),
            new NodeMergeConfig(),
            new NodeLineageConfig());
    }

    private sealed class FakeSourceNode : ISourceNode<object>
    {
        public IDataStream<object> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataStream<object>([], "fake-source");
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
