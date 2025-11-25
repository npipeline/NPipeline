using System.Collections.Immutable;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

public sealed class PipelineRunnerCircuitBreakerTests
{
    private readonly IPipelineExecutionCoordinator _executionCoordinator = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateExecutionCoordinator();
    private readonly IPipelineInfrastructureService _infrastructureService = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateInfrastructureService();
    private readonly INodeFactory _nodeFactory = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateNodeFactory();
    private readonly IObservabilitySurface _observabilitySurface = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateObservabilitySurface();
    private readonly IPipelineFactory _pipelineFactory = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreatePipelineFactory();

    [Fact]
    public async Task RunAsync_NodeFailsAndCircuitBreakerTrips_ThrowsCircuitBreakerTrippedException()
    {
        // Arrange
        var nodeId = "circuitBreakerNode";
        var failingNode = new PipelineRunnerTestHelpers.FailingNode(3); // Fails 3 times
        var nodeDef = PipelineRunnerTestHelpers.NodeDefinitionFactory.CreateSourceNodeDefinition(nodeId);

        var graph = PipelineRunnerTestHelpers.PipelineGraphFactory.CreateGraphWithCircuitBreakerOptions(
            nodeDef,
            new PipelineCircuitBreakerOptions(2, TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(1))); // Trips after 2 failures

        A.CallTo(() => _pipelineFactory.Create<PipelineRunnerTestHelpers.TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(graph));

        A.CallTo(() => _nodeFactory.Create(A<NodeDefinition>._, A<PipelineGraph>._)).Returns(failingNode);

        A.CallTo(() => _executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Returns(new Dictionary<string, INode> { { nodeId, failingNode } });

        A.CallTo(() => _executionCoordinator.BuildInputLookup(A<PipelineGraph>._)).Returns(A.Fake<ILookup<string, Edge>>());
        A.CallTo(() => _executionCoordinator.TopologicalSort(A<PipelineGraph>._)).Returns([nodeId]);

        A.CallTo(() => _infrastructureService.ExecuteWithRetriesAsync(
                A<NodeDefinition>._,
                A<INode>._,
                A<PipelineGraph>._,
                A<PipelineContext>._,
                A<Func<Task>>._,
                A<CancellationToken>._))
            .Throws(new NodeExecutionException(nodeId, "Node execution failed", new CircuitBreakerTrippedException(2, nodeId)));

        var runner = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateRunner(
            _pipelineFactory,
            _nodeFactory,
            _executionCoordinator,
            _infrastructureService,
            _observabilitySurface);

        var context = PipelineContext.Default;

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NodeExecutionException>(() =>
            runner.RunAsync<PipelineRunnerTestHelpers.TestPipelineDefinition>(context));

        exception.NodeId.Should().Be(nodeId);
        exception.InnerException.Should().BeOfType<CircuitBreakerTrippedException>();
        var circuitBreakerTripped = (CircuitBreakerTrippedException)exception.InnerException;
        circuitBreakerTripped.NodeId.Should().Be(nodeId);
        circuitBreakerTripped.FailureThreshold.Should().Be(2);
    }

    [Fact]
    public async Task RunAsync_WithCircuitBreakerMemoryOptions_SurfacesOptionsAndManager()
    {
        // Arrange
        var memoryOptions = new CircuitBreakerMemoryManagementOptions(
            TimeSpan.FromSeconds(2),
            TimeSpan.FromSeconds(5),
            false,
            8).Validate();

        var circuitBreakerOptions = new PipelineCircuitBreakerOptions(1, TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(1));

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .WithCircuitBreakerOptions(circuitBreakerOptions)
            .WithCircuitBreakerMemoryOptions(memoryOptions)
            .Build();

        A.CallTo(() => _pipelineFactory.Create<PipelineRunnerTestHelpers.TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(graph));

        A.CallTo(() => _executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Returns(new Dictionary<string, INode>());

        A.CallTo(() => _executionCoordinator.BuildPlans(A<PipelineGraph>._, A<IReadOnlyDictionary<string, INode>>._))
            .Returns(new Dictionary<string, NodeExecutionPlan>());

        A.CallTo(() => _executionCoordinator.BuildInputLookup(A<PipelineGraph>._))
            .Returns(A.Fake<ILookup<string, Edge>>());

        A.CallTo(() => _executionCoordinator.TopologicalSort(A<PipelineGraph>._))
            .Returns(new List<string>());

        var runner = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateRunner(
            _pipelineFactory,
            _nodeFactory,
            _executionCoordinator,
            _infrastructureService,
            _observabilitySurface);

        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<PipelineRunnerTestHelpers.TestPipelineDefinition>(context);

        // Assert
        _ = context.Items.Should().ContainKey(PipelineContextKeys.CircuitBreakerMemoryOptions)
            .WhoseValue.Should().BeSameAs(memoryOptions);

        _ = context.Items.Should().ContainKey(PipelineContextKeys.CircuitBreakerManager)
            .WhoseValue.Should().BeOfType<CircuitBreakerManager>();
    }
}
