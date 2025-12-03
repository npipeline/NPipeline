using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

/// <summary>
///     Unit tests for PipelineRunner error handling behavior.
///     Tests retry logic, cancellation handling, and exception wrapping.
/// </summary>
public sealed class PipelineRunnerTests
{
    private readonly IPipelineExecutionCoordinator _executionCoordinator = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateExecutionCoordinator();
    private readonly IPipelineInfrastructureService _infrastructureService = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateInfrastructureService();
    private readonly INodeFactory _nodeFactory = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateNodeFactory();
    private readonly IObservabilitySurface _observabilitySurface = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateObservabilitySurface();
    private readonly IPipelineFactory _pipelineFactory = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreatePipelineFactory();

    #region Retry Tests

    [Fact]
    public async Task RunAsync_NodeFailsAndRetriesExhausted_ThrowsRetryExhaustedException()
    {
        // Arrange
        var nodeId = "failingNode";
        var failingNode = new PipelineRunnerTestHelpers.FailingNode(3); // Fails 3 times
        var nodeDef = PipelineRunnerTestHelpers.NodeDefinitionFactory.CreateSourceNodeDefinition(nodeId);

        var graph = PipelineRunnerTestHelpers.PipelineGraphFactory.CreateGraphWithRetryOptions(
            nodeDef,
            new PipelineRetryOptions(MaxNodeRestartAttempts: 3, MaxSequentialNodeAttempts: 3, MaxItemRetries: 0));

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
            .Throws(new NodeExecutionException(nodeId, "Node execution failed",
                new RetryExhaustedException(nodeId, 3, new InvalidOperationException("Simulated failure"))));

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
        exception.InnerException.Should().BeOfType<RetryExhaustedException>();
        var retryExhausted = (RetryExhaustedException)exception.InnerException;
        retryExhausted.AttemptCount.Should().Be(3);
        retryExhausted.InnerException.Should().BeOfType<InvalidOperationException>();
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public async Task RunAsync_CancellationRequested_ThrowsOperationCanceledException()
    {
        // Arrange
        var nodeId = "cancellationNode";
        var nodeDef = PipelineRunnerTestHelpers.NodeDefinitionFactory.CreateSourceNodeDefinition(nodeId);
        var graph = PipelineRunnerTestHelpers.PipelineGraphFactory.CreateSimpleGraph(nodeDef);

        A.CallTo(() => _pipelineFactory.Create<PipelineRunnerTestHelpers.TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(graph));

        A.CallTo(() => _executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Returns(new Dictionary<string, INode> { { nodeId, A.Fake<INode>() } });

        A.CallTo(() => _executionCoordinator.BuildInputLookup(A<PipelineGraph>._)).Returns(A.Fake<ILookup<string, Edge>>());
        A.CallTo(() => _executionCoordinator.TopologicalSort(A<PipelineGraph>._)).Returns([nodeId]);

        A.CallTo(() => _infrastructureService.ExecuteWithRetriesAsync(
                A<NodeDefinition>._,
                A<INode>._,
                A<PipelineGraph>._,
                A<PipelineContext>._,
                A<Func<Task>>._,
                A<CancellationToken>._))
            .Throws(new OperationCanceledException());

        var runner = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateRunner(
            _pipelineFactory,
            _nodeFactory,
            _executionCoordinator,
            _infrastructureService,
            _observabilitySurface);

        var context = PipelineContext.Default;
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        // Act & Assert
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            runner.RunAsync<PipelineRunnerTestHelpers.TestPipelineDefinition>(context, cts.Token));
    }

    [Fact]
    public async Task RunAsync_NodeThrowsException_WrapsInPipelineExecutionException()
    {
        // Arrange
        var nodeId = "exceptionNode";
        var nodeDef = PipelineRunnerTestHelpers.NodeDefinitionFactory.CreateSourceNodeDefinition(nodeId);
        var graph = PipelineRunnerTestHelpers.PipelineGraphFactory.CreateSimpleGraph(nodeDef);

        A.CallTo(() => _pipelineFactory.Create<PipelineRunnerTestHelpers.TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(graph));

        A.CallTo(() => _executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Returns(new Dictionary<string, INode> { { nodeId, A.Fake<INode>() } });

        A.CallTo(() => _executionCoordinator.BuildInputLookup(A<PipelineGraph>._)).Returns(A.Fake<ILookup<string, Edge>>());
        A.CallTo(() => _executionCoordinator.TopologicalSort(A<PipelineGraph>._)).Returns([nodeId]);

        A.CallTo(() => _infrastructureService.ExecuteWithRetriesAsync(
                A<NodeDefinition>._,
                A<INode>._,
                A<PipelineGraph>._,
                A<PipelineContext>._,
                A<Func<Task>>._,
                A<CancellationToken>._))
            .Throws(new InvalidOperationException("Node execution failed"));

        var runner = PipelineRunnerTestHelpers.PipelineRunnerMockFactory.CreateRunner(
            _pipelineFactory,
            _nodeFactory,
            _executionCoordinator,
            _infrastructureService,
            _observabilitySurface);

        var context = PipelineContext.Default;

        // Act & Assert
        var exception = await Assert.ThrowsAsync<PipelineExecutionException>(() =>
            runner.RunAsync<PipelineRunnerTestHelpers.TestPipelineDefinition>(context));

        exception.Message.Should().Contain($"Pipeline execution failed at node '{nodeId}'");
        exception.InnerException.Should().BeOfType<InvalidOperationException>();
        exception.InnerException!.Message.Should().Be("Node execution failed");
    }

    #endregion
}
