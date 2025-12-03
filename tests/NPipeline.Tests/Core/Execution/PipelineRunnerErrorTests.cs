using System.Collections.Immutable;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Execution;

public sealed class PipelineRunnerErrorTests
{
    private readonly IPipelineExecutionCoordinator _executionCoordinator = A.Fake<IPipelineExecutionCoordinator>();
    private readonly IPipelineInfrastructureService _infrastructureService = A.Fake<IPipelineInfrastructureService>();
    private readonly INodeFactory _nodeFactory = A.Fake<INodeFactory>();
    private readonly IObservabilitySurface _observabilitySurface = A.Fake<IObservabilitySurface>();
    private readonly IPipelineFactory _pipelineFactory = A.Fake<IPipelineFactory>();

    [Fact]
    public async Task RunAsync_NodeFailsAndRetriesExhausted_ThrowsRetryExhaustedException()
    {
        // Arrange
        var nodeId = "failingNode";
        var failingNode = new FailingNode(3); // Fails 3 times

        var nodeDef = new NodeDefinition(
            nodeId, nodeId, typeof(FailingNode), NodeKind.Source,
            null, null, typeof(object));

        var graph = PipelineGraphBuilder.Create()
            .WithNodes([nodeDef])
            .WithEdges([])
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        _ = A.CallTo(() => _pipelineFactory.Create<TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(graph));
        _ = A.CallTo(() => _nodeFactory.Create(A<NodeDefinition>._, A<PipelineGraph>._)).Returns(failingNode);

        _ = A.CallTo(() => _executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Returns(new Dictionary<string, INode> { { nodeId, failingNode } });

        _ = A.CallTo(() => _executionCoordinator.BuildInputLookup(A<PipelineGraph>._)).Returns(A.Fake<ILookup<string, Edge>>());
        _ = A.CallTo(() => _executionCoordinator.TopologicalSort(A<PipelineGraph>._)).Returns([nodeId]);

        _ = A.CallTo(() => _infrastructureService.ExecuteWithRetriesAsync(
                A<NodeDefinition>._,
                A<INode>._,
                A<PipelineGraph>._,
                A<PipelineContext>._,
                A<Func<Task>>._,
                A<CancellationToken>._))
            .Throws(new NodeExecutionException(nodeId, "Node execution failed",
                new RetryExhaustedException(nodeId, 3, new InvalidOperationException("Simulated failure"))));

        var runner = new PipelineRunnerBuilder()
            .WithPipelineFactory(_pipelineFactory)
            .WithNodeFactory(_nodeFactory)
            .WithExecutionCoordinator(_executionCoordinator)
            .WithInfrastructureService(_infrastructureService)
            .WithObservabilitySurface(_observabilitySurface)
            .Build();

        var context = PipelineContext.Default;

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NodeExecutionException>(() =>
            runner.RunAsync<TestPipelineDefinition>(context));

        _ = exception.NodeId.Should().Be(nodeId);
        _ = exception.InnerException.Should().BeOfType<RetryExhaustedException>();
        var retryExhausted = (RetryExhaustedException)exception.InnerException;
        _ = retryExhausted.AttemptCount.Should().Be(3);
        _ = retryExhausted.InnerException.Should().BeOfType<InvalidOperationException>();
    }

    [Fact]
    public async Task RunAsync_CancellationTokenCancelled_ThrowsOperationCanceledException()
    {
        // Arrange
        var nodeId = "cancellableNode";

        var nodeDef = new NodeDefinition(
            nodeId, nodeId, typeof(FailingNode), NodeKind.Source, // Using FailingNode but it won't actually fail
            null, null, null, typeof(object));

        var graph = new PipelineGraph
        {
            Nodes = [nodeDef],
            Edges = [],
            PreconfiguredNodeInstances = ImmutableDictionary<string, INode>.Empty,
        };

        var cts = new CancellationTokenSource();
        var failingNode = new FailingNode(0); // This node will not fail on its own

        _ = A.CallTo(() => _pipelineFactory.Create<TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(graph));
        _ = A.CallTo(() => _nodeFactory.Create(A<NodeDefinition>._, A<PipelineGraph>._)).Returns(failingNode);

        _ = A.CallTo(() => _executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Returns(new Dictionary<string, INode> { { nodeId, failingNode } });

        _ = A.CallTo(() => _executionCoordinator.BuildInputLookup(A<PipelineGraph>._)).Returns(A.Fake<ILookup<string, Edge>>());
        _ = A.CallTo(() => _executionCoordinator.TopologicalSort(A<PipelineGraph>._)).Returns([nodeId]);

        _ = A.CallTo(() => _infrastructureService.ExecuteWithRetriesAsync(
                A<NodeDefinition>._,
                A<INode>._,
                A<PipelineGraph>._,
                A<PipelineContext>._,
                A<Func<Task>>._,
                A<CancellationToken>._))
            .ReturnsLazily(async (NodeDefinition nd, INode ni, PipelineGraph pg, PipelineContext pc, Func<Task> execBody, CancellationToken ct) =>
            {
                // Simulate cancellation during execution
                await cts.CancelAsync();
                ct.ThrowIfCancellationRequested();
                await execBody(); // This should not be reached
            });

        var runner = new PipelineRunnerBuilder()
            .WithPipelineFactory(_pipelineFactory)
            .WithNodeFactory(_nodeFactory)
            .WithExecutionCoordinator(_executionCoordinator)
            .WithInfrastructureService(_infrastructureService)
            .WithObservabilitySurface(_observabilitySurface)
            .Build();

        var context = new PipelineContextBuilder()
            .WithCancellation(cts.Token)
            .Build();

        // Act & Assert
        _ = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            runner.RunAsync<TestPipelineDefinition>(context, cts.Token));
    }

    [Fact]
    public async Task RunAsync_NodeExecutionException_WrapsCorrectly()
    {
        // Arrange
        var nodeId = "nodeExecutionErrorNode";
        var innerException = new NodeExecutionException(nodeId, "Specific node error");
        var failingNode = new FailingNode(1); // Fails once

        var nodeDef = new NodeDefinition(
            nodeId, nodeId, typeof(FailingNode), NodeKind.Source,
            null, null, null, typeof(object));

        var graph = new PipelineGraph
        {
            Nodes = [nodeDef],
            Edges = [],
            PreconfiguredNodeInstances = ImmutableDictionary<string, INode>.Empty,
        };

        _ = A.CallTo(() => _pipelineFactory.Create<TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(graph));
        _ = A.CallTo(() => _nodeFactory.Create(A<NodeDefinition>._, A<PipelineGraph>._)).Returns(failingNode);

        _ = A.CallTo(() => _executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Returns(new Dictionary<string, INode> { { nodeId, failingNode } });

        _ = A.CallTo(() => _executionCoordinator.BuildInputLookup(A<PipelineGraph>._)).Returns(A.Fake<ILookup<string, Edge>>());
        _ = A.CallTo(() => _executionCoordinator.TopologicalSort(A<PipelineGraph>._)).Returns([nodeId]);

        _ = A.CallTo(() => _infrastructureService.ExecuteWithRetriesAsync(
                A<NodeDefinition>._,
                A<INode>._,
                A<PipelineGraph>._,
                A<PipelineContext>._,
                A<Func<Task>>._,
                A<CancellationToken>._))
            .Throws(innerException); // Directly throw NodeExecutionException

        var runner = new PipelineRunnerBuilder()
            .WithPipelineFactory(_pipelineFactory)
            .WithNodeFactory(_nodeFactory)
            .WithExecutionCoordinator(_executionCoordinator)
            .WithInfrastructureService(_infrastructureService)
            .WithObservabilitySurface(_observabilitySurface)
            .Build();

        var context = PipelineContext.Default;

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NodeExecutionException>(() =>
            runner.RunAsync<TestPipelineDefinition>(context));

        _ = exception.Should().BeSameAs(innerException); // Should not be re-wrapped
    }

    [Fact]
    public async Task RunAsync_NonPipelineException_WrapsCorrectly()
    {
        // Arrange
        var nodeId = "generalErrorNode";
        var innerException = new Exception("A general error occurred");
        var failingNode = new FailingNode(1); // Fails once

        var nodeDef = new NodeDefinition(
            nodeId, nodeId, typeof(FailingNode), NodeKind.Source,
            null, null, null, typeof(object));

        var graph = new PipelineGraph
        {
            Nodes = [nodeDef],
            Edges = [],
            PreconfiguredNodeInstances = ImmutableDictionary<string, INode>.Empty,
        };

        _ = A.CallTo(() => _pipelineFactory.Create<TestPipelineDefinition>(A<PipelineContext>._))
            .Returns(new NPipeline.Pipeline.Pipeline(graph));
        _ = A.CallTo(() => _nodeFactory.Create(A<NodeDefinition>._, A<PipelineGraph>._)).Returns(failingNode);

        _ = A.CallTo(() => _executionCoordinator.InstantiateNodes(A<PipelineGraph>._, A<INodeFactory>._))
            .Returns(new Dictionary<string, INode> { { nodeId, failingNode } });

        _ = A.CallTo(() => _executionCoordinator.BuildInputLookup(A<PipelineGraph>._)).Returns(A.Fake<ILookup<string, Edge>>());
        _ = A.CallTo(() => _executionCoordinator.TopologicalSort(A<PipelineGraph>._)).Returns([nodeId]);

        _ = A.CallTo(() => _infrastructureService.ExecuteWithRetriesAsync(
                A<NodeDefinition>._,
                A<INode>._,
                A<PipelineGraph>._,
                A<PipelineContext>._,
                A<Func<Task>>._,
                A<CancellationToken>._))
            .Throws(innerException); // Directly throw a general Exception

        var runner = new PipelineRunnerBuilder()
            .WithPipelineFactory(_pipelineFactory)
            .WithNodeFactory(_nodeFactory)
            .WithExecutionCoordinator(_executionCoordinator)
            .WithInfrastructureService(_infrastructureService)
            .WithObservabilitySurface(_observabilitySurface)
            .Build();

        var context = PipelineContext.Default;

        // Act & Assert
        var exception = await Assert.ThrowsAsync<PipelineExecutionException>(() =>
            runner.RunAsync<TestPipelineDefinition>(context));

        _ = exception.InnerException.Should().BeSameAs(innerException);
        _ = exception.Message.Should().Contain($"Pipeline execution failed at node '{nodeId}'");
    }

    private sealed class TestPipelineDefinition : IPipelineDefinition
    {
        public PipelineGraph Graph { get; init; } = PipelineGraphBuilder.Create()
            .WithNodes([])
            .WithEdges([])
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            // No-op for this test pipeline definition
        }
    }

    private sealed class FailingNode(int failCount) : ISourceNode<object>
    {
        private int _currentAttempt;

        public IDataPipe<object> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            _currentAttempt++;

            if (_currentAttempt <= failCount)
            {
                throw new InvalidOperationException($"Simulated failure on attempt {_currentAttempt}");
            }

            return new InMemoryDataPipe<object>([new()], "failing-output");
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
