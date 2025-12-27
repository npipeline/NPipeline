using System.Collections.Immutable;
using FakeItEasy;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Tests.ErrorHandling;

public static class PipelineRunnerTestHelpers
{
    public sealed class TestPipelineDefinition : IPipelineDefinition
    {
        public PipelineGraph Graph { get; init; } = PipelineGraphBuilder.Create()
            .WithNodes(ImmutableList<NodeDefinition>.Empty)
            .WithEdges(ImmutableList<Edge>.Empty)
            .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
            .Build();

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            // No-op for this test pipeline definition
        }
    }

    public sealed class FailingNode(int failCount) : ISourceNode<object>
    {
        private int _currentAttempt;

        public IDataPipe<object> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            _currentAttempt++;

            if (_currentAttempt <= failCount)
                throw new InvalidOperationException($"Simulated failure on attempt {_currentAttempt}");

            return new InMemoryDataPipe<object>(new List<object> { new() }, "failing-output");
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    public static class PipelineRunnerMockFactory
    {
        public static IPipelineFactory CreatePipelineFactory()
        {
            return A.Fake<IPipelineFactory>();
        }

        public static INodeFactory CreateNodeFactory()
        {
            return A.Fake<INodeFactory>();
        }

        public static IPipelineExecutionCoordinator CreateExecutionCoordinator()
        {
            return A.Fake<IPipelineExecutionCoordinator>();
        }

        public static IPipelineInfrastructureService CreateInfrastructureService()
        {
            return A.Fake<IPipelineInfrastructureService>();
        }

        public static IObservabilitySurface CreateObservabilitySurface()
        {
            return A.Fake<IObservabilitySurface>();
        }

        public static PipelineRunner CreateRunner(
            IPipelineFactory? pipelineFactory = null,
            INodeFactory? nodeFactory = null,
            IPipelineExecutionCoordinator? executionCoordinator = null,
            IPipelineInfrastructureService? infrastructureService = null,
            IObservabilitySurface? observabilitySurface = null)
        {
            return new PipelineRunnerBuilder()
                .WithPipelineFactory(pipelineFactory ?? CreatePipelineFactory())
                .WithNodeFactory(nodeFactory ?? CreateNodeFactory())
                .WithExecutionCoordinator(executionCoordinator ?? CreateExecutionCoordinator())
                .WithInfrastructureService(infrastructureService ?? CreateInfrastructureService())
                .WithObservabilitySurface(observabilitySurface ?? CreateObservabilitySurface())
                .Build();
        }
    }

    public static class NodeDefinitionFactory
    {
        public static NodeDefinition CreateSourceNodeDefinition(string id, Type nodeType)
        {
            return new NodeDefinition(
                new NodeIdentity(id, id),
                new NodeTypeSystem(nodeType, NodeKind.Source, null, typeof(object)),
                new NodeExecutionConfig(),
                new NodeMergeConfig(),
                new NodeLineageConfig());
        }

        public static NodeDefinition CreateSourceNodeDefinition(string id)
        {
            return CreateSourceNodeDefinition(id, typeof(FailingNode));
        }
    }

    public static class PipelineGraphFactory
    {
        public static PipelineGraph CreateGraphWithRetryOptions(NodeDefinition nodeDef, PipelineRetryOptions retryOptions)
        {
            return PipelineGraphBuilder.Create()
                .WithNodes(ImmutableList.Create(nodeDef))
                .WithEdges(ImmutableList<Edge>.Empty)
                .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
                .WithRetryOptions(retryOptions)
                .Build();
        }

        public static PipelineGraph CreateGraphWithCircuitBreakerOptions(
            NodeDefinition nodeDef,
            PipelineCircuitBreakerOptions circuitBreakerOptions,
            CircuitBreakerMemoryManagementOptions? memoryOptions = null)
        {
            return PipelineGraphBuilder.Create()
                .WithNodes(ImmutableList.Create(nodeDef))
                .WithEdges(ImmutableList<Edge>.Empty)
                .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
                .WithCircuitBreakerOptions(circuitBreakerOptions)
                .WithCircuitBreakerMemoryOptions(memoryOptions)
                .Build();
        }

        public static PipelineGraph CreateSimpleGraph(NodeDefinition nodeDef)
        {
            return PipelineGraphBuilder.Create()
                .WithNodes(ImmutableList.Create(nodeDef))
                .WithEdges(ImmutableList<Edge>.Empty)
                .WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode>.Empty)
                .Build();
        }
    }
}
