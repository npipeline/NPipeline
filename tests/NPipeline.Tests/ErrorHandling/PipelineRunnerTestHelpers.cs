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

        public IDataStream<object> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            _currentAttempt++;

            if (_currentAttempt <= failCount)
                throw new InvalidOperationException($"Simulated failure on attempt {_currentAttempt}");

            return new InMemoryDataStream<object>(new List<object> { new() }, "failing-output");
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

        public static INodeExecutor CreateNodeExecutor()
        {
            return A.Fake<INodeExecutor>();
        }

        public static ITopologyService CreateTopologyService()
        {
            return A.Fake<ITopologyService>();
        }

        public static INodeInstantiationService CreateNodeInstantiationService()
        {
            return A.Fake<INodeInstantiationService>();
        }

        public static IErrorHandlingService CreateErrorHandlingService()
        {
            return A.Fake<IErrorHandlingService>();
        }

        public static IPersistenceService CreatePersistenceService()
        {
            return A.Fake<IPersistenceService>();
        }

        public static IObservabilitySurface CreateObservabilitySurface()
        {
            return NullObservabilitySurface.Instance;
        }

        public static PipelineRunner CreateRunner(
            IPipelineFactory? pipelineFactory = null,
            INodeFactory? nodeFactory = null,
            INodeExecutor? nodeExecutor = null,
            ITopologyService? topologyService = null,
            INodeInstantiationService? nodeInstantiationService = null,
            IErrorHandlingService? errorHandlingService = null,
            IPersistenceService? persistenceService = null,
            IObservabilitySurface? observabilitySurface = null)
        {
            return new PipelineRunnerBuilder()
                .WithPipelineFactory(pipelineFactory ?? CreatePipelineFactory())
                .WithNodeFactory(nodeFactory ?? CreateNodeFactory())
                .WithNodeExecutor(nodeExecutor ?? CreateNodeExecutor())
                .WithTopologyService(topologyService ?? CreateTopologyService())
                .WithNodeInstantiationService(nodeInstantiationService ?? CreateNodeInstantiationService())
                .WithErrorHandlingService(errorHandlingService ?? CreateErrorHandlingService())
                .WithPersistenceService(persistenceService ?? CreatePersistenceService())
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
