using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Factories;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit.Abstractions;

namespace NPipeline.Tests.Core.Factory;

public sealed class DefaultNodeFactoryTests(ITestOutputHelper output)
{
    [Fact]
    public void CreatesNode_Using_ErrorHandlerFactory_Constructor()
    {
        _ = output;

        // Arrange - use the PipelineBuilder to construct pipeline graph and node definition
        var builder = new PipelineBuilder();
        var source = builder.AddSource<InMemorySourceNode<int>, int>("s");
        var handle = builder.AddTransform<NodeWithErrorHandlerFactoryCtor, int, int>("n");
        builder.Connect(source, handle);
        var pipeline = builder.Build();
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == handle.Id);

        var factory = new DefaultNodeFactory();

        // Act
        var instance = factory.Create(nodeDef, pipeline.Graph);

        // Assert
        instance.Should().BeOfType<NodeWithErrorHandlerFactoryCtor>();
        var typed = (NodeWithErrorHandlerFactoryCtor)instance;
        typed.ReceivedFactory.Should().NotBeNull();
    }

    private sealed class NodeWithErrorHandlerFactoryCtor(IErrorHandlerFactory errorHandlerFactory) : TransformNode<int, int>
    {
        public IErrorHandlerFactory? ReceivedFactory { get; } = errorHandlerFactory;

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }
}
