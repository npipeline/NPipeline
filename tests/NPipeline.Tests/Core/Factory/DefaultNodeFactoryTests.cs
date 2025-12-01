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
    public void CreatesNode_With_Parameterless_Constructor()
    {
        _ = output;

        // Arrange - use the PipelineBuilder to construct pipeline graph and node definition
        var builder = new PipelineBuilder();
        var source = builder.AddSource<InMemorySourceNode<int>, int>("s");
        var handle = builder.AddTransform<SimpleTransformNode, int, int>("n");
        _ = builder.Connect(source, handle);
        var pipeline = builder.Build();
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == handle.Id);

        var factory = new DefaultNodeFactory();

        // Act
        var instance = factory.Create(nodeDef, pipeline.Graph);

        // Assert
        _ = instance.Should().BeOfType<SimpleTransformNode>();
    }

    [Fact]
    public void Create_With_Custom_ErrorHandlerFactory_Uses_Custom_Handler()
    {
        _ = output;

        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddSource<InMemorySourceNode<int>, int>("s");
        var handle = builder.AddTransform<SimpleTransformNode, int, int>("n");
        _ = builder.Connect(source, handle);
        var pipeline = builder.Build();
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == handle.Id);

        var customFactory = new CapturingErrorHandlerFactory();
        var factory = new DefaultNodeFactory(customFactory);
        var configuredNode = nodeDef with { ErrorHandlerType = typeof(TestNodeErrorHandler) };

        // Act
        var instance = (SimpleTransformNode)factory.Create(configuredNode, pipeline.Graph);

        // Assert
        _ = instance.ErrorHandler.Should().BeOfType<TestNodeErrorHandler>();
        _ = customFactory.RequestedNodeHandlers.Should().Contain(typeof(TestNodeErrorHandler));
    }

    [Fact]
    public void Create_With_Preconfigured_Instance_Returns_Instance()
    {
        _ = output;

        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddSource<InMemorySourceNode<int>, int>("s");
        var handle = builder.AddTransform<SimpleTransformNode, int, int>("n");

        var preconfiguredInstance = new SimpleTransformNode();
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, preconfiguredInstance);

        var pipeline = builder.Build();
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == handle.Id);

        var factory = new DefaultNodeFactory();

        // Act
        var instance = factory.Create(nodeDef, pipeline.Graph);

        // Assert
        _ = instance.Should().BeSameAs(preconfiguredInstance);
    }

    [Fact]
    public void Create_Without_Parameterless_Constructor_Throws_With_Helpful_Message()
    {
        _ = output;

        // Arrange
        var builder = new PipelineBuilder();
        var source = builder.AddSource<InMemorySourceNode<int>, int>("s");
        var handle = builder.AddTransform<NodeWithDependencyConstructor, int, int>("n");
        _ = builder.Connect(source, handle);
        var pipeline = builder.Build();
        var nodeDef = pipeline.Graph.Nodes.Single(n => n.Id == handle.Id);

        var factory = new DefaultNodeFactory();

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => factory.Create(nodeDef, pipeline.Graph));
        _ = exception.Message.Should().Contain("Failed to create node instance");
        _ = exception.Message.Should().Contain("public parameterless constructor");
        _ = exception.Message.Should().Contain("AddPreconfiguredNodeInstance");
        _ = exception.Message.Should().Contain("DIContainerNodeFactory");
    }

    private sealed class SimpleTransformNode : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    private sealed class NodeWithDependencyConstructor(IErrorHandlerFactory errorHandlerFactory) : TransformNode<int, int>
    {
        private readonly IErrorHandlerFactory _errorHandlerFactory = errorHandlerFactory;

        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _ = _errorHandlerFactory;
            return Task.FromResult(item);
        }
    }

    private sealed class CapturingErrorHandlerFactory : IErrorHandlerFactory
    {
        public List<Type> RequestedNodeHandlers { get; } = [];

        public IPipelineErrorHandler? CreateErrorHandler(Type handlerType)
        {
            return null;
        }

        public INodeErrorHandler? CreateNodeErrorHandler(Type handlerType)
        {
            RequestedNodeHandlers.Add(handlerType);

            return handlerType == typeof(TestNodeErrorHandler)
                ? new TestNodeErrorHandler()
                : null;
        }

        public IDeadLetterSink? CreateDeadLetterSink(Type sinkType)
        {
            return null;
        }
    }

    private sealed class TestNodeErrorHandler : INodeErrorHandler<ITransformNode<int, int>, int>
    {
        public Task<NodeErrorDecision> HandleAsync(
            ITransformNode<int, int> node,
            int failedItem,
            Exception error,
            PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(NodeErrorDecision.Fail);
        }
    }
}
