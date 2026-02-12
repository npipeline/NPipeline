using AwesomeAssertions;
using Microsoft.Extensions.Logging;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

public class TestingUtilitiesTests
{
    [Fact]
    public async Task MockNode_Should_UseProvidedLogic()
    {
        // Arrange
        var nodeFactory = new SimpleNodeFactory();
        var pipelineFactory = new PipelineFactory();

        var pipelineRunner = new PipelineRunnerBuilder()
            .WithPipelineFactory(pipelineFactory)
            .WithNodeFactory(nodeFactory)
            .Build();

        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;
        var sourceData = new[] { "a", "b", "c" };
        context.SetSourceData(sourceData);

        // Act
        var result = await testRunner.RunAndGetResultAsync<MockNodeTestPipeline, int>(context);

        // Assert
        result.Should().BeEquivalentTo([1, 2, 3]);
    }

    [Fact]
    public async Task ExceptionThrowingNode_Should_ThrowException()
    {
        // Arrange
        var nodeFactory = new SimpleNodeFactory();
        var pipelineFactory = new PipelineFactory();

        var pipelineRunner = new PipelineRunnerBuilder()
            .WithPipelineFactory(pipelineFactory)
            .WithNodeFactory(nodeFactory)
            .Build();

        var testRunner = new TestPipelineRunner(pipelineRunner);
        var context = PipelineContext.Default;
        var sourceData = new[] { 1, 2, 3 };
        context.SetSourceData(sourceData);

        // Act
        var action = async () => await testRunner.RunAndGetResultAsync<ExceptionThrowingPipeline, int>(context);

        // Assert
        await action.Should().ThrowAsync<NodeExecutionException>();
    }

    [Fact]
    public void CapturingLogger_Should_CaptureLogs()
    {
        // Arrange
        var logger = new CapturingLogger();

        // Act
        logger.Log(LogLevel.Information, "Hello, {Name}", "World");

        // Assert
        logger.LogEntries.Should().ContainSingle();
        logger.LogEntries[0].LogLevel.Should().Be(LogLevel.Information);
        logger.LogEntries[0].Message.Should().Be("Hello, {Name}");
        logger.LogEntries[0].Args.Should().BeEquivalentTo(["World"]);
    }


    private sealed class MockNodeTestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<string>("source");
            var transform = builder.AddTransform<MockNode<string, int>, string, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    private sealed class ExceptionThrowingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource<int>("source");
            var transform = builder.AddTransform<ExceptionThrowingNode<int>, int, int>("transform");
            var sink = builder.AddInMemorySink<int>("sink");

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    public class SimpleNodeFactory : INodeFactory
    {
        public INode Create(NodeDefinition nodeDefinition, PipelineGraph graph)
        {
            if (nodeDefinition.NodeType == typeof(MockNode<string, int>))
            {
                return new MockNode<string, int>((item, _, _) =>
                {
                    var length = item.Length;

                    if (item == "a")
                        length = 1;

                    if (item == "b")
                        length = 2;

                    if (item == "c")
                        length = 3;

                    return Task.FromResult(length);
                });
            }

            if (nodeDefinition.NodeType == typeof(ExceptionThrowingNode<int>))
                return new ExceptionThrowingNode<int>(new InvalidOperationException());

            return (INode)Activator.CreateInstance(nodeDefinition.NodeType)!;
        }
    }
}
