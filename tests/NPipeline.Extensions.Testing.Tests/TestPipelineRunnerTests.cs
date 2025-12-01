// ReSharper disable ClassNeverInstantiated.Local

using AwesomeAssertions;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

public class TestPipelineRunnerTests
{
    [Fact]
    public async Task RunAndGetResultAsync_ShouldExecutePipelineAndReturnResults()
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

        // Act
        var result = await testRunner.RunAndGetResultAsync<TestPipeline, int>(context);

        // Assert
        result.Should().BeEquivalentTo(new[] { 1, 2, 3 }.Select(x => x * 2));
    }

    [Fact]
    public void Should_Run_Simple_Pipeline()
    {
        // Arrange
        var context = PipelineContext.Default;
        var pipelineFactory = new PipelineFactory();

        // Act
        var pipeline = pipelineFactory.Create<TestPipeline>(context);

        // Assert
        pipeline.Should().NotBeNull();
    }

    [Fact]
    public async Task RunAndGetResultAsync_UsingNewTestHarness_ShouldExecutePipelineAndReturnResults()
    {
        // Arrange & Act
        var harness = new PipelineTestHarness<TestPipeline>();
        var harnessResult = await harness.RunAsync();

        // Assert
        harnessResult.AssertSuccess();

        var sink = harnessResult.GetSink<InMemorySinkNode<int>>();
        var results = await sink.Completion;
        results.Should().BeEquivalentTo(new[] { 2, 4, 6 });
    }

    private sealed class TestPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySourceWithDataFromContext(context, "Source", new[] { 1, 2, 3 });
            var transform = builder.AddTransform<MultiplyByTwoTransform, int, int>("Transform");
            var sink = builder.AddInMemorySink<int>("Sink", context);
            builder.Connect(source, transform).Connect(transform, sink);
        }
    }

    private sealed class MultiplyByTwoTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item * 2);
        }
    }

    public class SimpleNodeFactory : INodeFactory
    {
        public INode Create(NodeDefinition nodeDefinition, PipelineGraph graph)
        {
            return (INode)Activator.CreateInstance(nodeDefinition.NodeType)!;
        }
    }
}
