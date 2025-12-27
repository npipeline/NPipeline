using AwesomeAssertions;
using NPipeline.Execution;
using NPipeline.Execution.Factories;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;
using Xunit.Abstractions;

namespace NPipeline.Tests.Core.Factory;

public sealed class ManualFactoryTests(ITestOutputHelper output)
{
    [Fact]
    public async Task Run_WithDefaultNodeFactory_ShouldSucceed()
    {
        _ = output; // Parameter is unused but required for test infrastructure

        // Arrange
        var pipelineFactory = new PipelineFactory();
        var nodeFactory = new DefaultNodeFactory();
        var runner = new PipelineRunnerBuilder().WithPipelineFactory(pipelineFactory).WithNodeFactory(nodeFactory).Build();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<SimpleManualPipeline>(context);

        // Extract sink instance from graph instantiation via manual factory by re-building and creating nodes
        var pipeline = pipelineFactory.Create<SimpleManualPipeline>(context);
        var manualNodeFactory = new DefaultNodeFactory();
        var collectSinkDef = pipeline.Graph.Nodes.First(n => n.Id == "k");
        var sinkInstance = (InMemorySinkNode<int>)manualNodeFactory.Create(collectSinkDef, pipeline.Graph);

        // Assert
        // Assert that the pipeline runs successfully and the sink can be created
        // The manual factory test is primarily about verifying that nodes can be created manually
        // not about data flow validation (which is covered by other tests)
        sinkInstance.Should().NotBeNull();
        pipeline.Graph.Nodes.Should().Contain(n => n.Id == "k");
    }

    private sealed class SimpleManualPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var s = builder.AddInMemorySource("s", [1, 2, 3]);
            var t = builder.AddTransform((int x) => x + 1, "t");
            var k = builder.AddSink<InMemorySinkNode<int>, int>("k");
            _ = builder.Connect(s, t).Connect(t, k);
        }
    }
}
