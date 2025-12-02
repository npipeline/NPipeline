using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Factory;

public sealed class PipelineFactoryTests
{
    [Fact]
    public void Create_ShouldConstructPipeline_FromDefinition()
    {
        // Arrange
        var factory = new PipelineFactory();
        var context = PipelineContext.Default;

        // Act
        var pipeline = factory.Create<TestPipelineDefinition>(context);

        // Assert
        pipeline.Should().NotBeNull();
        pipeline.Graph.Nodes.Should().ContainSingle();
        pipeline.Graph.Nodes[0].Id.Should().Be("source");
    }

    // Test Node Implementations
    private sealed class TestSourceNode : SourceNode<string>
    {
        public override IDataPipe<string> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }

    // Test Pipeline Definition
    private sealed class TestPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            builder.AddSource<TestSourceNode, string>("source");
        }
    }
}
