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
        pipeline.Graph.Nodes.Should().HaveCount(2); // source + sink
        pipeline.Graph.Nodes.Should().Contain(n => n.Id == "source");
        pipeline.Graph.Nodes.Should().Contain(n => n.Id == "sink");
    }

    // Test Node Implementations
    private sealed class TestSourceNode : SourceNode<string>
    {
        public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }

    private sealed class TestSinkNode : SinkNode<string>
    {
        public override Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }

    // Test Pipeline Definition
    private sealed class TestPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSourceNode, string>("source");
            var sink = builder.AddSink<TestSinkNode, string>("sink");
            builder.Connect(source, sink);
        }
    }
}
