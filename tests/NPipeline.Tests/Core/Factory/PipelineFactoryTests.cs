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
        var context = PipelineContext.CreateDefault();

        // Act
        var pipeline = factory.Create<TestPipelineDefinition>(context);

        // Assert
        pipeline.Should().NotBeNull();
        pipeline.Graph.Nodes.Should().HaveCount(2); // source + sink
        pipeline.Graph.Nodes.Should().Contain(n => n.Id == "source");
        pipeline.Graph.Nodes.Should().Contain(n => n.Id == "sink");
    }

    [Fact]
    public void Create_WithAnUnknownPreconfiguredNodeId_Throws()
    {
        // Arrange
        var factory = new PipelineFactory();
        var context = PipelineContext.CreateDefault();

        context.NodeEnvironment.PreconfiguredNodeInstances["Sink"] = new TestSinkNode();

        // Act
        var act = () => factory.Create<TestPipelineDefinition>(context);

        // Assert - a silently ignored id would leave the real node running instead of the test double
        var thrown = act.Should().Throw<InvalidOperationException>();
        _ = thrown.Which.Message.Should().Contain("Sink");
        _ = thrown.Which.Message.Should().Contain("sanitized, lower-case form");
    }

    [Fact]
    public void Create_WithAContextInstanceForAKnownId_ReplacesTheBuilderInstance()
    {
        // Arrange
        var factory = new PipelineFactory();
        var context = PipelineContext.CreateDefault();
        var overrideSink = new TestSinkNode();

        context.NodeEnvironment.PreconfiguredNodeInstances["sink"] = overrideSink;

        // Act
        var pipeline = factory.Create<OverridableSinkPipelineDefinition>(context);

        // Assert
        _ = pipeline.Graph.PreconfiguredNodeInstances["sink"].Should().BeSameAs(overrideSink);
    }

    // Test Node Implementations
    private sealed class TestSourceNode : SourceNode<string>
    {
        public override IDataStream<string> OpenStream(PipelineContext context, CancellationToken cancellationToken) => throw new NotImplementedException();
    }

    private sealed class TestSinkNode : SinkNode<string>
    {
        public override Task ConsumeAsync(IDataStream<string> input, PipelineContext context, CancellationToken cancellationToken) =>
            throw new NotImplementedException();
    }

    private sealed class TestPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSourceNode, string>("source");
            var sink = builder.AddSink<TestSinkNode, string>("sink");
            builder.Connect(source, sink);
        }
    }

    private sealed class OverridableSinkPipelineDefinition : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSourceNode, string>("source");
            var sink = builder.AddSink(new TestSinkNode(), "sink");
            builder.Connect(source, sink);
        }
    }
}
