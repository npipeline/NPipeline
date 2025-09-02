using AwesomeAssertions;
using NPipeline.Diagnostics.Export;
using NPipeline.Extensions.Testing;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Diagnostics;

public sealed class PipelineGraphExporterTests
{
    [Fact]
    public void ToMermaid_ShouldRenderNodesAndEdges()
    {
        var b = new PipelineBuilder();
        var src = b.AddSource<InMemorySourceNode<int>, int>("source");
        var t = b.AddTransform<Inc, int, int>("increment");
        var sink = b.AddSink<InMemorySinkNode<int>, int>("sink");
        b.Connect(src, t).Connect(t, sink);

        var pipeline = b.Build();
        var mermaid = PipelineGraphExporter.ToMermaid(pipeline.Graph);

        mermaid.Should().Contain("graph TD");
        mermaid.Should().Contain("source");
        mermaid.Should().Contain("increment");
        mermaid.Should().Contain("sink");
        mermaid.Should().Contain("source -->");
        mermaid.Should().Contain("-->"); // at least one edge
    }

    [Fact]
    public void Describe_ShouldIncludeTypesAndKinds()
    {
        var b = new PipelineBuilder();
        var src = b.AddSource<InMemorySourceNode<int>, int>("source");
        var t = b.AddTransform<Inc, int, int>("increment");
        var sink = b.AddSink<InMemorySinkNode<int>, int>("sink");
        b.Connect(src, t).Connect(t, sink);

        var pipeline = b.Build();
        var desc = PipelineGraphExporter.Describe(pipeline.Graph);

        desc.Should().Contain("Nodes:");
        desc.Should().Contain("Edges:");
        desc.Should().Contain("source");
        desc.Should().Contain("increment");
        desc.Should().Contain("sink");
        desc.Should().Contain("Source");
        desc.Should().Contain("Transform");
        desc.Should().Contain("Sink");
    }

    private sealed class Inc : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item + 1);
        }
    }
}
