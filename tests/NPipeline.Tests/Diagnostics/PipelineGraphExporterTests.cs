using System.Collections.Frozen;
using AwesomeAssertions;
using NPipeline.Diagnostics.Export;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
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
        mermaid.Should().Contain(" --> "); // at least one edge
    }

    [Fact]
    public void ToMermaid_NodeNamedEnd_DoesNotEmitReservedId()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        _ = b.AddSource<InMemorySourceNode<int>, int>("End");

        var pipeline = b.Build();
        var mermaid = PipelineGraphExporter.ToMermaid(pipeline.Graph);

        foreach (var line in mermaid.Split('\n'))
            line.TrimStart().Should().NotStartWith("end[");
    }

    [Fact]
    public void ToMermaid_NameWithQuote_UsesEntityForm()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        _ = b.AddSource<InMemorySourceNode<int>, int>("say\"hi");

        var pipeline = b.Build();
        var mermaid = PipelineGraphExporter.ToMermaid(pipeline.Graph);

        mermaid.Should().Contain("#quot;");
        mermaid.Should().NotContain("\\\"");
    }

    [Fact]
    public void ToMermaid_CollidingIds_GetDistinctGeneratedIds()
    {
        // The old exporter mapped both ids to "a_b", merging two distinct nodes.
        var graph = new PipelineGraph
        {
            Nodes =
            [
                new NodeDefinition("a-b", "a-b", typeof(object), NodeKind.Transform, typeof(int), typeof(int)),
                new NodeDefinition("a_b", "a_b", typeof(object), NodeKind.Transform, typeof(int), typeof(int)),
            ],
            Edges = [],
            PreconfiguredNodeInstances = FrozenDictionary<string, INode>.Empty,
        };

        var mermaid = PipelineGraphExporter.ToMermaid(graph);

        var declarationLines = mermaid.Split('\n')
            .Where(line => line.Contains("[\"", StringComparison.Ordinal))
            .Select(line => line.Trim())
            .ToArray();

        declarationLines.Should().HaveCount(2);
        declarationLines[0].Split('[')[0].Should().NotBe(declarationLines[1].Split('[')[0]);
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
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item + 1);
    }
}
