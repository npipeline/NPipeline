using System.Collections.Frozen;
using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Tests.Execution.Services;

/// <summary>
///     The topological sort is the last line of defence against a cyclic graph that bypassed builder
///     validation. When it trips, the message has to name the nodes involved — finding 27.
/// </summary>
public sealed class TopologyServiceCycleTests
{
    private static PipelineGraph Graph(string[] nodeIds, params (string Source, string Target)[] edges)
    {
        return new PipelineGraph
        {
            Nodes = [.. nodeIds.Select(id => new NodeDefinition(id, id, typeof(object), NodeKind.Transform))],
            Edges = [.. edges.Select(edge => new Edge(edge.Source, edge.Target))],
            PreconfiguredNodeInstances = FrozenDictionary<string, INode>.Empty,
        };
    }

    [Fact]
    public void TopologicalSort_Should_NameTheNodesInTheCycle()
    {
        var graph = Graph(["source", "a", "b", "c"], ("source", "a"), ("a", "b"), ("b", "c"), ("c", "a"));

        var act = () => new TopologyService().TopologicalSort(graph);

        var message = act.Should().Throw<InvalidOperationException>().Which.Message;
        message.Should().Contain("NP0103");
        message.Should().Contain("'a'").And.Contain("'b'").And.Contain("'c'");
        message.Should().NotContain("'source'", "the acyclic prefix sorted cleanly and is not part of the cycle");
    }

    [Fact]
    public void TopologicalSort_Should_ReportTheCyclePathInEdgeDirection()
    {
        var graph = Graph(["a", "b", "c"], ("a", "b"), ("b", "c"), ("c", "a"));

        var act = () => new TopologyService().TopologicalSort(graph);

        var message = act.Should().Throw<InvalidOperationException>().Which.Message;
        message.Should().MatchRegex("Cycle: 'a' -> 'b' -> 'c' -> 'a'|Cycle: 'b' -> 'c' -> 'a' -> 'b'|Cycle: 'c' -> 'a' -> 'b' -> 'c'");
    }

    [Fact]
    public void TopologicalSort_Should_ReportTheCycle_When_DownstreamNodesAreAlsoBlocked()
    {
        // 'sink' is unordered only because it hangs off the cycle; the trace must still find a -> b -> a.
        var graph = Graph(["a", "b", "sink"], ("a", "b"), ("b", "a"), ("b", "sink"));

        var act = () => new TopologyService().TopologicalSort(graph);

        var message = act.Should().Throw<InvalidOperationException>().Which.Message;
        message.Should().Contain("'sink'", "it could not be ordered either");
        message.Should().MatchRegex("Cycle: 'a' -> 'b' -> 'a'|Cycle: 'b' -> 'a' -> 'b'");
        message.Should().NotMatchRegex(@"Cycle: [^.]*'sink'");
    }

    [Fact]
    public void TopologicalSort_Should_Succeed_When_GraphIsAcyclic()
    {
        var graph = Graph(["a", "b", "c"], ("a", "b"), ("b", "c"));

        new TopologyService().TopologicalSort(graph).Should().Equal("a", "b", "c");
    }
}
