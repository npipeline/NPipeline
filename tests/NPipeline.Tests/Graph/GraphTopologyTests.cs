using System.Collections.Frozen;
using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Tests.Graph;

/// <summary>
///     Adjacency and ordering used to be recomputed on every run, and the outgoing view was rebuilt by scanning the
///     whole edge array once per node — O(N·E) with an array allocated per node. <see cref="GraphTopology" /> computes
///     all three views once and memoizes them.
///     <para>
///         The memo is keyed on the identity of the node and edge arrays rather than the graph object, because the
///         runtime binder rewrites the graph with a <c>with</c> expression on every run while carrying the same arrays
///         forward. These tests pin both halves of that: a rewrite that keeps the arrays hits, and one that changes
///         them misses, so a stale topology cannot survive a real edit.
///     </para>
/// </summary>
public sealed class GraphTopologyTests
{
    [Fact]
    public void For_ReturnsTheSameInstance_ForTheSameGraph()
    {
        var graph = Graph(["a", "b"], ("a", "b"));

        GraphTopology.For(graph).Should().BeSameAs(GraphTopology.For(graph));
    }

    [Fact]
    public void For_ReturnsTheSameInstance_AfterARewriteThatKeepsTheNodesAndEdges()
    {
        var graph = Graph(["a", "b"], ("a", "b"));
        var rewritten = graph with { ChildGraphs = FrozenDictionary<string, PipelineGraph>.Empty };

        GraphTopology.For(rewritten).Should().BeSameAs(
            GraphTopology.For(graph),
            "the runtime binder rewrites the graph on every run without touching its nodes or edges");
    }

    [Fact]
    public void For_Recomputes_WhenTheEdgesChange()
    {
        var graph = Graph(["a", "b", "c"], ("a", "b"));
        var original = GraphTopology.For(graph);

        var rewired = graph with { Edges = [new Edge("a", "b"), new Edge("b", "c")] };
        var updated = GraphTopology.For(rewired);

        updated.Should().NotBeSameAs(original);
        updated.OutgoingFrom("b").Should().ContainSingle(edge => edge.TargetNodeId == "c");
        original.OutgoingFrom("b").Should().BeEmpty("the first topology must not see the rewrite");
    }

    [Fact]
    public void For_Recomputes_WhenTheNodesChange()
    {
        var graph = Graph(["a", "b"], ("a", "b"));
        var original = GraphTopology.For(graph);

        var extended = graph with { Nodes = [.. graph.Nodes, new NodeDefinition("c", "c", typeof(object), NodeKind.Transform)] };
        var updated = GraphTopology.For(extended);

        updated.Should().NotBeSameAs(original);
        updated.TopologicalOrder.Should().HaveCount(3);
        original.TopologicalOrder.Should().HaveCount(2);
    }

    [Fact]
    public void OutgoingFrom_ReturnsTheEdgesOfOneNode()
    {
        var graph = Graph(["source", "left", "right"], ("source", "left"), ("source", "right"));

        var outgoing = GraphTopology.For(graph).OutgoingFrom("source");

        outgoing.Select(edge => edge.TargetNodeId).Should().BeEquivalentTo(["left", "right"]);
    }

    [Fact]
    public void OutgoingFrom_ReturnsEmpty_ForANodeWithNoOutgoingEdges()
    {
        var graph = Graph(["a", "b"], ("a", "b"));

        GraphTopology.For(graph).OutgoingFrom("b").Should().BeEmpty();
    }

    [Fact]
    public void IncomingEdges_GroupsByTarget()
    {
        var graph = Graph(["left", "right", "join"], ("left", "join"), ("right", "join"));

        GraphTopology.For(graph).IncomingEdges["join"]
            .Select(edge => edge.SourceNodeId)
            .Should().BeEquivalentTo(["left", "right"]);
    }

    [Fact]
    public void TopologicalOrder_PutsEveryNodeAfterItsDependencies()
    {
        var graph = Graph(["c", "a", "b"], ("a", "b"), ("b", "c"));

        var order = GraphTopology.For(graph).TopologicalOrder;

        order.Should().Equal("a", "b", "c");
    }

    [Fact]
    public void HasCycle_ReportsTheCycleInsteadOfThrowing()
    {
        var graph = Graph(["a", "b"], ("a", "b"), ("b", "a"));

        var topology = GraphTopology.For(graph);

        topology.HasCycle.Should().BeTrue();
        topology.TopologicalOrder.Should().BeEmpty();
        topology.CycleNodes.Should().BeEquivalentTo(["a", "b"]);
    }

    /// <summary>
    ///     A dangling edge is a validation error that <c>EdgeReferenceRule</c> reports by name. Ordering must not turn
    ///     it into a <see cref="KeyNotFoundException" /> from inside the sort.
    /// </summary>
    [Fact]
    public void For_IgnoresEdgesToNodesTheGraphDoesNotDeclare()
    {
        var graph = Graph(["a"], ("a", "not-a-node"));

        var topology = GraphTopology.For(graph);

        topology.HasCycle.Should().BeFalse();
        topology.TopologicalOrder.Should().Equal("a");
        topology.OutgoingFrom("a").Should().ContainSingle(edge => edge.TargetNodeId == "not-a-node");
    }

    private static PipelineGraph Graph(string[] nodeIds, params (string Source, string Target)[] edges)
    {
        return new PipelineGraph
        {
            Nodes = [.. nodeIds.Select(id => new NodeDefinition(id, id, typeof(object), NodeKind.Transform))],
            Edges = [.. edges.Select(edge => new Edge(edge.Source, edge.Target))],
            PreconfiguredNodeInstances = FrozenDictionary<string, INode>.Empty,
        };
    }
}
