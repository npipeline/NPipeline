using System.Collections.Frozen;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace NPipeline.Graph;

/// <summary>
///     The adjacency and ordering derived from a graph's nodes and edges, computed once per distinct graph shape.
/// </summary>
/// <remarks>
///     <para>
///         All three views used to be recomputed on every run, and the outgoing view was rebuilt by scanning the whole
///         edge array once per node — O(N·E) with an array allocated per node. They are pure functions of
///         <see cref="PipelineGraph.Nodes" /> and <see cref="PipelineGraph.Edges" />, so they are computed once and
///         memoized.
///     </para>
///     <para>
///         The memo is keyed on the identity of the underlying node and edge arrays rather than on the graph object.
///         A <c>with</c> expression produces a new <see cref="PipelineGraph" /> while carrying the same node and edge
///         arrays forward, and the runtime binder rewrites the graph that way on every run. Keying on the arrays is
///         also what keeps the memo honest: a rewrite that genuinely changes the nodes or the edges produces different
///         arrays and therefore misses, so a stale topology cannot survive one.
///     </para>
/// </remarks>
public sealed class GraphTopology
{
    /// <summary>
    ///     Weak-keyed memo. Entries are a pure function of the key and die with it, so this holds no state and leaks
    ///     no graph: it is not shared configuration in the sense that a mutable static would be.
    /// </summary>
    private static readonly ConditionalWeakTable<NodeDefinition[], GraphTopology> Cache = new();

    private readonly Edge[] _edgesKey;

    private GraphTopology(
        Edge[] edgesKey,
        FrozenDictionary<string, ImmutableArray<Edge>> outgoingEdges,
        ILookup<string, Edge> incomingEdges,
        ImmutableArray<string> topologicalOrder,
        ImmutableArray<string> cycleNodes,
        ImmutableArray<string> cyclePath)
    {
        _edgesKey = edgesKey;
        OutgoingEdges = outgoingEdges;
        IncomingEdges = incomingEdges;
        TopologicalOrder = topologicalOrder;
        CycleNodes = cycleNodes;
        CyclePath = cyclePath;
    }

    /// <summary>
    ///     Outgoing edges by source node ID. A node with no outgoing edges is absent rather than empty.
    /// </summary>
    public FrozenDictionary<string, ImmutableArray<Edge>> OutgoingEdges { get; }

    /// <summary>
    ///     Incoming edges by target node ID.
    /// </summary>
    public ILookup<string, Edge> IncomingEdges { get; }

    /// <summary>
    ///     Node IDs in dependency order, or empty when the graph has a cycle.
    /// </summary>
    public ImmutableArray<string> TopologicalOrder { get; }

    /// <summary>
    ///     True when the graph could not be ordered because it contains a cycle.
    /// </summary>
    public bool HasCycle => !CycleNodes.IsEmpty;

    /// <summary>
    ///     The nodes that survived the sort because they sit on or downstream of a cycle. Empty when there is none.
    /// </summary>
    /// <remarks>
    ///     The cycle is recorded rather than thrown so that the memo can be built once. The caller raises the error,
    ///     which keeps the stack pointing at the run that hit it rather than at whichever run happened to be first.
    /// </remarks>
    public ImmutableArray<string> CycleNodes { get; }

    /// <summary>
    ///     One concrete cycle, read in edge direction and closed by repeating its first node. Empty when there is none.
    /// </summary>
    public ImmutableArray<string> CyclePath { get; }

    /// <summary>
    ///     Returns the topology of a graph, computing it on first use.
    /// </summary>
    /// <param name="graph">The graph to derive adjacency for.</param>
    /// <returns>The graph's adjacency views.</returns>
    public static GraphTopology For(PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);

        var nodesKey = ImmutableCollectionsMarshal.AsArray(graph.Nodes);
        var edgesKey = ImmutableCollectionsMarshal.AsArray(graph.Edges);

        if (nodesKey is null || edgesKey is null)
            return Compute(graph, edgesKey ?? []);

        if (Cache.TryGetValue(nodesKey, out var cached) && ReferenceEquals(cached._edgesKey, edgesKey))
            return cached;

        var computed = Compute(graph, edgesKey);
        Cache.AddOrUpdate(nodesKey, computed);

        return computed;
    }

    /// <summary>
    ///     Returns the outgoing edges of one node, or an empty span when it has none.
    /// </summary>
    /// <param name="nodeId">The source node ID.</param>
    /// <returns>The node's outgoing edges.</returns>
    public ImmutableArray<Edge> OutgoingFrom(string nodeId)
    {
        return OutgoingEdges.TryGetValue(nodeId, out var edges)
            ? edges
            : [];
    }

    private static GraphTopology Compute(PipelineGraph graph, Edge[] edgesKey)
    {
        Dictionary<string, ImmutableArray<Edge>.Builder>? bySource = null;

        foreach (var edge in graph.Edges)
        {
            bySource ??= new Dictionary<string, ImmutableArray<Edge>.Builder>(StringComparer.Ordinal);

            if (!bySource.TryGetValue(edge.SourceNodeId, out var builder))
            {
                builder = ImmutableArray.CreateBuilder<Edge>();
                bySource[edge.SourceNodeId] = builder;
            }

            builder.Add(edge);
        }

        var outgoing = bySource is null
            ? FrozenDictionary<string, ImmutableArray<Edge>>.Empty
            : bySource.ToFrozenDictionary(
                static pair => pair.Key,
                static pair => pair.Value.DrainToImmutable(),
                StringComparer.Ordinal);

        var incoming = graph.Edges.ToLookup(static edge => edge.TargetNodeId, StringComparer.Ordinal);
        var (order, cycleNodes, cyclePath) = Sort(graph, outgoing, incoming);

        return new GraphTopology(edgesKey, outgoing, incoming, order, cycleNodes, cyclePath);
    }

    /// <summary>
    ///     Kahn's algorithm over the precomputed adjacency.
    /// </summary>
    private static (ImmutableArray<string> Order, ImmutableArray<string> CycleNodes, ImmutableArray<string> CyclePath) Sort(
        PipelineGraph graph,
        FrozenDictionary<string, ImmutableArray<Edge>> outgoing,
        ILookup<string, Edge> incoming)
    {
        var inDegree = new Dictionary<string, int>(graph.Nodes.Length, StringComparer.Ordinal);

        foreach (var node in graph.Nodes)
        {
            inDegree[node.Id] = 0;
        }

        foreach (var edge in graph.Edges)
        {
            // An edge to a node the graph does not declare is a validation error, reported by EdgeReferenceRule
            // with a message that names it. Ordering ignores it rather than failing with a KeyNotFoundException.
            if (inDegree.TryGetValue(edge.TargetNodeId, out var current))
                inDegree[edge.TargetNodeId] = current + 1;
        }

        var order = ImmutableArray.CreateBuilder<string>(graph.Nodes.Length);
        var queue = new Queue<string>();

        foreach (var node in graph.Nodes)
        {
            if (inDegree[node.Id] == 0)
                queue.Enqueue(node.Id);
        }

        while (queue.Count > 0)
        {
            var nodeId = queue.Dequeue();
            order.Add(nodeId);

            if (!outgoing.TryGetValue(nodeId, out var outgoingEdges))
                continue;

            foreach (var edge in outgoingEdges)
            {
                if (!inDegree.TryGetValue(edge.TargetNodeId, out var pending))
                    continue;

                inDegree[edge.TargetNodeId] = --pending;

                if (pending == 0)
                    queue.Enqueue(edge.TargetNodeId);
            }
        }

        if (order.Count == graph.Nodes.Length)
            return (order.DrainToImmutable(), [], []);

        var remaining = graph.Nodes
            .Where(node => inDegree[node.Id] > 0)
            .Select(static node => node.Id)
            .ToImmutableArray();

        return ([], remaining, TraceCycle(remaining, incoming));
    }

    /// <summary>
    ///     Recovers one concrete cycle from the nodes that survived the sort.
    ///     A surviving node has a non-zero in-degree because at least one of its predecessors also never
    ///     got processed, so walking backwards stays inside the set and is guaranteed to revisit a node.
    ///     The first repeat closes the cycle.
    /// </summary>
    private static ImmutableArray<string> TraceCycle(ImmutableArray<string> remaining, ILookup<string, Edge> incoming)
    {
        if (remaining.IsEmpty)
            return [];

        var unprocessed = new HashSet<string>(remaining, StringComparer.Ordinal);
        var path = new List<string>();
        var positionInPath = new Dictionary<string, int>(StringComparer.Ordinal);
        var current = remaining[0];

        while (positionInPath.TryAdd(current, path.Count))
        {
            path.Add(current);

            var predecessor = incoming[current]
                .Select(static edge => edge.SourceNodeId)
                .FirstOrDefault(unprocessed.Contains);

            if (predecessor is null)
                return [];

            current = predecessor;
        }

        // Drop the tail that leads out of the cycle, then reverse so the path reads in edge direction.
        var cycle = path.Skip(positionInPath[current]).Reverse().ToList();
        cycle.Add(cycle[0]);

        return [.. cycle];
    }
}
