using NPipeline.Graph;

namespace NPipeline.Execution.Services;

/// <summary>
///     Service responsible for topological sorting and input lookup operations
/// </summary>
public sealed class TopologyService : ITopologyService
{
    /// <summary>
    ///     Performs topological sort of nodes based on graph dependencies
    /// </summary>
    public List<string> TopologicalSort(PipelineGraph graph)
    {
        var sortedOrder = new List<string>(graph.Nodes.Length);
        var inDegree = new Dictionary<string, int>(graph.Nodes.Length);
        Queue<string> queue = new();

        var edgesByTarget = graph.Edges.ToLookup(edge => edge.TargetNodeId, edge => edge.SourceNodeId);
        var edgesBySource = graph.Edges.ToLookup(edge => edge.SourceNodeId, edge => edge.TargetNodeId);

        // Initialize in-degrees
        foreach (var node in graph.Nodes)
        {
            inDegree[node.Id] = 0;
        }

        // Calculate in-degrees
        foreach (var edge in graph.Edges)
        {
            inDegree[edge.TargetNodeId]++;
        }

        // Find nodes with no incoming edges
        foreach (var node in graph.Nodes)
        {
            if (inDegree[node.Id] == 0)
                queue.Enqueue(node.Id);
        }

        // Process nodes
        while (queue.Count > 0)
        {
            var nodeId = queue.Dequeue();
            sortedOrder.Add(nodeId);

            // Reduce in-degree of neighbors
            foreach (var targetNodeId in edgesBySource[nodeId])
            {
                inDegree[targetNodeId]--;

                if (inDegree[targetNodeId] == 0)
                    queue.Enqueue(targetNodeId);
            }
        }

        // Check for cycles
        if (sortedOrder.Count != graph.Nodes.Length)
        {
            var remaining = graph.Nodes.Where(node => inDegree[node.Id] > 0).Select(node => node.Id).ToArray();

            throw new InvalidOperationException(ErrorMessages.CyclicDependencyDetected(remaining, TraceCycle(remaining, edgesByTarget)));
        }

        return sortedOrder;
    }

    /// <summary>
    ///     Recovers one concrete cycle from the nodes that survived the topological sort.
    ///     A surviving node has a non-zero in-degree because at least one of its predecessors also never
    ///     got processed, so walking backwards stays inside the set and is guaranteed to revisit a node.
    ///     The first repeat closes the cycle.
    /// </summary>
    private static IReadOnlyList<string> TraceCycle(IReadOnlyList<string> remaining, ILookup<string, string> edgesByTarget)
    {
        if (remaining.Count == 0)
            return [];

        var unprocessed = new HashSet<string>(remaining);
        var path = new List<string>();
        var positionInPath = new Dictionary<string, int>();
        var current = remaining[0];

        while (positionInPath.TryAdd(current, path.Count))
        {
            path.Add(current);
            var predecessor = edgesByTarget[current].FirstOrDefault(unprocessed.Contains);

            if (predecessor is null)
                return [];

            current = predecessor;
        }

        // Drop the tail that leads out of the cycle, then reverse so the path reads in edge direction.
        var cycle = path.Skip(positionInPath[current]).Reverse().ToList();
        cycle.Add(cycle[0]);

        return cycle;
    }

    /// <summary>
    ///     Builds an input lookup table for the graph
    /// </summary>
    public ILookup<string, Edge> BuildInputLookup(PipelineGraph graph)
    {
        return graph.Edges.ToLookup(edge => edge.TargetNodeId, edge => edge);
    }
}
