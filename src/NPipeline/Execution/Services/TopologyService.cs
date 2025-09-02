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
        var sortedOrder = new List<string>();
        var inDegree = graph.Nodes.ToDictionary(n => n.Id, _ => 0);
        var edgesByTarget = graph.Edges.ToLookup(edge => edge.TargetNodeId, edge => edge.SourceNodeId);
        var edgesBySource = graph.Edges.ToLookup(edge => edge.SourceNodeId, edge => edge.TargetNodeId);

        // Calculate in-degrees
        foreach (var edge in graph.Edges)
        {
            inDegree[edge.TargetNodeId]++;
        }

        // Find nodes with no incoming edges
        var queue = new Queue<string>();

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
        if (sortedOrder.Count != graph.Nodes.Count)
            throw new InvalidOperationException(ErrorMessages.CyclicDependencyDetected());

        return sortedOrder;
    }

    /// <summary>
    ///     Builds an input lookup table for the graph
    /// </summary>
    public ILookup<string, Edge> BuildInputLookup(PipelineGraph graph)
    {
        return graph.Edges.ToLookup(edge => edge.TargetNodeId, edge => edge);
    }
}
