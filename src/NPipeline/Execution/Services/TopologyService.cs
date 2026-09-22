using NPipeline.Graph;

namespace NPipeline.Execution.Services;

/// <summary>
///     Service responsible for topological sorting and input lookup operations.
/// </summary>
/// <remarks>
///     Both results are pure functions of the graph's nodes and edges, so both come from
///     <see cref="GraphTopology" />, which computes them once per distinct graph shape rather than once per run.
/// </remarks>
public sealed class TopologyService : ITopologyService
{
    /// <summary>
    ///     Performs topological sort of nodes based on graph dependencies.
    /// </summary>
    public IReadOnlyList<string> TopologicalSort(PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);

        var topology = GraphTopology.For(graph);

        if (topology.HasCycle)
        {
            throw new InvalidOperationException(
                ErrorMessages.CyclicDependencyDetected(topology.CycleNodes, topology.CyclePath));
        }

        return topology.TopologicalOrder;
    }

    /// <summary>
    ///     Builds an input lookup table for the graph.
    /// </summary>
    public ILookup<string, Edge> BuildInputLookup(PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);

        return GraphTopology.For(graph).IncomingEdges;
    }
}
