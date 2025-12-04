using NPipeline.Graph;

namespace NPipeline.Execution;

/// <summary>
///     Provides topology analysis services for pipeline graphs.
/// </summary>
public interface ITopologyService
{
    /// <summary>
    ///     Performs a topological sort of the nodes in the pipeline graph.
    /// </summary>
    /// <param name="graph">The pipeline graph to sort.</param>
    /// <returns>A list of node IDs in topological order.</returns>
    List<string> TopologicalSort(PipelineGraph graph);

    /// <summary>
    ///     Builds a lookup table mapping node IDs to their input edges.
    /// </summary>
    /// <param name="graph">The pipeline graph to analyze.</param>
    /// <returns>A lookup table where each key is a node ID and value is a collection of input edges.</returns>
    ILookup<string, Edge> BuildInputLookup(PipelineGraph graph);
}
