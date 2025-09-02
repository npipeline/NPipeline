using NPipeline.Graph;

namespace NPipeline.Execution;

/// <summary>
///     A service for resolving execution annotations from a pipeline graph.
/// </summary>
public interface IExecutionAnnotationsService
{
    /// <summary>
    ///     Resolves the execution options for a given node from the pipeline graph.
    /// </summary>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="nodeId">The ID of the node for which to resolve options.</param>
    /// <returns>A <see cref="NodeExecutionOptions" /> object containing the resolved options.</returns>
    NodeExecutionOptions GetOptions(PipelineGraph graph, string nodeId);
}
