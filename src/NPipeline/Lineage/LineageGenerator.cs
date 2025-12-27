using NPipeline.Graph;

namespace NPipeline.Lineage;

/// <summary>
///     Generates a serializable lineage report from a pipeline graph.
/// </summary>
public static class LineageGenerator
{
    /// <summary>
    ///     Creates a lineage report from the given pipeline graph.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline definition.</param>
    /// <param name="graph">The pipeline graph to analyze.</param>
    /// <param name="runId">The unique ID for the current pipeline run.</param>
    /// <returns>A new <see cref="PipelineLineageReport" />.</returns>
    public static PipelineLineageReport Generate(string pipelineName, PipelineGraph graph, Guid runId)
    {
        var nodes = graph.Nodes.Select(n =>
        {
            // Use builder-populated metadata instead of reflection on interfaces.
            var inputType = n.InputType;
            var outputType = n.OutputType;
            return new NodeLineageInfo(n.Id, n.NodeType.Name, inputType?.FullName, outputType?.FullName);
        }).ToList();

        var edges = graph.Edges.Select(e => new EdgeLineageInfo(e.SourceNodeId, e.TargetNodeId)).ToList();

        return new PipelineLineageReport(pipelineName, runId, nodes, edges);
    }

    // Reflection-based GetNodeTypes removed (metadata now provided by builder at NodeDefinition time).
}
