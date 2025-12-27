using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Execution;

/// <summary>
///     Defines a factory for creating instances of pipeline nodes.
/// </summary>
public interface INodeFactory
{
    /// <summary>
    ///     Creates an instance of a node based on its definition.
    /// </summary>
    /// <param name="nodeDefinition">The definition of the node to create.</param>
    /// <param name="graph">The pipeline graph that the node belongs to.</param>
    /// <returns>An instance of the specified node.</returns>
    INode Create(NodeDefinition nodeDefinition, PipelineGraph graph);
}
