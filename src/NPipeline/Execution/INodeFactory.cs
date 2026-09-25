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

    /// <summary>
    ///     Whether the pipeline run that asked for <paramref name="instance" /> owns it and must dispose it.
    ///     Return false for instances whose lifetime a container manages.
    /// </summary>
    /// <param name="nodeDefinition">The definition of the node the instance was created for.</param>
    /// <param name="instance">The instance returned by <see cref="Create" />.</param>
    /// <returns><see langword="true" /> when the run owns the instance; otherwise <see langword="false" />.</returns>
    bool IsOwnedByRun(NodeDefinition nodeDefinition, INode instance) => true;
}
