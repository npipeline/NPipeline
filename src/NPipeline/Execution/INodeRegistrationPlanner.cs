using NPipeline.Graph;
using NPipeline.Graph.PipelineDelegates;

namespace NPipeline.Execution;

/// <summary>
///     Produces execution-related registration artifacts for nodes during builder registration.
/// </summary>
public interface INodeRegistrationPlanner
{
    /// <summary>
    ///     Prepares execution metadata for a node registration event.
    /// </summary>
    /// <param name="kind">The node kind being registered.</param>
    /// <param name="nodeType">The concrete node type being registered.</param>
    void PrepareNode(NodeKind kind, Type nodeType);

    /// <summary>
    ///     Builds a custom merge delegate for a node type that supports custom merge.
    /// </summary>
    /// <param name="nodeType">The concrete node type.</param>
    /// <returns>A custom merge delegate that can be invoked at runtime.</returns>
    CustomMergeDelegate BuildCustomMergeDelegate(Type nodeType);
}