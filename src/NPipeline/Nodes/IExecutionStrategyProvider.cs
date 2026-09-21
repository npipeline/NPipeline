using NPipeline.Execution;

namespace NPipeline.Nodes;

/// <summary>
///     Implemented by node types that only make sense under a particular execution strategy, such as the batching
///     and unbatching nodes.
/// </summary>
/// <remarks>
///     The strategy a node supplies here is a default, not a requirement: a strategy configured on the node's
///     <see cref="Graph.NodeDefinition" /> with <c>WithExecutionStrategy</c> takes precedence. The property is read-only
///     and is read once per run, so a node instance is never mutated by the framework.
/// </remarks>
public interface IExecutionStrategyProvider
{
    /// <summary>
    ///     Gets the execution strategy to use when the graph does not configure one for this node.
    /// </summary>
    IExecutionStrategy DefaultExecutionStrategy { get; }
}
