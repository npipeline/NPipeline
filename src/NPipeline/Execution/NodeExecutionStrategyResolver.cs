using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Execution;

/// <summary>
///     Resolves the execution strategy for a node from the graph configuration, falling back to the node's own
///     default and then to sequential execution.
/// </summary>
/// <remarks>
///     Resolution happens per run and per node, and nothing is written back to the node instance, so a node object
///     stays safe to share and the graph remains the single source of truth for how a node is run.
/// </remarks>
internal static class NodeExecutionStrategyResolver
{
    public static IExecutionStrategy Resolve(NodeDefinition definition, INode instance)
    {
        return definition.ExecutionStrategy
               ?? (instance as IExecutionStrategyProvider)?.DefaultExecutionStrategy
               ?? SequentialExecutionStrategy.Instance;
    }

    /// <summary>
    ///     Resolves the strategy for a stream transform node, which must be stream-capable.
    /// </summary>
    public static IStreamExecutionStrategy ResolveStream(NodeDefinition definition, INode instance)
    {
        var strategy = Resolve(definition, instance);

        if (strategy is IStreamExecutionStrategy streamStrategy)
            return streamStrategy;

        throw new InvalidOperationException(ErrorMessages.StreamTransformNodeRequiresStreamStrategy(
            definition.Id,
            instance.GetType().FullName ?? instance.GetType().Name,
            strategy.GetType().FullName ?? strategy.GetType().Name));
    }
}
