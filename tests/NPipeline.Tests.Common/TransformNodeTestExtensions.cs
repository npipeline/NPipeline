using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Common;

/// <summary>
///     Test-only extension methods for testing transform nodes.
///     Not part of the production API surface.
/// </summary>
public static class TransformNodeTestExtensions
{
    /// <summary>
    ///     Executes the transform node under an execution strategy for testing purposes.
    ///     This simulates how the node would be executed within the pipeline.
    /// </summary>
    /// <typeparam name="TIn">The input type.</typeparam>
    /// <typeparam name="TOut">The output type.</typeparam>
    /// <param name="node">The transform node to execute.</param>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="nodeId">The id the node is executing under; defaults to the context's current node id.</param>
    /// <param name="strategy">The strategy to run the node under; defaults to sequential execution, as the pipeline does.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The output data pipe.</returns>
    public static Task<IDataStream<TOut>> ExecuteWithStrategyAsync<TIn, TOut>(
        this ITransformNode<TIn, TOut> node,
        IDataStream<TIn> input,
        PipelineContext context,
        string? nodeId = null,
        IExecutionStrategy? strategy = null,
        CancellationToken cancellationToken = default)
    {
        var effectiveStrategy = strategy
                                ?? (node as IExecutionStrategyProvider)?.DefaultExecutionStrategy
                                ?? new SequentialExecutionStrategy();

        return effectiveStrategy.ExecuteAsync(input, node, context, nodeId ?? context.NodeEnvironment.CurrentNodeId, cancellationToken);
    }

    /// <summary>
    ///     Directly tests the TransformAsync method of a transform node.
    /// </summary>
    /// <typeparam name="TIn">The input type.</typeparam>
    /// <typeparam name="TOut">The output type.</typeparam>
    /// <param name="node">The transform node to test.</param>
    /// <param name="item">The input item to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The transformed output item.</returns>
    public static ValueTask<TOut> TransformAsync<TIn, TOut>(
        this ITransformNode<TIn, TOut> node,
        TIn item,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        return node.TransformAsync(item, context, cancellationToken);
    }
}
