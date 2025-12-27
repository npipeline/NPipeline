using NPipeline.DataFlow;
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
    ///     Executes the transform node using its execution strategy for testing purposes.
    ///     This simulates how the node would be executed within the pipeline.
    /// </summary>
    /// <typeparam name="TIn">The input type.</typeparam>
    /// <typeparam name="TOut">The output type.</typeparam>
    /// <param name="node">The transform node to execute.</param>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The output data pipe.</returns>
    public static Task<IDataPipe<TOut>> ExecuteWithStrategyAsync<TIn, TOut>(
        this ITransformNode<TIn, TOut> node,
        IDataPipe<TIn> input,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        return node.ExecutionStrategy.ExecuteAsync(input, node, context, cancellationToken);
    }

    /// <summary>
    ///     Directly tests the ExecuteAsync method of a transform node.
    /// </summary>
    /// <typeparam name="TIn">The input type.</typeparam>
    /// <typeparam name="TOut">The output type.</typeparam>
    /// <param name="node">The transform node to test.</param>
    /// <param name="item">The input item to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The transformed output item.</returns>
    public static Task<TOut> ExecuteAsync<TIn, TOut>(
        this ITransformNode<TIn, TOut> node,
        TIn item,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        return node.ExecuteAsync(item, context, cancellationToken);
    }
}
