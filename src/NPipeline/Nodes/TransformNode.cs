using NPipeline.Pipeline;
using NPipeline.Utils;

namespace NPipeline.Nodes;

/// <summary>
///     A base class for transform nodes.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
/// <remarks>
///     A node holds no execution strategy of its own: how the node is run is configured on the graph with
///     <c>WithExecutionStrategy</c> and defaults to <see cref="Execution.Strategies.SequentialExecutionStrategy" />.
/// </remarks>
public abstract class TransformNode<TIn, TOut>
    : ITransformNode<TIn, TOut>, INodeTypeMetadata
{
    /// <summary>
    ///     Gets the input type of the transform node.
    /// </summary>
    public Type InputType => typeof(TIn);

    /// <summary>
    ///     Gets the output type of the transform node.
    /// </summary>
    public Type OutputType => typeof(TOut);

    /// <inheritdoc />
    public abstract ValueTask<TOut> TransformAsync(TIn item, PipelineContext context, CancellationToken cancellationToken);
}
