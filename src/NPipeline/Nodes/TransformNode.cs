using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;
using NPipeline.Utils;

namespace NPipeline.Nodes;

/// <summary>
///     A base class for transform nodes.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
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

    /// <summary>
    ///     Gets or sets the execution strategy for this transform node.
    ///     Defaults to <see cref="SequentialExecutionStrategy" />.
    ///     <para>
    ///         Use custom strategies for parallel processing, batching, or other advanced execution patterns.
    ///         Set this property directly or via the fluent API using <c>WithExecutionStrategy</c> extension method.
    ///     </para>
    /// </summary>
    public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();

    /// <inheritdoc />
    public abstract ValueTask<TOut> TransformAsync(TIn item, PipelineContext context, CancellationToken cancellationToken);

    /// <summary>
    ///     Asynchronously disposes of the node. This can be overridden by derived classes to release resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask; // base holds no resources
    }
}
