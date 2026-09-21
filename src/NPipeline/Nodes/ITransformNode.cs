using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Represents a transform node in the pipeline, which takes an input, processes it, and produces an output.
/// </summary>
/// <remarks>
///     How a node is run is a property of the graph, not of the node: the execution strategy lives on the
///     <see cref="Graph.NodeDefinition" /> and is set with <c>WithExecutionStrategy</c>. A node type with an inherent
///     default strategy declares it by implementing <see cref="IExecutionStrategyProvider" />.
/// </remarks>
public interface ITransformNode : INode
{
}

/// <summary>
///     Represents a strongly-typed transform node in the pipeline.
/// </summary>
/// <typeparam name="TIn">The type of the input data.</typeparam>
/// <typeparam name="TOut">The type of the output data.</typeparam>
public interface ITransformNode<in TIn, TOut> : ITransformNode
{
    /// <summary>
    ///     Transforms a single input item into an output item asynchronously.
    /// </summary>
    /// <param name="item">The input item to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The transformed output item.</returns>
    /// <remarks>
    ///     Returns <see cref="ValueTask{TResult}" /> rather than <see cref="Task{TResult}" /> so a transform that
    ///     completes synchronously — the common case — allocates nothing per item. An <c>async</c> method returning
    ///     <see cref="ValueTask{TResult}" /> needs no other change; a synchronous one returns
    ///     <c>ValueTask.FromResult(...)</c>.
    /// </remarks>
    ValueTask<TOut> TransformAsync(TIn item, PipelineContext context, CancellationToken cancellationToken);
}
