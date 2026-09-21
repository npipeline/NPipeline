using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Represents a stream-based transform node in the pipeline, which takes an input stream, processes it, and produces an output stream.
/// </summary>
/// <remarks>
///     How a node is run is a property of the graph, not of the node: the execution strategy lives on the
///     <see cref="Graph.NodeDefinition" /> and is set with <c>WithExecutionStrategy</c>. A node type with an inherent
///     default strategy declares it by implementing <see cref="IExecutionStrategyProvider" />.
/// </remarks>
public interface IStreamTransformNode : INode
{
}

/// <summary>
///     Represents a strongly-typed stream-based transform node in the pipeline.
/// </summary>
/// <typeparam name="TIn">The type of the input data.</typeparam>
/// <typeparam name="TOut">The type of the output data.</typeparam>
public interface IStreamTransformNode<in TIn, TOut> : IStreamTransformNode
{
    /// <summary>
    ///     Transforms an input stream of items into an output stream of items asynchronously.
    /// </summary>
    /// <param name="items">The input stream of items to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The transformed output stream of items.</returns>
    IAsyncEnumerable<TOut> TransformAsync(IAsyncEnumerable<TIn> items, PipelineContext context, CancellationToken cancellationToken);
}
