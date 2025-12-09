using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Represents a stream-based transform node in the pipeline, which takes an input stream, processes it, and produces an output stream.
/// </summary>
public interface IStreamTransformNode : INode
{
    /// <summary>
    ///     Gets or sets the execution strategy for this node.
    /// </summary>
    IExecutionStrategy ExecutionStrategy { get; set; }

    /// <summary>
    ///     Gets or sets the error handler for this node.
    /// </summary>
    INodeErrorHandler? ErrorHandler { get; set; }
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
    IAsyncEnumerable<TOut> ExecuteAsync(IAsyncEnumerable<TIn> items, PipelineContext context, CancellationToken cancellationToken);
}
