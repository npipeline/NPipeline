using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Represents a transform node in the pipeline, which takes an input, processes it, and produces an output.
/// </summary>
public interface ITransformNode : INode
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
    Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken);
}
