using NPipeline.DataFlow;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Represents a strongly-typed source node in the pipeline, which produces data.
/// </summary>
/// <typeparam name="TOut">The type of the output data.</typeparam>
public interface ISourceNode<out TOut> : INode
{
    /// <summary>
    ///     Executes the source node.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IDataPipe{TOut}" /> that produces the output data for downstream nodes.</returns>
    IDataPipe<TOut> Execute(PipelineContext context, CancellationToken cancellationToken);
}
