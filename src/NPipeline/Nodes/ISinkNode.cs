using NPipeline.DataFlow;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Represents a strongly-typed sink node in the pipeline, which takes an input and terminates the flow.
/// </summary>
/// <typeparam name="TIn">The type of the input data.</typeparam>
public interface ISinkNode<in TIn> : INode
{
    /// <summary>
    ///     Executes the sink node asynchronously.
    /// </summary>
    /// <param name="input">The input data to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task ExecuteAsync(IDataPipe<TIn> input, PipelineContext context, CancellationToken cancellationToken);
}
