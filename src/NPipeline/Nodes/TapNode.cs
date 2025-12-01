using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A specialized node that taps into data flow to send copies to a sink for monitoring purposes.
///     This allows for side-channel processing (monitoring, logging, etc.) without affecting the main data flow.
///     For general-purpose branching to multiple pathways, use <see cref="BranchNode{T}" /> instead.
/// </summary>
/// <typeparam name="T">The type of data being processed.</typeparam>
/// <param name="sink">The sink node to send data copies to.</param>
public sealed class TapNode<T>(ISinkNode<T> sink) : TransformNode<T, T>
{
    private readonly ISinkNode<T> _sink = sink;

    /// <inheritdoc />
    public override async Task<T> ExecuteAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Send a copy to the sink
        await using (var singlePipe = new InMemoryDataPipe<T>([item]))
        {
            await _sink.ExecuteAsync(
                singlePipe,
                context,
                cancellationToken).ConfigureAwait(false);
        }

        // Return the original item unchanged to the main pipeline
        return item;
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await _sink.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
