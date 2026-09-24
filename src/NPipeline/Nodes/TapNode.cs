using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A specialized node that taps into data flow to send copies to a sink for monitoring purposes.
///     This allows for side-channel processing (monitoring, logging, etc.) without affecting the main data flow.
///     For general-purpose branching to multiple pathways, use <see cref="BranchNode{T}" /> instead.
/// </summary>
/// <typeparam name="T">The type of data being processed.</typeparam>
/// <param name="sink">The sink node to send data copies to.</param>
public sealed class TapNode<T>(ISinkNode<T> sink) : TransformNode<T, T>, IAsyncDisposable
{
    private readonly ISinkNode<T> _sink = sink;

    /// <summary>
    ///     Disposes the sink this node taps into, if the sink owns resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        switch (_sink)
        {
            case IAsyncDisposable asyncDisposable:
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
                break;
            case IDisposable disposable:
                disposable.Dispose();
                break;
        }
    }

    /// <inheritdoc />
    public override async ValueTask<T> TransformAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Send a copy to the sink
        var singlePipe = new InMemoryDataStream<T>([item]);

        await using (((IAsyncDisposable)singlePipe).ConfigureAwait(false))
        {
            await _sink.ConsumeAsync(
                singlePipe,
                context,
                cancellationToken).ConfigureAwait(false);
        }

        // Return the original item unchanged to the main pipeline
        return item;
    }
}
