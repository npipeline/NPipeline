using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A specialized node that taps into data flow to send copies to a sink for monitoring purposes.
///     This allows for side-channel processing (monitoring, logging, etc.) without affecting the main data flow.
///     For general-purpose branching to multiple pathways, use <see cref="BranchNode{T}" /> instead.
/// </summary>
/// <typeparam name="T">The type of data being processed.</typeparam>
/// <param name="sink">The sink node to send data copies to.</param>
/// <remarks>
///     The sink is driven by a single stream: it receives one <see cref="ISinkNode{T}.ConsumeAsync" /> call for the
///     whole input, not one per item. Items are buffered in a bounded channel between the main path and the sink, so
///     a slow or absent sink applies backpressure rather than buffering the stream without limit.
/// </remarks>
public sealed class TapNode<T>(ISinkNode<T> sink) : IStreamTransformNode<T, T>, IExecutionStrategyProvider, IAsyncDisposable
{
    private const int BufferCapacity = 256;
    private readonly ISinkNode<T> _sink = sink ?? throw new ArgumentNullException(nameof(sink));

    /// <inheritdoc />
    public IExecutionStrategy DefaultExecutionStrategy => StreamPassthroughExecutionStrategy.Instance;

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
    public async IAsyncEnumerable<T> TransformAsync(
        IAsyncEnumerable<T> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(context);

        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(BufferCapacity)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        var sinkTask = _sink.ConsumeAsync(
            new DataStream<T>(channel.Reader.ReadAllAsync(cancellationToken), "TapStream"), context, cancellationToken);

        // If the sink finishes or fails early, close the channel so writes stop instead of blocking forever.
        _ = sinkTask.ContinueWith(static (t, state) =>
        {
            _ = t.Exception; // observe a sink failure nobody awaited, such as during teardown
            ((ChannelWriter<T>)state!).TryComplete();
        }, channel.Writer, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        var feeding = true;

        try
        {
            await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (feeding)
                    feeding = await TryFeedAsync(channel.Writer, sinkTask, item, cancellationToken).ConfigureAwait(false);

                yield return item;
            }

            _ = channel.Writer.TryComplete();
            await sinkTask.ConfigureAwait(false); // surface sink failures; wait for the sink to flush
        }
        finally
        {
            _ = channel.Writer.TryComplete();
        }
    }

    // Returns false once the sink has stopped reading. Rethrows the sink's exception if it failed.
    private static async ValueTask<bool> TryFeedAsync(ChannelWriter<T> writer, Task sinkTask, T item, CancellationToken ct)
    {
        try
        {
            await writer.WriteAsync(item, ct).ConfigureAwait(false);
            return true;
        }
        catch (ChannelClosedException)
        {
            await sinkTask.ConfigureAwait(false); // throws if the sink faulted
            return false; // the sink returned early: stop feeding it, keep passing items on
        }
    }
}
