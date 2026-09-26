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
///     a slow or absent sink applies backpressure rather than buffering the stream without limit. When the tapped
///     stream ends abnormally (an upstream failure, cancellation, or the downstream consumer stopping early), the
///     sink's cancellation token is cancelled instead of its input ending normally.
/// </remarks>
public sealed class TapNode<T>(ISinkNode<T> sink) : IStreamTransformNode<T, T>, IExecutionStrategyProvider, IAsyncDisposable
{
    private const int BufferCapacity = 256;
    private static readonly TimeSpan SinkShutdownTimeout = TimeSpan.FromSeconds(30);
    private readonly ISinkNode<T> _sink = sink ?? throw new ArgumentNullException(nameof(sink));
    private Task? _sinkTask;

    /// <inheritdoc />
    public IExecutionStrategy DefaultExecutionStrategy => StreamPassthroughExecutionStrategy.Instance;

    /// <summary>
    ///     Disposes the sink this node taps into, if the sink owns resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // A tapped stream that ended abnormally cancels its sink without waiting for it; let it stop before disposing.
        if (Volatile.Read(ref _sinkTask) is { IsCompleted: false } sinkTask)
        {
            try
            {
                await sinkTask.WaitAsync(SinkShutdownTimeout).ConfigureAwait(false);
            }
            catch
            {
                // The sink's failure was already observed, or it ignores cancellation; dispose it regardless.
            }
        }

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

        // The sink gets its own token so it can be told the tapped stream ended abnormally, instead of seeing a
        // normal end of stream and, for example, flushing a truncated file as if the run succeeded.
        var sinkCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        Task sinkTask;

        try
        {
            sinkTask = _sink.ConsumeAsync(
                new DataStream<T>(channel.Reader.ReadAllAsync(sinkCts.Token), "TapStream"), context, sinkCts.Token);
        }
        catch
        {
            sinkCts.Dispose();
            throw;
        }

        Volatile.Write(ref _sinkTask, sinkTask);

        // If the sink finishes or fails early, close the channel so writes stop instead of blocking forever. The token
        // source is disposed only once the sink no longer uses it.
        _ = sinkTask.ContinueWith(static (t, state) =>
        {
            _ = t.Exception; // observe a sink failure nobody awaited, such as during teardown
            var (writer, cts) = ((ChannelWriter<T>, CancellationTokenSource))state!;
            _ = writer.TryComplete();
            cts.Dispose();
        }, (channel.Writer, sinkCts), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);

        var feeding = true;
        var completed = false;

        try
        {
            await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (feeding)
                    feeding = await TryFeedAsync(channel.Writer, sinkTask, item, cancellationToken).ConfigureAwait(false);

                yield return item;
            }

            _ = channel.Writer.TryComplete();
            completed = true;
            await sinkTask.ConfigureAwait(false); // surface sink failures; wait for the sink to flush
        }
        finally
        {
            if (!completed)
            {
                // The input failed, was cancelled, or the consumer left early: abort the sink rather than end it.
                // Cancel before completing, so the sink sees the cancellation rather than a normal end of stream.
                CancelSink(sinkTask, sinkCts);
                _ = channel.Writer.TryComplete();
            }
        }
    }

    private static void CancelSink(Task sinkTask, CancellationTokenSource sinkCts)
    {
        if (sinkTask.IsCompleted)
            return; // the continuation may already have disposed the token source

        try
        {
            sinkCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The sink completed between the check and the cancel.
        }
        catch (AggregateException)
        {
            // A cancellation callback registered by the sink threw; the sink's own task reports its failure.
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
