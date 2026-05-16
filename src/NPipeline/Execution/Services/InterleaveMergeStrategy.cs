using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Execution.Services;

/// <summary>
///     Implements interleave merge strategy for merging multiple data streams.
/// </summary>
public sealed class InterleaveMergeStrategy<T> : IMergeStrategy<T>
{
    /// <inheritdoc />
    public IDataStream<T> Merge(IEnumerable<IDataStream<T>> pipes, CancellationToken cancellationToken)
    {
        var typedPipes = pipes as IReadOnlyList<IDataStream<T>> ?? pipes.ToList();
        var mergedStream = InterleaveBounded(typedPipes, null, cancellationToken);
        return new DataStream<T>(mergedStream, "InterleavedStream");
    }

    private static async IAsyncEnumerable<T> InterleaveBounded(
        IReadOnlyList<IDataStream<T>> dataStreams,
        int? capacity = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = capacity is { } c and > 0
            ? Channel.CreateBounded<T>(new BoundedChannelOptions(c)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            })
            : Channel.CreateUnbounded<T>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        var producerTasks = dataStreams
            .Select(dataStream => Task.Run(async () =>
            {
                await foreach (var item in dataStream.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    if (!await channel.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
                        break;

                    await channel.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                }
            }, cancellationToken))
            .ToList();

        _ = Task.WhenAll(producerTasks).ContinueWith(
            t => channel.Writer.TryComplete(t.Exception),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }
}
