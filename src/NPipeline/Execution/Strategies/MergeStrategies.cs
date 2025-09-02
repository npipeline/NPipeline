using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.Nodes;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     Provides different strategies for merging multiple <see cref="IAsyncEnumerable{T}" /> streams into one.
/// </summary>
public static class MergeStrategies
{
    /// <summary>
    ///     Merges multiple asynchronous streams into a single stream by concatenating them.
    ///     It processes all items from the first stream, then all from the second, and so on.
    /// </summary>
    public static async IAsyncEnumerable<T> Concatenate<T>(
        IEnumerable<IDataPipe> dataPipes,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var dataPipe in dataPipes)
        {
            if (dataPipe is not IDataPipe<T> typedPipe)
                throw new InvalidCastException($"Cannot concatenate streams. Expected pipe of '{typeof(T).Name}', but found '{dataPipe.GetType().Name}'.");

            await foreach (var item in typedPipe.WithCancellation(cancellationToken))
            {
                yield return item;
            }
        }
    }

    /// <summary>
    ///     Merges multiple asynchronous streams into a single stream by interleaving their items.
    ///     Items are yielded as they become available from any of the source streams.
    ///     This is ideal for responsive, real-time processing.
    /// </summary>
    public static async IAsyncEnumerable<T> Interleave<T>(
        IEnumerable<IDataPipe> dataPipes,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in InterleaveBounded<T>(dataPipes, null, cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    // New bounded variant for performance control
    public static async IAsyncEnumerable<T> InterleaveBounded<T>(
        IEnumerable<IDataPipe> dataPipes,
        int? capacity = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = capacity is { } c && c > 0
            ? Channel.CreateBounded<T>(new BoundedChannelOptions(c)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            })
            : Channel.CreateUnbounded<T>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        var producerTasks = dataPipes
            .Select(dataPipe => Task.Run(async () =>
            {
                if (dataPipe is not IDataPipe<T> typedPipe)
                {
                    channel.Writer.TryComplete(
                        new InvalidCastException($"Cannot interleave streams. Expected pipe of '{typeof(T).Name}', but found '{dataPipe.GetType().Name}'."));

                    return;
                }

                await foreach (var item in typedPipe.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    if (!await channel.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
                        break;

                    await channel.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                }
            }, cancellationToken))
            .ToList();

        _ = Task.WhenAll(producerTasks).ContinueWith(
            t => channel.Writer.TryComplete(t.Exception?.InnerException),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>
    ///     Allows applying a custom merge function to a set of asynchronous streams.
    ///     This provides maximum flexibility for complex merge scenarios.
    ///     This is a generic helper method intended to be used within an <see cref="ICustomMergeNode{TIn}" /> implementation.
    /// </summary>
    /// <typeparam name="T">The type of data in the streams.</typeparam>
    /// <param name="mergeFunc">The custom function to apply to the streams.</param>
    /// <param name="sources">The source streams to merge.</param>
    /// <returns>A single merged asynchronous stream.</returns>
    public static IAsyncEnumerable<T> Custom<T>(
        Func<IAsyncEnumerable<T>[], IAsyncEnumerable<T>> mergeFunc,
        params IAsyncEnumerable<T>[] sources)
    {
        return mergeFunc(sources);
    }
}
