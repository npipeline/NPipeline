using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace NPipeline.DataFlow;

/// <summary>
///     Extension methods for working with <see cref="IAsyncEnumerable{T}" /> sequences.
/// </summary>
public static class AsyncEnumerableExtensions
{
#if !NET10_0
    /// <summary>
    ///     Asynchronously creates a <see cref="List{T}" /> from an <see cref="IAsyncEnumerable{T}" /> sequence.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <param name="source">The source asynchronous sequence to enumerate.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the sequence to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a list with the elements from the source sequence.</returns>
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);

        var list = new List<T>();

        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            list.Add(item);
        }

        return list;
    }
#endif

    /// <summary>
    ///     Batches the elements of an asynchronous sequence into chunks of a specified size or within a specified time window.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source asynchronous sequence.</param>
    /// <param name="batchSize">The maximum number of elements in a batch.</param>
    /// <param name="timespan">
    ///     The maximum time to wait before emitting a batch, measured from the batch's first item.
    ///     <see cref="TimeSpan.Zero" /> emits whatever is immediately available.
    /// </param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An asynchronous sequence of batches.</returns>
    /// <remarks>
    ///     The producer stops when the consumer leaves early, and a source failure surfaces as-is, including an
    ///     <see cref="OperationCanceledException" /> that did not come from <paramref name="cancellationToken" />.
    /// </remarks>
    public static async IAsyncEnumerable<IReadOnlyCollection<T>> BatchAsync<T>(
        this IAsyncEnumerable<T> source,
        int batchSize,
        TimeSpan timespan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        ArgumentOutOfRangeException.ThrowIfLessThan(timespan, TimeSpan.Zero);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(Math.Max(batchSize * 2, 1))
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        var producer = Task.Run(async () =>
        {
            Exception? error = null;

            try
            {
                await foreach (var item in source.WithCancellation(cts.Token).ConfigureAwait(false))
                    await channel.Writer.WriteAsync(item, cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cts.IsCancellationRequested)
            {
                // Stopped by the consumer or the caller.
            }
            catch (Exception ex)
            {
                error = ex; // includes foreign OperationCanceledExceptions
            }
            finally
            {
                _ = channel.Writer.TryComplete(error);
            }
        }, CancellationToken.None);

        try
        {
            var reader = channel.Reader;

            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false)) // throws the producer's error
            {
                var batch = new List<T>(batchSize);

                while (batch.Count < batchSize && reader.TryRead(out var item))
                    batch.Add(item);

                if (batch.Count == 0)
                    continue;

                // The time window starts at the batch's first item.
                if (batch.Count < batchSize && timespan > TimeSpan.Zero)
                {
                    using var windowCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    windowCts.CancelAfter(timespan);

                    try
                    {
                        while (batch.Count < batchSize && await reader.WaitToReadAsync(windowCts.Token).ConfigureAwait(false))
                        {
                            while (batch.Count < batchSize && reader.TryRead(out var item))
                                batch.Add(item);
                        }
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        // The window elapsed.
                    }
                }

                yield return batch; // List<T> is IReadOnlyCollection<T>; no ToArray copy
            }

            await producer.ConfigureAwait(false);
        }
        finally
        {
            cts.Cancel(); // stop the producer if the consumer left early

            try
            {
                await producer.ConfigureAwait(false);
            }
            catch
            {
                // Already surfaced through the channel, or cancelled.
            }
        }
    }

    /// <summary>
    ///     Flattens a sequence of sequences into a single sequence.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the inner sequences.</typeparam>
    /// <param name="source">An asynchronous sequence of sequences to flatten.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An asynchronous sequence containing the flattened elements.</returns>
    public static async IAsyncEnumerable<T> FlattenAsync<T>(
        this IAsyncEnumerable<IEnumerable<T>> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);

        await foreach (var batch in source.WithCancellation(cancellationToken))
        foreach (var item in batch)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }
}
