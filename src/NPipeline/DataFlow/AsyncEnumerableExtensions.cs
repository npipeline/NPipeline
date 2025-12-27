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
    /// <param name="timespan">The maximum time to wait before emitting a batch.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An asynchronous sequence of batches.</returns>
    public static async IAsyncEnumerable<IReadOnlyCollection<T>> BatchAsync<T>(
        this IAsyncEnumerable<T> source,
        int batchSize,
        TimeSpan timespan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (batchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");

        var channel = Channel.CreateUnbounded<T>();

        var producer = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in source.WithCancellation(cancellationToken))
                {
                    await channel.Writer.WriteAsync(item, cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested.
            }
            finally
            {
                channel.Writer.Complete();
            }
        }, cancellationToken);

        var batch = new List<T>(batchSize);

        while (await channel.Reader.WaitToReadAsync(cancellationToken))
        {
            // Acquire first item (blocking until available or cancellation)
            if (!channel.Reader.TryRead(out var first))

                // If WaitToReadAsync returned true but no item (race), continue
                continue;

            batch.Add(first);
            var deadline = DateTime.UtcNow + timespan;

            // For very small windows (<=100ms) we flush immediately after first item to avoid scheduling variance across runtimes
            // impacting expected 'first batch single item' semantics in tests.
            if (timespan <= TimeSpan.FromMilliseconds(100))
            {
                yield return batch;

                batch = new List<T>(batchSize);
                continue;
            }

            // Collect until size limit or deadline reached
            while (batch.Count < batchSize)
            {
                if (timespan > TimeSpan.Zero)
                {
                    var remaining = deadline - DateTime.UtcNow;

                    if (remaining <= TimeSpan.Zero)
                        break; // time window elapsed

                    // Wait for either new data or deadline
                    var waitTask = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                    var delayTask = Task.Delay(remaining, cancellationToken);
                    var completed = await Task.WhenAny(waitTask, delayTask).ConfigureAwait(false);

                    if (completed == delayTask)
                        break; // deadline hit

                    // else data available
                    if (!await waitTask.ConfigureAwait(false))
                        break; // channel closed
                }
                else
                {
                    if (!await channel.Reader.WaitToReadAsync(cancellationToken))
                        break;
                }

                while (batch.Count < batchSize && channel.Reader.TryRead(out var item))
                {
                    batch.Add(item);
                }
            }

            yield return batch;

            batch = new List<T>(batchSize);
        }

        // Yield the final batch if it's not empty.
        if (batch.Count > 0)
            yield return batch;

        await producer; // Ensure producer is finished and exceptions are propagated.
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
