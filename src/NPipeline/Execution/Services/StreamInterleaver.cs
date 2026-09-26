using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace NPipeline.Execution.Services;

/// <summary>
///     Merges several async streams into one, in arrival order. The first failure surfaces at once with its original
///     type and stops the other producers. Consumer exit also stops them. Buffering is bounded.
/// </summary>
/// <remarks>
///     Disposing the merged enumerator waits for every producer to stop, so each source must observe the enumerator
///     cancellation token. A source that ignores it delays disposal until its next item arrives.
/// </remarks>
internal static class StreamInterleaver
{
    public const int DefaultCapacity = 1024;

    public static async IAsyncEnumerable<T> Interleave<T>(
        IReadOnlyList<IAsyncEnumerable<T>> sources,
        int? capacity,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(sources);
        if (sources.Count == 0)
            yield break;

        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity is > 0 ? capacity.Value : DefaultCapacity)
        {
            SingleReader = true,
            SingleWriter = sources.Count == 1,
            FullMode = BoundedChannelFullMode.Wait,
        });

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = cts.Token;
        var remaining = sources.Count;
        var producers = new Task[sources.Count];

        for (var i = 0; i < sources.Count; i++)
        {
            var source = sources[i];
            producers[i] = Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in source.WithCancellation(token).ConfigureAwait(false))
                    {
                        if (!channel.Writer.TryWrite(item))
                            await channel.Writer.WriteAsync(item, token).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
                {
                    return; // stopped by the consumer leaving or by a failing sibling
                }
                catch (Exception ex)
                {
                    _ = channel.Writer.TryComplete(ex); // first failure wins, original type
                    cts.Cancel();                       // stop the siblings
                    return;
                }

                if (Interlocked.Decrement(ref remaining) == 0)
                    _ = channel.Writer.TryComplete();
            }, CancellationToken.None);
        }

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return item;
        }
        finally
        {
            cts.Cancel();
            try
            {
                await Task.WhenAll(producers).ConfigureAwait(false);
            }
            catch
            {
                // Failures already surfaced through the channel.
            }
        }
    }
}