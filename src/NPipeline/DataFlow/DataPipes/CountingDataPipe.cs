using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     Internal stats holder used for atomic total item counting.
///     Stored in PipelineContext.Items under the key "stats.totalProcessedItems".
/// </summary>
internal sealed class StatsCounter
{
    public long Total;
}

/// <summary>
///     IDataPipe wrapper that increments a shared counter each time an item is yielded.
///     Placed before multicast so each produced item is counted exactly once regardless of subscribers.
/// </summary>
internal sealed class CountingDataPipe<T> : DataPipeBase<T>
{
    private readonly PipelineContext? _context;
    private readonly StatsCounter _counter;

    public CountingDataPipe(IDataPipe<T> inner, StatsCounter counter, PipelineContext? context = null)
        : base(inner)
    {
        _counter = counter ?? throw new ArgumentNullException(nameof(counter));
        _context = context;
    }

    public override string StreamName => $"Counted_{Inner.StreamName}";

    public override IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return EnumerateWithCounting(Inner, _counter, _context, cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    public async IAsyncEnumerable<T> WithCancellation([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var enumerator = Inner.WithCancellation(cancellationToken).GetAsyncEnumerator();

        while (true)
        {
            T item;

            try
            {
                if (!await enumerator.MoveNextAsync())
                    break;

                item = enumerator.Current;
            }
            catch (Exception ex)
            {
                if (ex is RetryExhaustedException retryEx)
                {
                    // Store the RetryExhaustedException in the context for downstream nodes to access
                    // This helps downstream nodes (like sinks) know that the failure was due to an upstream retry exhaustion
                    if (_context is not null)
                        _context.Items[PipelineContextKeys.LastRetryExhaustedException] = retryEx;

                    ExceptionDispatchInfo.Capture(retryEx).Throw();
                    yield break; // This will never be reached but required for compiler
                }

                ExceptionDispatchInfo.Capture(ex).Throw();
                yield break; // This will never be reached but required for compiler
            }

            Interlocked.Increment(ref _counter.Total);
            yield return item;
        }
    }

    private static async IAsyncEnumerable<T> EnumerateWithCounting(
        IDataPipe<T> inner,
        StatsCounter counter,
        PipelineContext? context,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var enumerator = inner.WithCancellation(ct).GetAsyncEnumerator();
        var exceptionToThrow = null as Exception;
        var items = new List<T>();

        try
        {
            while (await enumerator.MoveNextAsync())
            {
                var item = enumerator.Current;
                Interlocked.Increment(ref counter.Total);
                items.Add(item);
            }
        }
        catch (Exception ex)
        {
            if (ex is RetryExhaustedException retryEx)
            {
                // Store the RetryExhaustedException in the context for downstream nodes to access
                // This helps downstream nodes (like sinks) know that the failure was due to an upstream retry exhaustion
                if (context is not null)
                    context.Items[PipelineContextKeys.LastRetryExhaustedException] = retryEx;

                exceptionToThrow = retryEx;
            }
            else
                exceptionToThrow = ex;
        }
        finally
        {
            await enumerator.DisposeAsync();
        }

        // Only yield collected items if the exception is not a RetryExhaustedException
        // This ensures that when a transform fails with RetryExhaustedException, the sink doesn't try to process any data
        if (exceptionToThrow is not RetryExhaustedException)

            // Yield all collected items outside the try-catch block
        {
            foreach (var item in items)
            {
                yield return item;
            }
        }

        if (exceptionToThrow != null)

            // Preserve the stack trace when re-throwing
            ExceptionDispatchInfo.Capture(exceptionToThrow).Throw();
    }
}
