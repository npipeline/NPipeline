using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     Data pipe that combines counting with passthrough (no branching).
///     This eliminates one layer of wrapping when no multicast is needed.
/// </summary>
internal sealed class CountingPassthroughDataPipe<T> : IDataPipe<T>, IStreamingDataPipe
{
    private readonly PipelineContext? _context;
    private readonly StatsCounter _counter;
    private readonly IDataPipe<T> _inner;
    private bool _disposed;

    public CountingPassthroughDataPipe(IDataPipe<T> inner, StatsCounter counter, PipelineContext? context = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(counter);
        _inner = inner;
        _counter = counter;
        _context = context;
    }

    public string StreamName => $"Counted_{_inner.StreamName}";

    public Type GetDataType()
    {
        return typeof(T);
    }

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return EnumerateWithCounting(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await foreach (var item in EnumerateWithCounting(cancellationToken))
        {
            if (item is not null)
                yield return item;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await _inner.DisposeAsync().ConfigureAwait(false);
    }

    private async IAsyncEnumerable<T> EnumerateWithCounting([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var enumerator = _inner.WithCancellation(cancellationToken).GetAsyncEnumerator();

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
                    if (_context is not null)
                        _context.Items[PipelineContextKeys.LastRetryExhaustedException] = retryEx;

                    ExceptionDispatchInfo.Capture(retryEx).Throw();
                    yield break; // Never reached but required for compiler
                }

                ExceptionDispatchInfo.Capture(ex).Throw();
                yield break; // Never reached but required for compiler
            }

            // Inline counting - no extra wrapper layer
            _ = Interlocked.Increment(ref _counter.GetTotalRef());
            yield return item;
        }
    }
}
