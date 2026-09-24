using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Data pipe that combines counting with passthrough (no branching).
///     This eliminates one layer of wrapping when no multicast is needed.
/// </summary>
internal sealed class CountingPassthroughDataStream<T> : IForwardOnlyDataStream<T>
{
    private readonly StatsCounter _counter;
    private readonly IDataStream<T> _inner;
    private bool _disposed;

    public CountingPassthroughDataStream(IDataStream<T> inner, StatsCounter counter)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(counter);
        _inner = inner;
        _counter = counter;
    }

    public string StreamName => $"Counted_{_inner.StreamName}";

    public Type GetDataType() => typeof(T);

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return EnumerateWithCounting(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await foreach (var item in EnumerateWithCounting(cancellationToken).ConfigureAwait(false))
        {
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
#pragma warning disable CA2007

        // CA2007 false positive: the enumerator comes from a ConfigureAwait(false) sequence, so its
        // MoveNextAsync and DisposeAsync already return configured awaitables - the analyzer only
        // recognises ConfigureAwait applied directly to the await using expression.
        await using var enumerator = _inner.WithCancellation(cancellationToken).ConfigureAwait(false).GetAsyncEnumerator();
#pragma warning restore CA2007

        // Counted locally and folded into the shared counter once, so the per-item path carries no
        // atomic and parallel nodes do not contend for the counter's cache line.
        var counted = 0L;

        try
        {
            while (true)
            {
                if (!await enumerator.MoveNextAsync())
                    break;

                counted++;
                yield return enumerator.Current;
            }
        }
        finally
        {
            // Runs on normal completion, on an abandoned enumeration and on a thrown exception.
            _counter.Add(counted);
        }
    }
}
