using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     An implementation of <see cref="IDataPipe" /> that enables true, non-buffering streaming.
///     It wraps an <see cref="IAsyncEnumerable{T}" /> and processes items lazily as they are requested by the consumer.
///     This is the recommended implementation for all transform nodes to ensure low memory overhead.
/// </summary>
public sealed class StreamingDataPipe<T>(IAsyncEnumerable<T> stream, string streamName = "DefaultStream") : IDataPipe<T>, IStreamingDataPipe, IAsyncDisposable
{
    private readonly IAsyncEnumerable<T> _stream = stream ?? throw new ArgumentNullException(nameof(stream));
    private bool _disposed;

    /// <summary>
    ///     Gets the name of the stream for identification purposes.
    /// </summary>
    public string StreamName { get; } = streamName;

    /// <summary>
    ///     Gets the enumerator that iterates asynchronously through the stream.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An asynchronous enumerator.</returns>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _stream.GetAsyncEnumerator(cancellationToken);
    }

    /// <summary>
    ///     Internal method to get a non-generic async enumerable from this stream.
    ///     This provides efficient untyped access for internal framework code without reflection.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable of untyped objects.</returns>
    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await foreach (var item in _stream.WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    ///     Gets the data type of the items in this data pipe.
    /// </summary>
    /// <returns>The type of data items in the pipe.</returns>
    public Type GetDataType()
    {
        return typeof(T);
    }

    /// <summary>
    ///     Asynchronously disposes of the data pipe. If the underlying stream implements <see cref="IAsyncDisposable" />, it will be disposed as well.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_stream is IAsyncDisposable disposable)
            await disposable.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Returns an async enumerable that supports cancellation.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable with cancellation support.</returns>
    public async IAsyncEnumerable<T> WithCancellation([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await foreach (var item in _stream.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }
}
