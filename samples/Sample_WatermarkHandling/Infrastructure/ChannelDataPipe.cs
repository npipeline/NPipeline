using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;

namespace Sample_WatermarkHandling.Infrastructure;

/// <summary>
///     High-performance data pipe implementation using Channel&lt;T&gt;
///     for concurrent data streaming.
///     Provides lock-free producer-consumer pattern with configurable backpressure handling.
/// </summary>
/// <typeparam name="T">The type of data flowing through the pipe</typeparam>
public sealed class ChannelDataPipe<T> : IDataPipe<T>, IAsyncDisposable
{
    private readonly Channel<T> _channel;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new ChannelDataPipe with the specified channel and stream name.
    /// </summary>
    /// <param name="channel">The underlying channel for data flow</param>
    /// <param name="streamName">Optional name for identifying the stream</param>
    public ChannelDataPipe(Channel<T> channel, string? streamName = null)
    {
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        StreamName = streamName ?? $"Channel-{Guid.NewGuid():N}";
        _disposed = false;
    }

    /// <summary>
    ///     Gets the completion task for the channel reader.
    /// </summary>
    public Task Completion => _channel.Reader.Completion;

    /// <summary>
    ///     Gets whether the channel has been completed.
    /// </summary>
    public bool IsCompleted => _channel.Reader.Completion.IsCompleted;

    /// <summary>
    ///     Gets the name of the stream.
    /// </summary>
    public string StreamName { get; }

    /// <summary>
    ///     Gets the data type of the pipe.
    /// </summary>
    /// <returns>The type of data flowing through the pipe.</returns>
    public Type GetDataType()
    {
        return typeof(T);
    }

    /// <summary>
    ///     Converts the pipe to a non-generic async enumerable for framework compatibility.
    ///     This method is for internal framework use and should not be called directly.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable of untyped objects.</returns>
    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        await foreach (var item in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    ///     Asynchronously disposes of the channel and completes all pending operations.
    /// </summary>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        // Signal completion to all readers
        _ = _channel.Writer.TryComplete();

        // Wait for all items to be consumed
        await _channel.Reader.Completion;
        _disposed = true;
    }

    /// <summary>
    ///     Gets an async enumerator for the channel.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerator for the channel.</returns>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return GetAsyncEnumerable(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    /// <summary>
    ///     Provides async enumeration of items from the channel.
    ///     Uses efficient channel reader for lock-free consumption.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable of items from the channel.</returns>
    public async IAsyncEnumerable<T> GetAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        await foreach (var item in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    ///     Reads a single item from the channel asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the read operation.</returns>
    public async Task<T?> ReadAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        return await _channel.Reader.ReadAsync(cancellationToken);
    }

    /// <summary>
    ///     Writes an item to the channel asynchronously.
    /// </summary>
    /// <param name="item">The item to write to the channel.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the write operation.</returns>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        await _channel.Writer.WriteAsync(item, cancellationToken);
    }

    /// <summary>
    ///     Attempts to write an item to the channel without blocking.
    ///     Returns false if the channel is full (bounded channels only).
    /// </summary>
    /// <param name="item">The item to write to the channel.</param>
    /// <returns>true if the item was written; false if the channel is full.</returns>
    public bool TryWrite(T item)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        return _channel.Writer.TryWrite(item);
    }
}
