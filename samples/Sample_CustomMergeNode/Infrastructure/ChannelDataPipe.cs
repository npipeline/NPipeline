using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NPipeline.DataFlow;

namespace Sample_CustomMergeNode.Infrastructure;

/// <summary>
///     High-performance data pipe implementation using Channel
///     <T>
///         for concurrent data streaming.
///         Provides lock-free producer-consumer pattern with configurable backpressure handling.
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

    public string StreamName { get; }

    public Type GetDataType()
    {
        return typeof(T);
    }

    /// <summary>
    ///     Converts the pipe to a non-generic async enumerable for framework compatibility.
    ///     This method is for internal framework use and should not be called directly.
    /// </summary>
    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        await foreach (var item in _channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    ///     Disposes the channel and completes all pending operations.
    /// </summary>
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
    ///     Gets the async enumerator for the channel.
    /// </summary>
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return GetAsyncEnumerable(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    /// <summary>
    ///     Provides async enumeration of items from the channel.
    ///     Uses efficient channel reader for lock-free consumption.
    /// </summary>
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
    public async Task<T?> ReadAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        return await _channel.Reader.ReadAsync(cancellationToken);
    }

    /// <summary>
    ///     Writes an item to the channel asynchronously.
    /// </summary>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        await _channel.Writer.WriteAsync(item, cancellationToken);
    }

    /// <summary>
    ///     Attempts to write an item to the channel without blocking.
    ///     Returns false if the channel is full (bounded channels only).
    /// </summary>
    public bool TryWrite(T item)
    {
        ObjectDisposedException.ThrowIf(_disposed, nameof(ChannelDataPipe<T>));

        return _channel.Writer.TryWrite(item);
    }
}
