using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     An implementation of <see cref="IDataPipe{T}" /> that wraps an in-memory list of items.
///     This is useful for testing or for scenarios where the entire dataset is already in memory.
/// </summary>
/// <remarks>
///     <para>
///         The pipe periodically yields control via <see cref="Task.Yield" /> to allow cancellation processing
///         without incurring overhead on every item. The frequency of yielding can be customized via the
///         <paramref name="yieldIntervalItems" /> parameter.
///     </para>
/// </remarks>
/// <typeparam name="T">The type of data held by the pipe.</typeparam>
public sealed class InMemoryDataPipe<T>(IReadOnlyList<T> items, string streamName = "", int yieldIntervalItems = 100)
    : IDataPipe<T>, IAsyncDisposable, IDisposable
{
    /// <summary>
    ///     The underlying list of items.
    /// </summary>
    public IReadOnlyList<T> Items { get; } = items ?? throw new ArgumentNullException(nameof(items));

    /// <summary>
    ///     The number of items to process before yielding control to allow cancellation processing.
    /// </summary>
    /// <remarks>
    ///     This value controls how often <see cref="Task.Yield" /> is called during enumeration.
    ///     Lower values (e.g., 10) increase responsiveness to cancellation but incur more overhead.
    ///     Higher values (e.g., 1000) reduce overhead but may delay cancellation processing.
    ///     The default of 100 provides a good balance for most scenarios.
    /// </remarks>
    public int YieldIntervalItems { get; } = yieldIntervalItems > 0
        ? yieldIntervalItems
        : throw new ArgumentException("Yield interval must be greater than zero.", nameof(yieldIntervalItems));

    /// <inheritdoc />
    public string StreamName { get; } = streamName;

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return ToAsyncEnumerableTyped(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    /// <summary>
    ///     Gets the data type of items carried by this pipe.
    /// </summary>
    /// <returns>The <see cref="Type" /> of data items in this pipe.</returns>
    public Type GetDataType()
    {
        return typeof(T);
    }

    /// <summary>
    ///     Internal method to get a non-generic async enumerable from this pipe.
    ///     This provides efficient untyped access for internal framework code.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable of untyped objects.</returns>
    public IAsyncEnumerable<object?> ToAsyncEnumerable(CancellationToken cancellationToken = default)
    {
        return CastToObject(ToAsyncEnumerableTyped(cancellationToken), cancellationToken);
    }

    /// <summary>
    ///     Asynchronously disposes of the data pipe. This implementation does nothing as there are no unmanaged resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public ValueTask DisposeAsync()
    {
        // Nothing to dispose for an in-memory list
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Disposes of the data pipe. This implementation calls DisposeAsync() and suppresses finalization.
    /// </summary>
    void IDisposable.Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private async IAsyncEnumerable<T> ToAsyncEnumerableTyped([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var count = 0;

        foreach (var item in Items)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Yield periodically to allow cancellation processing without per-item overhead
            if (++count % YieldIntervalItems == 0)
                await Task.Yield();

            yield return item;
        }
    }

    private static async IAsyncEnumerable<object> CastToObject<TSource>(IAsyncEnumerable<TSource> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            yield return item!;
        }
    }
}
