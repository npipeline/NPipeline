using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     An implementation of <see cref="IDataPipe{T}" /> that wraps an in-memory list of items.
///     This is useful for testing or for scenarios where the entire dataset is already in memory.
/// </summary>
/// <typeparam name="T">The type of data held by the pipe.</typeparam>
public sealed class ListDataPipe<T>(IReadOnlyList<T> items, string streamName = "") : IDataPipe<T>, IAsyncDisposable
{
    /// <summary>
    ///     The underlying list of items.
    /// </summary>
    public IReadOnlyList<T> Items { get; } = items ?? throw new ArgumentNullException(nameof(items));

    /// <inheritdoc />
    public string StreamName { get; } = streamName;

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return ToAsyncEnumerableTyped(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

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
        return ValueTask.CompletedTask;
    }

    private async IAsyncEnumerable<T> ToAsyncEnumerableTyped([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var count = 0;
        foreach (var item in Items)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Yield periodically to allow cancellation processing without per-item overhead
            if (++count % 100 == 0)
            {
                await Task.Yield();
            }

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
