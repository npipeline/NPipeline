namespace NPipeline.Connectors.Abstractions;

/// <summary>
///     Defines a contract for writing data to a database.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
public interface IDatabaseWriter<T> : IAsyncDisposable
{
    /// <summary>
    ///     Writes a single item to the database.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WriteAsync(T item, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Writes a batch of items to the database.
    /// </summary>
    /// <param name="items">The items to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Flushes any buffered data to the database.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task FlushAsync(CancellationToken cancellationToken = default);
}
