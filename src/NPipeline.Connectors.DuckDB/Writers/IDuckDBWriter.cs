namespace NPipeline.Connectors.DuckDB.Writers;

/// <summary>
///     Abstraction for writing rows to DuckDB.
/// </summary>
internal interface IDuckDBWriter<in T> : IAsyncDisposable
{
    /// <summary>
    ///     Writes a single item.
    /// </summary>
    Task WriteAsync(T item, CancellationToken cancellationToken);

    /// <summary>
    ///     Flushes any buffered items.
    /// </summary>
    Task FlushAsync(CancellationToken cancellationToken);
}
