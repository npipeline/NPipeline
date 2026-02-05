namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Database reader abstraction for reading query results.
/// </summary>
public interface IDatabaseReader : IAsyncDisposable
{
    /// <summary>
    ///     Gets a value indicating whether reader has rows.
    /// </summary>
    bool HasRows { get; }

    /// <summary>
    ///     Gets number of columns in current row.
    /// </summary>
    int FieldCount { get; }

    /// <summary>
    ///     Gets column name by ordinal position.
    /// </summary>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The column name.</returns>
    string GetName(int ordinal);

    /// <summary>
    ///     Gets column type by ordinal position.
    /// </summary>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The column type.</returns>
    Type GetFieldType(int ordinal);

    /// <summary>
    ///     Advances reader to next row.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if there are more rows, false otherwise.</returns>
    Task<bool> ReadAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Advances reader to next result set.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if there are more result sets, false otherwise.</returns>
    Task<bool> NextResultAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets field value by ordinal position.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The field value.</returns>
    T? GetFieldValue<T>(int ordinal);

    /// <summary>
    ///     Checks if field value is DBNull.
    /// </summary>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>True if value is DBNull, false otherwise.</returns>
    bool IsDBNull(int ordinal);
}
