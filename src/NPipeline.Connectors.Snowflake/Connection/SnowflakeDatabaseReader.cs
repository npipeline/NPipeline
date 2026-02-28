using System.Data.Common;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Connection;

/// <summary>
///     Snowflake implementation of IDatabaseReader.
///     Wraps DbDataReader for database-agnostic operations.
/// </summary>
internal sealed class SnowflakeDatabaseReader(DbDataReader reader) : IDatabaseReader
{
    internal DbDataReader Reader { get; } = reader ?? throw new ArgumentNullException(nameof(reader));

    /// <summary>
    ///     Gets a value indicating whether reader has rows.
    /// </summary>
    public bool HasRows => Reader.HasRows;

    /// <summary>
    ///     Gets number of columns in current row.
    /// </summary>
    public int FieldCount => Reader.FieldCount;

    /// <summary>
    ///     Gets column name by ordinal position.
    /// </summary>
    public string GetName(int ordinal) => Reader.GetName(ordinal);

    /// <summary>
    ///     Gets column type by ordinal position.
    /// </summary>
    public Type GetFieldType(int ordinal) => Reader.GetFieldType(ordinal);

    /// <summary>
    ///     Advances reader to next row.
    /// </summary>
    public Task<bool> ReadAsync(CancellationToken cancellationToken = default) =>
        Reader.ReadAsync(cancellationToken);

    /// <summary>
    ///     Advances reader to next result set.
    /// </summary>
    public Task<bool> NextResultAsync(CancellationToken cancellationToken = default) =>
        Reader.NextResultAsync(cancellationToken);

    /// <summary>
    ///     Gets field value by ordinal position.
    /// </summary>
    public T? GetFieldValue<T>(int ordinal)
    {
        return Reader.IsDBNull(ordinal)
            ? default
            : Reader.GetFieldValue<T>(ordinal);
    }

    /// <summary>
    ///     Checks if field value is DBNull.
    /// </summary>
    public bool IsDBNull(int ordinal) => Reader.IsDBNull(ordinal);

    /// <summary>
    ///     Disposes reader asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await Reader.DisposeAsync().ConfigureAwait(false);
    }
}
