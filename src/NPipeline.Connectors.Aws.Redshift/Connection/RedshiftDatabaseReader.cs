using Npgsql;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift.Connection;

/// <summary>
///     Redshift implementation of IDatabaseReader.
///     Wraps NpgsqlDataReader for database-agnostic operations.
/// </summary>
internal sealed class RedshiftDatabaseReader : IDatabaseReader
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftDatabaseReader" /> class.
    /// </summary>
    /// <param name="reader">The Npgsql data reader to wrap.</param>
    public RedshiftDatabaseReader(NpgsqlDataReader reader)
    {
        Reader = reader ?? throw new ArgumentNullException(nameof(reader));
    }

    /// <summary>
    ///     Gets the underlying NpgsqlDataReader for Redshift-specific operations.
    /// </summary>
    internal NpgsqlDataReader Reader { get; }

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
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The column name.</returns>
    public string GetName(int ordinal)
    {
        return Reader.GetName(ordinal);
    }

    /// <summary>
    ///     Gets column type by ordinal position.
    /// </summary>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The column type.</returns>
    public Type GetFieldType(int ordinal)
    {
        return Reader.GetFieldType(ordinal);
    }

    /// <summary>
    ///     Advances reader to next row.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if there are more rows, false otherwise.</returns>
    public Task<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        return Reader.ReadAsync(cancellationToken);
    }

    /// <summary>
    ///     Advances reader to next result set.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if there are more result sets, false otherwise.</returns>
    public Task<bool> NextResultAsync(CancellationToken cancellationToken = default)
    {
        return Reader.NextResultAsync(cancellationToken);
    }

    /// <summary>
    ///     Gets field value by ordinal position.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The field value.</returns>
    public T? GetFieldValue<T>(int ordinal)
    {
        return Reader.IsDBNull(ordinal)
            ? default
            : Reader.GetFieldValue<T>(ordinal);
    }

    /// <summary>
    ///     Checks if field value is DBNull.
    /// </summary>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>True if value is DBNull, false otherwise.</returns>
    public bool IsDBNull(int ordinal)
    {
        return Reader.IsDBNull(ordinal);
    }

    /// <summary>
    ///     Disposes reader asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await Reader.DisposeAsync().ConfigureAwait(false);
    }
}
