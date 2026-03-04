using MySqlConnector;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.MySql.Connection;

/// <summary>
///     MySQL implementation of <see cref="IDatabaseReader" />.
///     Wraps <see cref="MySqlDataReader" /> for database-agnostic operations.
/// </summary>
internal sealed class MySqlDatabaseReader(MySqlDataReader reader) : IDatabaseReader
{
    /// <summary>
    ///     Gets the underlying <see cref="MySqlDataReader" /> for MySQL-specific operations.
    /// </summary>
    internal MySqlDataReader Reader { get; } =
        reader ?? throw new ArgumentNullException(nameof(reader));

    /// <inheritdoc />
    public bool HasRows => Reader.HasRows;

    /// <inheritdoc />
    public int FieldCount => Reader.FieldCount;

    /// <inheritdoc />
    public string GetName(int ordinal) => Reader.GetName(ordinal);

    /// <inheritdoc />
    public Type GetFieldType(int ordinal) => Reader.GetFieldType(ordinal);

    /// <inheritdoc />
    public Task<bool> ReadAsync(CancellationToken cancellationToken = default) =>
        Reader.ReadAsync(cancellationToken);

    /// <inheritdoc />
    public Task<bool> NextResultAsync(CancellationToken cancellationToken = default) =>
        Reader.NextResultAsync(cancellationToken);

    /// <inheritdoc />
    public T? GetFieldValue<T>(int ordinal) =>
        Reader.IsDBNull(ordinal) ? default : Reader.GetFieldValue<T>(ordinal);

    /// <inheritdoc />
    public bool IsDBNull(int ordinal) => Reader.IsDBNull(ordinal);

    /// <inheritdoc />
    public object? GetValue(int ordinal) =>
        Reader.IsDBNull(ordinal) ? null : Reader.GetValue(ordinal);

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await Reader.DisposeAsync().ConfigureAwait(false);
    }
}
