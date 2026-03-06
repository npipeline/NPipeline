using System.Data;
using MySqlConnector;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.MySql.Connection;

/// <summary>
///     MySQL implementation of <see cref="IDatabaseCommand" />.
///     Wraps <see cref="MySqlCommand" /> for database-agnostic operations.
/// </summary>
internal sealed class MySqlDatabaseCommand(MySqlCommand command) : IDatabaseCommand
{
    /// <summary>
    ///     Gets the underlying <see cref="MySqlCommand" /> for MySQL-specific parameter access.
    /// </summary>
    internal MySqlCommand UnderlyingCommand { get; } = command ?? throw new ArgumentNullException(nameof(command));

    /// <inheritdoc />
    public string CommandText
    {
        get => UnderlyingCommand.CommandText;
        set => UnderlyingCommand.CommandText = value;
    }

    /// <inheritdoc />
    public int CommandTimeout
    {
        get => UnderlyingCommand.CommandTimeout;
        set => UnderlyingCommand.CommandTimeout = value;
    }

    /// <inheritdoc />
    public CommandType CommandType
    {
        get => UnderlyingCommand.CommandType;
        set => UnderlyingCommand.CommandType = value;
    }

    /// <inheritdoc />
    public void AddParameter(string name, object? value)
    {
        _ = UnderlyingCommand.Parameters.AddWithValue(name, value ?? DBNull.Value);
    }

    /// <inheritdoc />
    public async Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
    {
        var reader = await UnderlyingCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return new MySqlDatabaseReader(reader);
    }

    /// <inheritdoc />
    public async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        return await UnderlyingCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await UnderlyingCommand.DisposeAsync().ConfigureAwait(false);
    }
}
