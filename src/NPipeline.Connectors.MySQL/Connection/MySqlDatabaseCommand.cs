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
    private readonly MySqlCommand _command = command ?? throw new ArgumentNullException(nameof(command));

    /// <inheritdoc />
    public string CommandText
    {
        get => _command.CommandText;
        set => _command.CommandText = value;
    }

    /// <inheritdoc />
    public int CommandTimeout
    {
        get => _command.CommandTimeout;
        set => _command.CommandTimeout = value;
    }

    /// <inheritdoc />
    public CommandType CommandType
    {
        get => _command.CommandType;
        set => _command.CommandType = value;
    }

    /// <summary>
    ///     Gets the underlying <see cref="MySqlCommand" /> for MySQL-specific parameter access.
    /// </summary>
    internal MySqlCommand UnderlyingCommand => _command;

    /// <inheritdoc />
    public void AddParameter(string name, object? value)
    {
        _ = _command.Parameters.AddWithValue(name, value ?? DBNull.Value);
    }

    /// <inheritdoc />
    public async Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
    {
        var reader = await _command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return new MySqlDatabaseReader(reader);
    }

    /// <inheritdoc />
    public async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        return await _command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _command.DisposeAsync().ConfigureAwait(false);
    }
}
