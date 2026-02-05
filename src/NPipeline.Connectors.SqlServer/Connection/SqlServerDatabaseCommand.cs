using System.Data;
using Microsoft.Data.SqlClient;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.SqlServer.Connection;

/// <summary>
///     SQL Server implementation of IDatabaseCommand.
///     Wraps SqlCommand for database-agnostic operations.
/// </summary>
internal sealed class SqlServerDatabaseCommand(SqlCommand command) : IDatabaseCommand
{
    private readonly SqlCommand _command = command ?? throw new ArgumentNullException(nameof(command));

    /// <summary>
    ///     Gets or sets the command text (SQL query or stored procedure name).
    /// </summary>
    public string CommandText
    {
        get => _command.CommandText;
        set => _command.CommandText = value;
    }

    /// <summary>
    ///     Gets or sets the command timeout in seconds.
    /// </summary>
    public int CommandTimeout
    {
        get => _command.CommandTimeout;
        set => _command.CommandTimeout = value;
    }

    /// <summary>
    ///     Gets or sets the command type (Text, StoredProcedure, TableDirect).
    /// </summary>
    public CommandType CommandType
    {
        get => _command.CommandType;
        set => _command.CommandType = value;
    }

    /// <summary>
    ///     Adds a parameter to the command.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    public void AddParameter(string name, object? value)
    {
        _ = _command.Parameters.AddWithValue(name, value ?? DBNull.Value);
    }

    /// <summary>
    ///     Executes the command and returns a data reader.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
    {
        var reader = await _command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return new SqlServerDatabaseReader(reader);
    }

    /// <summary>
    ///     Executes the command and returns the number of rows affected.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default)
    {
        return await _command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Disposes the command asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _command.DisposeAsync().ConfigureAwait(false);
    }
}
