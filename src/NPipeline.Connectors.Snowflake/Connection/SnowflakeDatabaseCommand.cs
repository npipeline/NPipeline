using System.Data;
using System.Data.Common;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Connection;

/// <summary>
///     Snowflake implementation of IDatabaseCommand.
///     Wraps DbCommand for database-agnostic operations.
/// </summary>
internal sealed class SnowflakeDatabaseCommand(DbCommand command) : IDatabaseCommand
{
    private readonly DbCommand _command = command ?? throw new ArgumentNullException(nameof(command));

    /// <summary>
    ///     Gets or sets the command text (SQL query).
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
    ///     Gets or sets the command type.
    /// </summary>
    public CommandType CommandType
    {
        get => _command.CommandType;
        set => _command.CommandType = value;
    }

    /// <summary>
    ///     Adds a parameter to the command.
    /// </summary>
    public void AddParameter(string name, object? value)
    {
        var parameter = _command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        _ = _command.Parameters.Add(parameter);
    }

    /// <summary>
    ///     Executes the command and returns a data reader.
    /// </summary>
    public async Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default)
    {
        var reader = await _command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return new SnowflakeDatabaseReader(reader);
    }

    /// <summary>
    ///     Executes the command and returns the number of rows affected.
    /// </summary>
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
