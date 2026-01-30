using System.Data;
using Npgsql;
using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors.PostgreSQL.Connection;

/// <summary>
///     PostgreSQL implementation of IDatabaseConnection.
///     Wraps NpgsqlConnection for database-agnostic operations.
/// </summary>
internal sealed class PostgresDatabaseConnection(NpgsqlConnection connection) : IDatabaseConnection
{
    private readonly NpgsqlConnection _connection = connection ?? throw new ArgumentNullException(nameof(connection));

    /// <summary>
    ///     Gets a value indicating whether the connection is currently open.
    /// </summary>
    public bool IsOpen => _connection.State == ConnectionState.Open;

    /// <summary>
    ///     Opens the database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task OpenAsync(CancellationToken cancellationToken = default)
    {
        if (_connection.State == ConnectionState.Closed)
            await _connection.OpenAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Closes the database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        _connection.Close();
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Creates a database command for this connection.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var command = _connection.CreateCommand();
        return Task.FromResult<IDatabaseCommand>(new PostgresDatabaseCommand(command));
    }

    /// <summary>
    ///     Disposes the connection asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}
