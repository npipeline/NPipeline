using System.Data;
using Microsoft.Data.SqlClient;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.SqlServer.Connection;

/// <summary>
///     SQL Server implementation of IDatabaseConnection.
///     Wraps SqlConnection for database-agnostic operations.
/// </summary>
internal sealed class SqlServerDatabaseConnection(SqlConnection connection) : IDatabaseConnection
{
    private readonly SqlConnection _connection = connection ?? throw new ArgumentNullException(nameof(connection));

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
        return Task.FromResult<IDatabaseCommand>(new SqlServerDatabaseCommand(command));
    }

    /// <summary>
    ///     Disposes the connection asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}
