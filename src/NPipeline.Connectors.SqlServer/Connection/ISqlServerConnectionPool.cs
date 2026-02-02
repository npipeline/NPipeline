using Microsoft.Data.SqlClient;

namespace NPipeline.Connectors.SqlServer.Connection;

/// <summary>
///     Abstraction for SQL Server connection pool management.
///     Provides connection lifecycle management and SqlConnection access.
/// </summary>
public interface ISqlServerConnectionPool : IAsyncDisposable
{
    /// <summary>
    ///     Gets the connection string used by this pool.
    ///     Returns the default connection string, or null if no default connection is configured.
    /// </summary>
    string? ConnectionString { get; }

    /// <summary>
    ///     Gets a connection from the pool asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An open SqlConnection.</returns>
    Task<SqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets a connection for a named connection string.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An open SqlConnection.</returns>
    Task<SqlConnection> GetConnectionAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    /// <param name="name">The name of the connection.</param>
    /// <returns>True if the named connection exists; otherwise, false.</returns>
    bool HasNamedConnection(string name);

    /// <summary>
    ///     Gets all named connection names.
    /// </summary>
    /// <returns>A collection of named connection names.</returns>
    IEnumerable<string> GetNamedConnectionNames();
}
