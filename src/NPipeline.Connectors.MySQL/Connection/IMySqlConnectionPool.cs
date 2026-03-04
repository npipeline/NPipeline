using MySqlConnector;

namespace NPipeline.Connectors.MySql.Connection;

/// <summary>
///     Abstraction for MySQL connection pool management.
///     Provides connection lifecycle management and MySqlConnection access.
/// </summary>
public interface IMySqlConnectionPool : IAsyncDisposable
{
    /// <summary>
    ///     Gets the connection string used by this pool.
    ///     Returns the default connection string, or <c>null</c> if no default connection is configured.
    /// </summary>
    string? ConnectionString { get; }

    /// <summary>
    ///     Gets a connection from the pool asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An open <see cref="MySqlConnection"/>.</returns>
    Task<MySqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Gets a named connection from the pool asynchronously.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An open <see cref="MySqlConnection"/>.</returns>
    Task<MySqlConnection> GetConnectionAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    bool HasNamedConnection(string name);

    /// <summary>
    ///     Gets all named connection names.
    /// </summary>
    IEnumerable<string> GetNamedConnectionNames();
}
