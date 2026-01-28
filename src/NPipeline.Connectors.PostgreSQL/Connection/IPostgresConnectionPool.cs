using Npgsql;

namespace NPipeline.Connectors.PostgreSQL.Connection
{
    /// <summary>
    /// Abstraction for PostgreSQL connection pool management.
    /// Provides connection lifecycle management and NpgsqlDataSource access.
    /// </summary>
    public interface IPostgresConnectionPool : IAsyncDisposable
    {
        /// <summary>
        /// Gets a connection from the pool asynchronously.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An open NpgsqlConnection.</returns>
        Task<NpgsqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets a connection for a named connection string.
        /// </summary>
        /// <param name="name">The connection name.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An open NpgsqlConnection.</returns>
        Task<NpgsqlConnection> GetConnectionAsync(string name, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the NpgsqlDataSource for this pool.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The NpgsqlDataSource.</returns>
        Task<NpgsqlDataSource> GetDataSourceAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the NpgsqlDataSource for a named connection.
        /// </summary>
        /// <param name="name">The connection name.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The NpgsqlDataSource.</returns>
        Task<NpgsqlDataSource> GetDataSourceAsync(string name, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the connection string used by this pool.
        /// Returns the default connection string, or null if no default connection is configured.
        /// </summary>
        string? ConnectionString { get; }

        /// <summary>
        /// Checks if a named connection exists.
        /// </summary>
        /// <param name="name">The name of the connection.</param>
        /// <returns>True if the named connection exists; otherwise, false.</returns>
        bool HasNamedConnection(string name);

        /// <summary>
        /// Gets all named connection names.
        /// </summary>
        /// <returns>A collection of named connection names.</returns>
        IEnumerable<string> GetNamedConnectionNames();
    }
}
