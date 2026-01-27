using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.PostgreSQL.Nodes;

namespace NPipeline.Connectors.PostgreSQL.DependencyInjection
{
    /// <summary>
    /// Factory for creating PostgreSQL sink nodes with dependency injection support.
    /// </summary>
    public class PostgresSinkNodeFactory(IPostgresConnectionPool connectionPool)
    {
        private readonly IPostgresConnectionPool _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));

        /// <summary>
        /// Creates a PostgreSQL sink node using a connection from the pool.
        /// </summary>
        /// <typeparam name="T">The type of objects to write.</typeparam>
        /// <param name="connectionName">The name of the connection to use.</param>
        /// <param name="tableName">The name of the target table.</param>
        /// <param name="configuration">Optional configuration. If null, defaults are used.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A configured PostgreSQL sink node.</returns>
        public Task<PostgresSinkNode<T>> CreateSinkAsync<T>(
            string connectionName,
            string tableName,
            PostgresConfiguration? configuration = null,
            CancellationToken cancellationToken = default)
            where T : class
        {
            ArgumentNullException.ThrowIfNull(connectionName);
            ArgumentNullException.ThrowIfNull(tableName);
            cancellationToken.ThrowIfCancellationRequested();

            // If configuration is provided, use the new API with configuration
            if (configuration != null)
            {
                return Task.FromResult(new PostgresSinkNode<T>(_connectionPool, tableName, configuration: configuration, connectionName: connectionName));
            }

            // Otherwise, use the data source constructor with default configuration
            return Task.FromResult(new PostgresSinkNode<T>(_connectionPool, tableName, connectionName: connectionName));
        }
    }
}
