using System.Collections.Concurrent;
using Npgsql;

namespace NPipeline.Connectors.PostgreSQL.Connection
{
    /// <summary>
    /// PostgreSQL connection pool implementation using NpgsqlDataSource.
    /// Provides efficient connection pooling and support for named connections.
    /// </summary>
    public class PostgresConnectionPool : IPostgresConnectionPool
    {
        private readonly NpgsqlDataSource _dataSource;
        private readonly ConcurrentDictionary<string, NpgsqlDataSource> _namedDataSources;

        /// <summary>
        /// Initializes a new instance of the PostgresConnectionPool with a single connection string.
        /// </summary>
        /// <param name="connectionString">The connection string for PostgreSQL.</param>
        public PostgresConnectionPool(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));
            }

            ConnectionString = connectionString;
            _dataSource = new NpgsqlDataSourceBuilder(connectionString).Build();
            _namedDataSources = new ConcurrentDictionary<string, NpgsqlDataSource>(StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Initializes a new instance of the PostgresConnectionPool with named connections.
        /// </summary>
        /// <param name="namedConnections">Dictionary of named connection strings.</param>
        public PostgresConnectionPool(IDictionary<string, string> namedConnections)
        {
            if (namedConnections == null || namedConnections.Count == 0)
            {
                throw new ArgumentException("Named connections cannot be null or empty.", nameof(namedConnections));
            }

            _namedDataSources = new ConcurrentDictionary<string, NpgsqlDataSource>(StringComparer.OrdinalIgnoreCase);

            foreach (var kvp in namedConnections)
            {
                if (string.IsNullOrWhiteSpace(kvp.Value))
                {
                    throw new ArgumentException($"Connection string for '{kvp.Key}' cannot be empty.", nameof(namedConnections));
                }

                _ = _namedDataSources.TryAdd(kvp.Key, new NpgsqlDataSourceBuilder(kvp.Value).Build());
            }

            _dataSource = _namedDataSources.Values.First();
            ConnectionString = namedConnections.Values.First();
        }

        /// <summary>
        /// Gets a connection from the pool asynchronously.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An open NpgsqlConnection.</returns>
        public async Task<NpgsqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }

        /// <summary>
        /// Gets the NpgsqlDataSource for this pool.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The NpgsqlDataSource.</returns>
        public Task<NpgsqlDataSource> GetDataSourceAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_dataSource);
        }

        /// <summary>
        /// Gets a connection for a named connection string.
        /// </summary>
        /// <param name="name">The name of the connection.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An open NpgsqlConnection.</returns>
        /// <exception cref="InvalidOperationException">Thrown when named connection is not found.</exception>
        public async Task<NpgsqlConnection> GetConnectionAsync(string name, CancellationToken cancellationToken = default)
        {
            if (!_namedDataSources.TryGetValue(name, out var dataSource))
            {
                throw new InvalidOperationException($"Named connection '{name}' not found.");
            }

            var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }

        /// <summary>
        /// Gets the NpgsqlDataSource for a named connection.
        /// </summary>
        /// <param name="name">The name of the connection.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The NpgsqlDataSource.</returns>
        /// <exception cref="InvalidOperationException">Thrown when named connection is not found.</exception>
        public Task<NpgsqlDataSource> GetDataSourceAsync(string name, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _namedDataSources.TryGetValue(name, out var dataSource)
                ? Task.FromResult(dataSource)
                : throw new InvalidOperationException($"Named connection '{name}' not found.");
        }

        /// <summary>
        /// Gets the connection string used by this pool.
        /// </summary>
        public string ConnectionString { get; }

        /// <summary>
        /// Checks if a named connection exists.
        /// </summary>
        /// <param name="name">The name of the connection.</param>
        /// <returns>True if the named connection exists; otherwise, false.</returns>
        public bool HasNamedConnection(string name)
        {
            return _namedDataSources.ContainsKey(name);
        }

        /// <summary>
        /// Gets all named connection names.
        /// </summary>
        /// <returns>A collection of named connection names.</returns>
        public IEnumerable<string> GetNamedConnectionNames()
        {
            return _namedDataSources.Keys;
        }

        /// <summary>
        /// Disposes the connection pool and all associated data sources.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            await _dataSource.DisposeAsync().ConfigureAwait(false);

            foreach (var dataSource in _namedDataSources.Values)
            {
                await dataSource.DisposeAsync().ConfigureAwait(false);
            }

            GC.SuppressFinalize(this);
        }
    }
}
