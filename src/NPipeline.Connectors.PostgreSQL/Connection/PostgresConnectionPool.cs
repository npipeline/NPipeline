using System.Collections.Concurrent;
using System.Linq;
using Npgsql;
using NPipeline.Connectors.PostgreSQL.Configuration;

namespace NPipeline.Connectors.PostgreSQL.Connection
{
    /// <summary>
    /// PostgreSQL connection pool implementation using NpgsqlDataSource.
    /// Provides efficient connection pooling and support for named connections.
    /// </summary>
    public class PostgresConnectionPool : IPostgresConnectionPool
    {
        private readonly NpgsqlDataSource? _defaultDataSource;
        private readonly ConcurrentDictionary<string, NpgsqlDataSource> _namedDataSources;

        /// <summary>
        /// Initializes a new instance of the PostgresConnectionPool with a single connection string.
        /// </summary>
        /// <param name="connectionString">The connection string for PostgreSQL.</param>
        public PostgresConnectionPool(string connectionString)
            : this(new PostgresOptions { DefaultConnectionString = connectionString })
        {
        }

        /// <summary>
        /// Initializes a new instance of the PostgresConnectionPool with named connections only.
        /// </summary>
        /// <param name="namedConnections">Dictionary of named connection strings.</param>
        public PostgresConnectionPool(IDictionary<string, string> namedConnections)
            : this(new PostgresOptions { NamedConnections = new Dictionary<string, string>(namedConnections, StringComparer.OrdinalIgnoreCase) })
        {
        }

        /// <summary>
        /// Initializes a new instance of the PostgresConnectionPool using configured options.
        /// </summary>
        /// <param name="options">The connector options.</param>
        public PostgresConnectionPool(PostgresOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);

            _namedDataSources = new ConcurrentDictionary<string, NpgsqlDataSource>(StringComparer.OrdinalIgnoreCase);

            var hasDefault = !string.IsNullOrWhiteSpace(options.DefaultConnectionString);
            if (hasDefault)
            {
                _defaultDataSource = new NpgsqlDataSourceBuilder(options.DefaultConnectionString).Build();
                ConnectionString = options.DefaultConnectionString;
            }

            foreach (var kvp in options.NamedConnections)
            {
                if (string.IsNullOrWhiteSpace(kvp.Value))
                {
                    throw new ArgumentException($"Connection string for '{kvp.Key}' cannot be empty.", nameof(options));
                }

                var dataSource = new NpgsqlDataSourceBuilder(kvp.Value).Build();
                _ = _namedDataSources.TryAdd(kvp.Key, dataSource);
                ConnectionString ??= kvp.Value;
                _defaultDataSource ??= dataSource;
            }

            if (_defaultDataSource == null)
            {
                throw new ArgumentException("At least one PostgreSQL connection string must be configured via DefaultConnectionString or NamedConnections.", nameof(options));
            }
        }

        /// <summary>
        /// Gets a connection from the pool asynchronously.
        /// </summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>An open NpgsqlConnection.</returns>
        public async Task<NpgsqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
        {
            var dataSource = _defaultDataSource ?? throw new InvalidOperationException("No default PostgreSQL connection string configured.");
            var connection = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
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
            return Task.FromResult(_defaultDataSource ?? throw new InvalidOperationException("No default PostgreSQL connection string configured."));
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
        public string? ConnectionString { get; private set; }

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
            if (_defaultDataSource != null)
            {
                await _defaultDataSource.DisposeAsync().ConfigureAwait(false);
            }

            foreach (var dataSource in _namedDataSources.Values.Distinct())
            {
                if (ReferenceEquals(dataSource, _defaultDataSource))
                {
                    continue;
                }

                await dataSource.DisposeAsync().ConfigureAwait(false);
            }

            GC.SuppressFinalize(this);
        }
    }
}
