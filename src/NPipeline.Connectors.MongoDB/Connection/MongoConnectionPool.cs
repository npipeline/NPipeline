using System.Collections.Concurrent;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.DependencyInjection;

namespace NPipeline.Connectors.MongoDB.Connection;

/// <summary>
///     MongoDB connection pool implementation.
///     Provides efficient connection management and support for named connections.
/// </summary>
public class MongoConnectionPool : IMongoConnectionPool
{
    private readonly ConcurrentDictionary<string, IMongoClient> _clients;
    private readonly MongoConfiguration _configuration;
    private readonly string? _defaultConnectionString;
    private readonly ConcurrentDictionary<string, IMongoClient> _namedClients;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoConnectionPool" /> class with a single connection string.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    public MongoConnectionPool(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        _clients = new ConcurrentDictionary<string, IMongoClient>(StringComparer.OrdinalIgnoreCase);
        _namedClients = new ConcurrentDictionary<string, IMongoClient>(StringComparer.OrdinalIgnoreCase);
        _configuration = new MongoConfiguration();
        _defaultConnectionString = connectionString;

        var defaultClient = MongoConnectionFactory.CreateClient(connectionString, _configuration);
        _clients[connectionString] = defaultClient;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoConnectionPool" /> class with named connections only.
    /// </summary>
    /// <param name="namedConnections">Dictionary of named connection strings.</param>
    public MongoConnectionPool(IDictionary<string, string> namedConnections)
    {
        ArgumentNullException.ThrowIfNull(namedConnections);

        _clients = new ConcurrentDictionary<string, IMongoClient>(StringComparer.OrdinalIgnoreCase);
        _namedClients = new ConcurrentDictionary<string, IMongoClient>(StringComparer.OrdinalIgnoreCase);
        _configuration = new MongoConfiguration();

        // Initialize named connections
        foreach (var kvp in namedConnections)
        {
            if (string.IsNullOrWhiteSpace(kvp.Value))
                throw new ArgumentException($"Connection string for '{kvp.Key}' cannot be empty.", nameof(namedConnections));

            var client = MongoConnectionFactory.CreateClient(kvp.Value, _configuration);
            _clients[kvp.Value] = client;
            _namedClients[kvp.Key] = client;
        }

        // Ensure at least one connection is available
        if (_clients.IsEmpty)
        {
            throw new ArgumentException(
                "At least one MongoDB connection string must be configured.",
                nameof(namedConnections));
        }
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoConnectionPool" /> class using configured options.
    /// </summary>
    /// <param name="options">The connector options.</param>
    public MongoConnectionPool(MongoConnectorOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _clients = new ConcurrentDictionary<string, IMongoClient>(StringComparer.OrdinalIgnoreCase);
        _namedClients = new ConcurrentDictionary<string, IMongoClient>(StringComparer.OrdinalIgnoreCase);
        _configuration = options.DefaultConfiguration ?? new MongoConfiguration();

        // Initialize default connection if provided
        if (!string.IsNullOrWhiteSpace(options.DefaultConnectionString))
        {
            _defaultConnectionString = options.DefaultConnectionString;
            var defaultClient = MongoConnectionFactory.CreateClient(options.DefaultConnectionString, _configuration);
            _clients[options.DefaultConnectionString] = defaultClient;
        }

        // Initialize named connections
        foreach (var kvp in options.Connections)
        {
            if (string.IsNullOrWhiteSpace(kvp.Value))
                throw new ArgumentException($"Connection string for '{kvp.Key}' cannot be empty.", nameof(options));

            var client = MongoConnectionFactory.CreateClient(kvp.Value, _configuration);
            _clients[kvp.Value] = client;
            _namedClients[kvp.Key] = client;
        }

        // No pre-configured connections is valid — callers may supply connection strings
        // on demand via GetClientForUri(), so do not enforce a minimum here.
    }

    /// <summary>
    ///     Gets a client for the specified named connection.
    /// </summary>
    /// <param name="connectionName">The name of the connection. If null, returns the default client.</param>
    /// <returns>An <see cref="IMongoClient" /> instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the named connection is not found.</exception>
    public IMongoClient GetClient(string? connectionName = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // If a named connection is requested
        if (!string.IsNullOrWhiteSpace(connectionName))
        {
            if (_namedClients.TryGetValue(connectionName, out var namedClient))
                return namedClient;

            throw new InvalidOperationException($"Named connection '{connectionName}' not found.");
        }

        // Return the default client
        if (!string.IsNullOrWhiteSpace(_defaultConnectionString) && _clients.TryGetValue(_defaultConnectionString, out var defaultClient))
            return defaultClient;

        // Return any available client (first from named connections)
        var firstClient = _namedClients.Values.FirstOrDefault();

        if (firstClient != null)
            return firstClient;

        // Return first client from cache
        if (_clients.IsEmpty)
        {
            throw new InvalidOperationException(
                "No MongoDB clients are available. Configure a DefaultConnectionString or a named connection, " +
                "or call GetClientForUri() with an explicit connection string.");
        }

        return _clients.Values.First();
    }

    /// <summary>
    ///     Gets a client for the specified connection string.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <returns>An <see cref="IMongoClient" /> instance.</returns>
    public IMongoClient GetClientForUri(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        ObjectDisposedException.ThrowIf(_disposed, this);

        return _clients.GetOrAdd(connectionString, cs => MongoConnectionFactory.CreateClient(cs, _configuration));
    }

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    /// <param name="name">The name of the connection.</param>
    /// <returns>True if the named connection exists; otherwise, false.</returns>
    public bool HasNamedConnection(string name)
    {
        return _namedClients.ContainsKey(name);
    }

    /// <summary>
    ///     Gets all named connection names.
    /// </summary>
    /// <returns>A collection of named connection names.</returns>
    public IEnumerable<string> GetNamedConnectionNames()
    {
        return _namedClients.Keys;
    }

    /// <summary>
    ///     Disposes the connection pool and all cached clients.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Dispose all unique clients (avoid disposing same client twice if referenced in both dictionaries)
        foreach (var client in _clients.Values.Distinct())
        {
            client?.Dispose();
        }

        _clients.Clear();
        _namedClients.Clear();

        GC.SuppressFinalize(this);
    }
}
