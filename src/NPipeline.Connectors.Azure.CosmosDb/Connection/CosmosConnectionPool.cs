using System.Collections.Concurrent;
using Azure.Core;
using Azure.Identity;
using Cassandra;
using Microsoft.Azure.Cosmos;
using MongoDB.Driver;
using NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Connection;

/// <summary>
///     Cosmos DB connection pool implementation using CosmosClient.
///     Provides efficient connection management and support for named connections.
/// </summary>
public class CosmosConnectionPool : ICosmosConnectionPool
{
    private const string DefaultConnectionName = "__default__";
    private readonly ConcurrentDictionary<string, CassandraClientContext> _cassandraClients;
    private readonly CosmosConfiguration _configuration;
    private readonly CassandraConnectionOptions? _defaultCassandraConnection;

    private readonly CosmosClient? _defaultClient;
    private readonly MongoClient? _defaultMongoClient;
    private readonly ConcurrentDictionary<string, CassandraConnectionOptions> _namedCassandraConnections;
    private readonly ConcurrentDictionary<string, CosmosClient> _namedClients;
    private readonly ConcurrentDictionary<string, MongoClient> _namedMongoClients;
    private readonly CosmosOptions _options;

    /// <summary>
    ///     Initializes a new instance of the CosmosConnectionPool with a single connection string.
    /// </summary>
    /// <param name="connectionString">The Cosmos DB connection string.</param>
    /// <param name="configuration">Optional configuration settings.</param>
    public CosmosConnectionPool(string connectionString, CosmosConfiguration? configuration = null)
        : this(new CosmosOptions { DefaultConnectionString = connectionString, DefaultConfiguration = configuration })
    {
    }

    /// <summary>
    ///     Initializes a new instance of the CosmosConnectionPool with an endpoint and token credential.
    /// </summary>
    /// <param name="endpoint">The Cosmos DB endpoint URI.</param>
    /// <param name="credential">The Azure token credential for authentication.</param>
    /// <param name="configuration">Optional configuration settings.</param>
    public CosmosConnectionPool(Uri endpoint, TokenCredential credential, CosmosConfiguration? configuration = null)
        : this(new CosmosOptions { DefaultEndpoint = endpoint, DefaultCredential = credential, DefaultConfiguration = configuration })
    {
    }

    /// <summary>
    ///     Initializes a new instance of the CosmosConnectionPool with named connections only.
    /// </summary>
    /// <param name="namedConnections">Dictionary of named connection strings.</param>
    /// <param name="configuration">Optional configuration settings.</param>
    public CosmosConnectionPool(IDictionary<string, string> namedConnections, CosmosConfiguration? configuration = null)
        : this(CreateOptionsFromNamedConnections(namedConnections, configuration))
    {
    }

    /// <summary>
    ///     Initializes a new instance of the CosmosConnectionPool using configured options.
    /// </summary>
    /// <param name="options">The connector options.</param>
    public CosmosConnectionPool(CosmosOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _options = options;
        _namedClients = new ConcurrentDictionary<string, CosmosClient>(StringComparer.OrdinalIgnoreCase);
        _namedMongoClients = new ConcurrentDictionary<string, MongoClient>(StringComparer.OrdinalIgnoreCase);
        _namedCassandraConnections = new ConcurrentDictionary<string, CassandraConnectionOptions>(StringComparer.OrdinalIgnoreCase);
        _cassandraClients = new ConcurrentDictionary<string, CassandraClientContext>(StringComparer.OrdinalIgnoreCase);
        _configuration = options.DefaultConfiguration ?? new CosmosConfiguration();

        var hasDefault = !string.IsNullOrWhiteSpace(options.DefaultConnectionString);

        if (hasDefault)
        {
            _defaultClient = BuildClientFromConnectionString(options.DefaultConnectionString!);
            ConnectionString = options.DefaultConnectionString;
        }

        foreach (var kvp in options.NamedConnections)
        {
            if (string.IsNullOrWhiteSpace(kvp.Value))
                throw new ArgumentException($"Connection string for '{kvp.Key}' cannot be empty.", nameof(options));

            var client = BuildClientFromConnectionString(kvp.Value);
            _ = _namedClients.TryAdd(kvp.Key, client);
            ConnectionString ??= kvp.Value;
            _defaultClient ??= client;
        }

        // Handle Azure AD connections
        if (options.DefaultEndpoint != null)
        {
            _defaultClient = BuildClientFromEndpoint(options.DefaultEndpoint, options.DefaultCredential);
            ConnectionString ??= options.DefaultEndpoint.ToString();
        }

        foreach (var kvp in options.NamedEndpoints)
        {
            if (kvp.Value.Endpoint == null)
                throw new ArgumentException($"Endpoint for '{kvp.Key}' cannot be null.", nameof(options));

            var client = BuildClientFromEndpoint(kvp.Value.Endpoint, kvp.Value.Credential);
            _ = _namedClients.TryAdd(kvp.Key, client);
            ConnectionString ??= kvp.Value.Endpoint.ToString();
            _defaultClient ??= client;
        }

        if (!string.IsNullOrWhiteSpace(options.DefaultMongoConnectionString))
            _defaultMongoClient = new MongoClient(options.DefaultMongoConnectionString);

        foreach (var kvp in options.NamedMongoConnections)
        {
            if (!string.IsNullOrWhiteSpace(kvp.Value))
                _namedMongoClients.TryAdd(kvp.Key, new MongoClient(kvp.Value));
        }

        if (_defaultMongoClient == null && !_namedMongoClients.IsEmpty)
            _defaultMongoClient = _namedMongoClients.Values.First();

        _defaultCassandraConnection = options.DefaultCassandraConnection;

        foreach (var kvp in options.NamedCassandraConnections)
        {
            _namedCassandraConnections.TryAdd(kvp.Key, kvp.Value);
        }

        if (_defaultCassandraConnection == null && !_namedCassandraConnections.IsEmpty)
            _defaultCassandraConnection = _namedCassandraConnections.Values.First();

        if (_defaultClient == null && _defaultMongoClient == null && _defaultCassandraConnection == null &&
            _namedClients.IsEmpty && _namedMongoClients.IsEmpty && _namedCassandraConnections.IsEmpty)
        {
            throw new ArgumentException(
                "At least one Cosmos SQL, Mongo, or Cassandra connection must be configured.",
                nameof(options));
        }
    }

    /// <summary>
    ///     Gets the connection string used by this pool.
    /// </summary>
    public string? ConnectionString { get; }

    /// <summary>
    ///     Gets a CosmosClient from the pool asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A CosmosClient instance.</returns>
    public Task<CosmosClient> GetClientAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_defaultClient ?? throw new InvalidOperationException("No default Cosmos DB connection configured."));
    }

    /// <summary>
    ///     Gets a CosmosClient for a named connection.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A CosmosClient instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when named connection is not found.</exception>
    public Task<CosmosClient> GetClientAsync(string name, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return _namedClients.TryGetValue(name, out var client)
            ? Task.FromResult(client)
            : throw new InvalidOperationException($"Named connection '{name}' not found.");
    }

    /// <inheritdoc />
    public async Task<TClient> GetClientAsync<TClient>(CosmosApiType apiType, CancellationToken cancellationToken = default)
        where TClient : class
    {
        return await GetClientAsync<TClient>(DefaultConnectionName, apiType, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<TClient> GetClientAsync<TClient>(string name, CosmosApiType apiType, CancellationToken cancellationToken = default)
        where TClient : class
    {
        cancellationToken.ThrowIfCancellationRequested();

        switch (apiType)
        {
            case CosmosApiType.Sql:
            {
                var sqlClient = string.Equals(name, DefaultConnectionName, StringComparison.OrdinalIgnoreCase)
                    ? await GetClientAsync(cancellationToken)
                    : await GetClientAsync(name, cancellationToken);

                return sqlClient as TClient
                       ?? throw new InvalidOperationException($"Requested client type '{typeof(TClient).Name}' is not compatible with SQL API.");
            }

            case CosmosApiType.Mongo:
            {
                var mongoClient = string.Equals(name, DefaultConnectionName, StringComparison.OrdinalIgnoreCase)
                    ? _defaultMongoClient ?? BuildDefaultMongoClient()
                    : GetNamedMongoClient(name);

                return mongoClient as TClient
                       ?? throw new InvalidOperationException($"Requested client type '{typeof(TClient).Name}' is not compatible with Mongo API.");
            }

            case CosmosApiType.Cassandra:
            {
                var context = await GetCassandraClientContextAsync(name, cancellationToken);

                if (typeof(TClient) == typeof(CassandraClientContext))
                    return (context as TClient)!;

                if (typeof(TClient) == typeof(ISession))
                    return (context.Session as TClient)!;

                throw new InvalidOperationException(
                    $"Requested client type '{typeof(TClient).Name}' is not supported for Cassandra API. Use '{nameof(CassandraClientContext)}' or '{nameof(ISession)}'.");
            }

            default:
                throw new NotSupportedException($"Cosmos API '{apiType}' is not supported.");
        }
    }

    /// <summary>
    ///     Gets a Container reference for the specified database and container.
    /// </summary>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Container reference.</returns>
    public async Task<Container> GetContainerAsync(
        string databaseId,
        string containerId,
        CancellationToken cancellationToken = default)
    {
        var client = await GetClientAsync(cancellationToken);
        return client.GetContainer(databaseId, containerId);
    }

    /// <summary>
    ///     Gets a Container reference using a named connection.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A Container reference.</returns>
    public async Task<Container> GetContainerAsync(
        string name,
        string databaseId,
        string containerId,
        CancellationToken cancellationToken = default)
    {
        var client = await GetClientAsync(name, cancellationToken);
        return client.GetContainer(databaseId, containerId);
    }

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    /// <param name="name">The name of the connection.</param>
    /// <returns>True if the named connection exists; otherwise, false.</returns>
    public bool HasNamedConnection(string name)
    {
        return _namedClients.ContainsKey(name) || _namedMongoClients.ContainsKey(name) || _namedCassandraConnections.ContainsKey(name);
    }

    /// <summary>
    ///     Gets all named connection names.
    /// </summary>
    /// <returns>A collection of named connection names.</returns>
    public IEnumerable<string> GetNamedConnectionNames()
    {
        return _namedClients.Keys
            .Concat(_namedMongoClients.Keys)
            .Concat(_namedCassandraConnections.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Disposes the connection pool and all associated clients.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_defaultClient != null)
            _defaultClient.Dispose();

        foreach (var client in _namedClients.Values.Distinct())
        {
            if (ReferenceEquals(client, _defaultClient))
                continue;

            client.Dispose();
        }

        // Dispose Mongo clients - MongoClient implements IDisposable and holds server connections
        _defaultMongoClient?.Dispose();

        foreach (var mongoClient in _namedMongoClients.Values)
        {
            mongoClient.Dispose();
        }

        foreach (var cassandraClient in _cassandraClients.Values)
        {
            await cassandraClient.DisposeAsync();
        }

        GC.SuppressFinalize(this);
        await ValueTask.CompletedTask;
    }

    private static CosmosOptions CreateOptionsFromNamedConnections(IDictionary<string, string> namedConnections, CosmosConfiguration? configuration)
    {
        var options = new CosmosOptions { DefaultConfiguration = configuration };

        foreach (var kvp in namedConnections)
        {
            options.AddOrUpdateConnection(kvp.Key, kvp.Value);
        }

        return options;
    }

    private CosmosClient BuildClientFromConnectionString(string connectionString)
    {
        var clientOptions = BuildClientOptions();
        return new CosmosClient(connectionString, clientOptions);
    }

    private CosmosClient BuildClientFromEndpoint(Uri endpoint, TokenCredential? credential)
    {
        var clientOptions = BuildClientOptions();

        if (credential != null)
            return new CosmosClient(endpoint.ToString(), credential, clientOptions);

        // Use DefaultAzureCredential if no specific credential provided
        var defaultCredential = new DefaultAzureCredential();
        return new CosmosClient(endpoint.ToString(), defaultCredential, clientOptions);
    }

    private CosmosClientOptions BuildClientOptions()
    {
        var options = new CosmosClientOptions
        {
            RequestTimeout = TimeSpan.FromSeconds(_configuration.RequestTimeout),
            MaxRetryAttemptsOnRateLimitedRequests = _configuration.MaxRetryAttempts,
            MaxRetryWaitTimeOnRateLimitedRequests = _configuration.MaxRetryWaitTime,
            EnableContentResponseOnWrite = _configuration.EnableContentResponseOnWrite,
            AllowBulkExecution = _configuration.AllowBulkExecution,
        };

        if (_configuration.ConsistencyLevel.HasValue)
            options.ConsistencyLevel = _configuration.ConsistencyLevel.Value;

        if (_configuration.PreferredRegions.Count > 0)
            options.ApplicationPreferredRegions = _configuration.PreferredRegions.ToList();

        if (_configuration.UseGatewayMode)
            options.ConnectionMode = ConnectionMode.Gateway;

        if (_configuration.HttpClientFactory is { } factory)
            options.HttpClientFactory = factory;

        return options;
    }

    private MongoClient BuildDefaultMongoClient()
    {
        var connectionString = _options.DefaultMongoConnectionString
                               ?? _configuration.MongoConnectionString
                               ?? _configuration.ConnectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new InvalidOperationException("No default Mongo connection configured.");

        return new MongoClient(connectionString);
    }

    private MongoClient GetNamedMongoClient(string name)
    {
        if (_namedMongoClients.TryGetValue(name, out var client))
            return client;

        throw new InvalidOperationException($"Named Mongo connection '{name}' not found.");
    }

    private async Task<CassandraClientContext> GetCassandraClientContextAsync(string name, CancellationToken cancellationToken)
    {
        var key = string.Equals(name, DefaultConnectionName, StringComparison.OrdinalIgnoreCase)
            ? DefaultConnectionName
            : name;

        if (_cassandraClients.TryGetValue(key, out var existing))
            return existing;

        var options = ResolveCassandraOptions(name);
        var builder = Cluster.Builder().AddContactPoint(options.ContactPoint).WithPort(options.Port);

        if (!string.IsNullOrWhiteSpace(options.Username))
            builder = builder.WithCredentials(options.Username, options.Password ?? string.Empty);

        var cluster = builder.Build();

        // Create a linked token source with connection timeout to prevent indefinite hangs
        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_configuration.ConnectionTimeout));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        var session = await cluster.ConnectAsync(options.Keyspace).WaitAsync(linkedCts.Token);
        var context = new CassandraClientContext(cluster, session);

        // Handle race condition: if another thread added a context while we were creating ours,
        // dispose our context and use theirs to avoid resource leaks
        if (_cassandraClients.TryAdd(key, context))
            return context;

        // Another thread won the race - dispose our context and return theirs
        await context.DisposeAsync();
        return _cassandraClients[key];
    }

    private CassandraConnectionOptions ResolveCassandraOptions(string name)
    {
        if (!string.Equals(name, DefaultConnectionName, StringComparison.OrdinalIgnoreCase) &&
            _namedCassandraConnections.TryGetValue(name, out var named))
            return named;

        if (_defaultCassandraConnection != null)
            return _defaultCassandraConnection;

        var contactPoint = _configuration.CassandraContactPoint;

        if (string.IsNullOrWhiteSpace(contactPoint) && Uri.TryCreate(_configuration.AccountEndpoint, UriKind.Absolute, out var endpointUri))
            contactPoint = endpointUri.Host;

        if (string.IsNullOrWhiteSpace(contactPoint))
            throw new InvalidOperationException("No default Cassandra contact point configured.");

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            throw new InvalidOperationException("DatabaseId is required to create a default Cassandra connection.");

        return new CassandraConnectionOptions
        {
            ContactPoint = contactPoint,
            Port = _configuration.CassandraPort,
            Keyspace = _configuration.DatabaseId,
            Username = _configuration.CassandraUsername,
            Password = _configuration.CassandraPassword,
        };
    }
}
