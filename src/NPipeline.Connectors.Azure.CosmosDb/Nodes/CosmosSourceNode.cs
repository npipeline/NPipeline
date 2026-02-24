using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Connection;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Nodes;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Azure.CosmosDb.Nodes;

/// <summary>
///     Cosmos DB source node for reading data using SQL queries.
/// </summary>
/// <typeparam name="T">The type of objects emitted by source.</typeparam>
public class CosmosSourceNode<T> : DatabaseSourceNode<IDatabaseReader, T>
{
    private static readonly ConcurrentDictionary<Type, Func<CosmosRow, T>> MapperCache = new();
    private static readonly Lazy<IReadOnlyList<PropertyBinding>> CachedBindings = new(BuildBindings);
    private static readonly Lazy<Func<T>> CachedCreateInstance = new(BuildCreateInstanceDelegate, LazyThreadSafetyMode.ExecutionAndPublication);

    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        CosmosStorageResolverFactory.CreateResolver,
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly Func<CosmosRow, T>? _cachedMapper;
    private readonly CosmosConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly ICosmosConnectionPool? _connectionPool;
    private readonly string _containerId;
    private readonly bool _continueOnError;
    private readonly string _databaseId;
    private readonly Func<CosmosRow, T>? _mapper;
    private readonly DatabaseParameter[] _parameters;
    private readonly string _query;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;
    private IDatabaseReader? _cachedReader;
    private CosmosRow? _cachedRow;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosSourceNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The Cosmos DB connection string.</param>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    public CosmosSourceNode(
        string connectionString,
        string databaseId,
        string containerId,
        string query,
        Func<CosmosRow, T>? mapper = null,
        CosmosConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(databaseId))
            throw new ArgumentNullException(nameof(databaseId));

        if (string.IsNullOrWhiteSpace(containerId))
            throw new ArgumentNullException(nameof(containerId));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new CosmosConfiguration();

        if (string.IsNullOrWhiteSpace(_configuration.ConnectionString))
            _configuration.ConnectionString = connectionString;

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            _configuration.DatabaseId = databaseId;

        _configuration.Validate();
        _connectionPool = new CosmosConnectionPool(connectionString, _configuration);
        _databaseId = databaseId;
        _containerId = containerId;
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(mapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosSourceNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="databaseId">The database identifier.</param>
    /// <param name="containerId">The container identifier.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public CosmosSourceNode(
        ICosmosConnectionPool connectionPool,
        string databaseId,
        string containerId,
        string query,
        Func<CosmosRow, T>? mapper = null,
        CosmosConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(databaseId))
            throw new ArgumentNullException(nameof(databaseId));

        if (string.IsNullOrWhiteSpace(containerId))
            throw new ArgumentNullException(nameof(containerId));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new CosmosConfiguration();
        _configuration.NamedConnection ??= connectionName;

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            _configuration.DatabaseId = databaseId;

        _configuration.Validate();
        _connectionPool = connectionPool;
        _databaseId = databaseId;
        _containerId = containerId;
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;

        _cachedMapper = ResolveDefaultMapper(mapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosSourceNode{T}" /> class using a <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing Cosmos DB connection information.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="resolver">The storage resolver used to obtain storage provider.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    public CosmosSourceNode(
        StorageUri uri,
        string query,
        IStorageResolver? resolver = null,
        Func<CosmosRow, T>? mapper = null,
        CosmosConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        // Extract database and container from URI path
        // Path format: /databaseId/containerId
        var (databaseId, containerId) = ParseUriPath(uri.Path);

        _storageUri = uri;
        _storageResolver = resolver;
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _configuration = configuration ?? new CosmosConfiguration();

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            _configuration.DatabaseId = databaseId;

        _configuration.Validate();
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;

        _databaseId = databaseId;
        _containerId = containerId;

        _cachedMapper = ResolveDefaultMapper(mapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosSourceNode{T}" /> class using a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="uri">The storage URI containing Cosmos DB connection information.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    public CosmosSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        string query,
        Func<CosmosRow, T>? mapper = null,
        CosmosConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(uri);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        // Extract database and container from URI path
        // Path format: /databaseId/containerId
        var (databaseId, containerId) = ParseUriPath(uri.Path);

        _storageProvider = provider;
        _storageUri = uri;
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _configuration = configuration ?? new CosmosConfiguration();

        if (string.IsNullOrWhiteSpace(_configuration.DatabaseId))
            _configuration.DatabaseId = databaseId;

        _configuration.Validate();
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;

        _databaseId = databaseId;
        _containerId = containerId;

        _cachedMapper = ResolveDefaultMapper(mapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Gets whether to stream results.
    /// </summary>
    protected override bool StreamResults => _configuration.StreamResults;

    /// <summary>
    ///     Gets fetch size for streaming.
    /// </summary>
    protected override int FetchSize => _configuration.FetchSize;

    /// <summary>
    ///     Gets delivery semantic.
    /// </summary>
    protected override DeliverySemantic DeliverySemantic => _configuration.DeliverySemantic;

    /// <summary>
    ///     Gets checkpoint strategy.
    /// </summary>
    protected override CheckpointStrategy CheckpointStrategy => _configuration.CheckpointStrategy;

    /// <summary>
    ///     Gets a database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override async Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        // If using StorageUri-based construction, get connection from database storage provider
        if (_storageUri != null)
        {
            var provider = _storageProvider ?? StorageProviderFactory.GetProviderOrThrow(
                _storageResolver ?? DefaultResolver.Value,
                _storageUri);

            if (provider is IDatabaseStorageProvider databaseProvider)
                return await databaseProvider.GetConnectionAsync(_storageUri, cancellationToken);

            throw new InvalidOperationException(
                $"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        // Original connection pool logic
        var client = _connectionName is { Length: > 0 }
            ? await _connectionPool!.GetClientAsync(_connectionName, cancellationToken)
            : await _connectionPool!.GetClientAsync(cancellationToken);

        var database = client.GetDatabase(_databaseId);
        var container = database.GetContainer(_containerId);

        return new CosmosDatabaseConnection(database, container, _configuration);
    }

    /// <summary>
    ///     Executes query and returns a database reader.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override async Task<IDatabaseReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
    {
        var cosmosConnection = (CosmosDatabaseConnection)connection;
        var command = await cosmosConnection.CreateCommandAsync(cancellationToken);

        command.CommandText = _query;
        command.CommandTimeout = _configuration.CommandTimeout;

        foreach (var param in _parameters)
        {
            command.AddParameter(param.Name, param.Value);
        }

        var reader = await command.ExecuteReaderAsync(cancellationToken);
        return reader;
    }

    /// <summary>
    ///     Maps a database row to an object.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <returns>The mapped object.</returns>
    protected override T MapRow(IDatabaseReader reader)
    {
        if (_cachedRow == null || !ReferenceEquals(_cachedReader, reader))
        {
            _cachedReader = reader;
            _cachedRow = new CosmosRow(reader, _configuration.CaseInsensitiveMapping);
        }

        var row = _cachedRow;

        if (_mapper != null)
            return _mapper(row);

        if (_cachedMapper != null)
        {
            try
            {
                return _cachedMapper(row);
            }
            catch
            {
                if (!_continueOnError)
                    throw;
            }
        }

        return MapConventionBased(row);
    }

    /// <summary>
    ///     Maps a row using convention-based mapping.
    /// </summary>
    /// <param name="row">The Cosmos DB row.</param>
    /// <returns>The mapped object.</returns>
    protected virtual T MapConventionBased(CosmosRow row)
    {
        var instance = CachedCreateInstance.Value();

        foreach (var binding in CachedBindings.Value)
        {
            try
            {
                var value = row.GetValue(binding.ColumnName);

                if (value != null)
                {
                    var convertedValue = ConvertValue(value, binding.PropertyType);

                    if (convertedValue != null || binding.PropertyType.IsClass ||
                        Nullable.GetUnderlyingType(binding.PropertyType) != null)
                        binding.Setter(instance, convertedValue);
                }
            }
            catch
            {
                if (!_continueOnError)
                    throw;
            }
        }

        return instance;
    }

    private static object? ConvertValue(object? value, Type targetType)
    {
        if (value is null)
            return null;

        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        return underlyingType.IsInstanceOfType(value)
            ? value
            : Convert.ChangeType(value, underlyingType);
    }

    private static IReadOnlyList<PropertyBinding> BuildBindings()
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !IsIgnored(p))
                .Select(p => new PropertyBinding(p.PropertyType, GetColumnName(p), BuildSetter(p))),
        ];
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        return property.IsDefined(typeof(IgnoreColumnAttribute), true);
    }

    private static Action<T, object?> BuildSetter(PropertyInfo property)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var valueParam = Expression.Parameter(typeof(object), "value");

        var convertedValue = Expression.Convert(valueParam, property.PropertyType);
        var setCall = Expression.Call(instanceParam, property.SetMethod!, convertedValue);

        return Expression.Lambda<Action<T, object?>>(setCall, instanceParam, valueParam).Compile();
    }

    private static Func<T> BuildCreateInstanceDelegate()
    {
        if (typeof(T).IsValueType)
            return Expression.Lambda<Func<T>>(Expression.Default(typeof(T))).Compile();

        var ctor = typeof(T).GetConstructor(Type.EmptyTypes)
                   ?? throw new InvalidOperationException(
                       $"Type '{typeof(T).FullName}' does not have a parameterless constructor");

        return Expression.Lambda<Func<T>>(Expression.New(ctor)).Compile();
    }

    private static string GetColumnName(PropertyInfo property)
    {
        // For Cosmos DB, use the property name as-is (case-preserving)
        return property.Name;
    }

    /// <summary>
    ///     Parses the URI path to extract database and container IDs.
    /// </summary>
    /// <param name="path">The URI path in format /databaseId/containerId.</param>
    /// <returns>A tuple containing database ID and container ID.</returns>
    private static (string databaseId, string containerId) ParseUriPath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("URI path cannot be empty.", nameof(path));

        // Remove leading slash and split
        var segments = path.TrimStart('/').Split('/', StringSplitOptions.RemoveEmptyEntries);

        if (segments.Length < 2)
        {
            throw new ArgumentException(
                "URI path must contain both database and container. Format: /databaseId/containerId", nameof(path));
        }

        return (segments[0], segments[1]);
    }

    private static Func<CosmosRow, T>? ResolveDefaultMapper(
        Func<CosmosRow, T>? mapper,
        CosmosConfiguration configuration,
        bool continueOnError)
    {
        if (mapper != null || continueOnError)
            return mapper;

        return configuration.CacheMappingMetadata
            ? MapperCache.GetOrAdd(typeof(T), _ => CosmosMapperBuilder.Build<T>())
            : CosmosMapperBuilder.Build<T>();
    }

    private sealed record PropertyBinding(Type PropertyType, string ColumnName, Action<T, object?> Setter);
}
