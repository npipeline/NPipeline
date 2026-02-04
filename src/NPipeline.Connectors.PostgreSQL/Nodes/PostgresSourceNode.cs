using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Npgsql;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.Nodes;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.PostgreSQL.Mapping;

namespace NPipeline.Connectors.PostgreSQL.Nodes;

/// <summary>
///     PostgreSQL source node for reading data from PostgreSQL database.
/// </summary>
/// <typeparam name="T">The type of objects emitted by source.</typeparam>
public class PostgresSourceNode<T> : DatabaseSourceNode<IDatabaseReader, T>
{
    private static readonly ConcurrentDictionary<Type, Func<PostgresRow, T>> MapperCache = new();
    private static readonly Lazy<IReadOnlyList<PropertyBinding>> CachedBindings = new(BuildBindings);
    private static readonly Lazy<Func<T>> CachedCreateInstance = new(() => BuildCreateInstanceDelegate());

    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => PostgresStorageResolverFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly Func<PostgresRow, T>? _cachedMapper;

    private readonly PostgresConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly IPostgresConnectionPool? _connectionPool;
    private readonly bool _continueOnError;
    private readonly Func<PostgresRow, T>? _mapper;
    private readonly DatabaseParameter[] _parameters;
    private readonly string _query;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;
    private NpgsqlDataReader? _cachedReader;
    private PostgresRow? _cachedRow;

    /// <summary>
    ///     Initializes a new instance of the <see cref="PostgresSourceNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    public PostgresSourceNode(
        string connectionString,
        string query,
        Func<PostgresRow, T>? mapper = null,
        PostgresConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new PostgresConfiguration();
        _configuration.Validate();
        _connectionPool = new PostgresConnectionPool(connectionString);
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(mapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="PostgresSourceNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public PostgresSourceNode(
        IPostgresConnectionPool connectionPool,
        string query,
        Func<PostgresRow, T>? mapper = null,
        PostgresConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new PostgresConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
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
    ///     Initializes a new instance of the <see cref="PostgresSourceNode{T}" /> class using a <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing PostgreSQL connection information.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="resolver">
    ///     The storage resolver used to obtain storage provider. If <c>null</c>, a default resolver
    ///     created by <see cref="PostgresStorageResolverFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public PostgresSourceNode(
        StorageUri uri,
        string query,
        IStorageResolver? resolver = null,
        Func<PostgresRow, T>? mapper = null,
        PostgresConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(query);

        _storageUri = uri;
        _storageResolver = resolver;
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _configuration = configuration ?? new PostgresConfiguration();
        _configuration.Validate();
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(mapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="PostgresSourceNode{T}" /> class using a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="uri">The storage URI containing PostgreSQL connection information.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    public PostgresSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        string query,
        Func<PostgresRow, T>? mapper = null,
        PostgresConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(query);

        _storageProvider = provider;
        _storageUri = uri;
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _configuration = configuration ?? new PostgresConfiguration();
        _configuration.Validate();
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
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

            throw new InvalidOperationException($"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        // Original connection pool logic
        var connection = _connectionName is { Length: > 0 }
            ? await _connectionPool!.GetConnectionAsync(_connectionName, cancellationToken)
            : await _connectionPool!.GetConnectionAsync(cancellationToken);

        return new PostgresDatabaseConnection(connection);
    }

    /// <summary>
    ///     Executes query and returns a database reader.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override async Task<IDatabaseReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
    {
        var postgresConnection = (PostgresDatabaseConnection)connection;
        var command = await postgresConnection.CreateCommandAsync(cancellationToken);

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
        var postgresReader = (PostgresDatabaseReader)reader;
        var dataReader = postgresReader.Reader;

        if (_cachedRow == null || !ReferenceEquals(_cachedReader, dataReader))
        {
            _cachedReader = dataReader;
            _cachedRow = new PostgresRow(dataReader, _configuration.CaseInsensitiveMapping);
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
    /// <param name="row">The PostgreSQL row.</param>
    /// <returns>The mapped object.</returns>
    protected virtual T MapConventionBased(PostgresRow row)
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

                    if (convertedValue != null || binding.PropertyType.IsClass || Nullable.GetUnderlyingType(binding.PropertyType) != null)
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

    /// <summary>
    ///     Converts a PascalCase string to snake_case.
    /// </summary>
    /// <param name="str">The string to convert.</param>
    /// <returns>The snake_case string.</returns>
    protected static string ToSnakeCase(string str)
    {
        return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x)
            ? "_" + x
            : x.ToString())).ToLowerInvariant();
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
        var columnAttribute = property.GetCustomAttribute<PostgresColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
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
                   ?? throw new InvalidOperationException($"Type '{typeof(T).FullName}' does not have a parameterless constructor");

        return Expression.Lambda<Func<T>>(Expression.New(ctor)).Compile();
    }

    private static string GetColumnName(PropertyInfo property)
    {
        var columnAttr = property.GetCustomAttribute<PostgresColumnAttribute>();

        return columnAttr?.Name is { Length: > 0 } name
            ? name
            : ToSnakeCase(property.Name);
    }

    private static Func<PostgresRow, T>? ResolveDefaultMapper(
        Func<PostgresRow, T>? mapper,
        PostgresConfiguration configuration,
        bool continueOnError)
    {
        if (mapper != null || continueOnError)
            return mapper;

        return configuration.CacheMappingMetadata
            ? MapperCache.GetOrAdd(typeof(T), _ => PostgresMapperBuilder.Build<T>())
            : PostgresMapperBuilder.Build<T>();
    }

    private sealed record PropertyBinding(Type PropertyType, string ColumnName, Action<T, object?> Setter);
}
