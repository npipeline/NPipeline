using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Nodes;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Snowflake.Nodes;

/// <summary>
///     Snowflake source node for reading data from Snowflake database.
/// </summary>
/// <typeparam name="T">The type of objects emitted by source.</typeparam>
public class SnowflakeSourceNode<T> : DatabaseSourceNode<IDatabaseReader, T>
{
    private static readonly ConcurrentDictionary<Type, Func<SnowflakeRow, T>> MapperCache = new();
    private static readonly Lazy<IReadOnlyList<PropertyBinding>> CachedBindings = new(BuildBindings);
    private static readonly Lazy<Func<T>> CachedCreateInstance = new(() => BuildCreateInstanceDelegate());

    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => SnowflakeStorageResolverFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly Func<SnowflakeRow, T>? _cachedMapper;

    private readonly SnowflakeConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly ISnowflakeConnectionPool? _connectionPool;
    private readonly bool _continueOnError;
    private readonly Func<SnowflakeRow, T>? _mapper;
    private readonly DatabaseParameter[] _parameters;
    private readonly string _query;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;
    private DbDataReader? _cachedReader;
    private SnowflakeRow? _cachedRow;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeSourceNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="configuration">Optional configuration.</param>
    public SnowflakeSourceNode(
        string connectionString,
        string query,
        SnowflakeConfiguration? configuration = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _connectionPool = new SnowflakeConnectionPool(connectionString);
        _mapper = null;
        _query = query;
        _parameters = [];
        _continueOnError = _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(null, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeSourceNode{T}" /> class with custom mapper.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    public SnowflakeSourceNode(
        string connectionString,
        string query,
        Func<SnowflakeRow, T>? customMapper,
        SnowflakeConfiguration? configuration = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _connectionPool = new SnowflakeConnectionPool(connectionString);
        _mapper = customMapper;
        _query = query;
        _parameters = [];
        _continueOnError = _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(customMapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeSourceNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public SnowflakeSourceNode(
        ISnowflakeConnectionPool connectionPool,
        string query,
        SnowflakeConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
        _mapper = null;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;

        _cachedMapper = ResolveDefaultMapper(null, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeSourceNode{T}" /> class with connection pool and custom mapper.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public SnowflakeSourceNode(
        ISnowflakeConnectionPool connectionPool,
        string query,
        Func<SnowflakeRow, T>? customMapper,
        SnowflakeConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
        _mapper = customMapper;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;

        _cachedMapper = ResolveDefaultMapper(customMapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeSourceNode{T}" /> class using a <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing Snowflake connection information.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="resolver">The storage resolver used to obtain storage provider.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    public SnowflakeSourceNode(
        StorageUri uri,
        string query,
        IStorageResolver? resolver = null,
        Func<SnowflakeRow, T>? customMapper = null,
        SnowflakeConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(query);

        _storageUri = uri;
        _storageResolver = resolver;
        _mapper = customMapper;
        _query = query;
        _parameters = parameters ?? [];
        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(customMapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SnowflakeSourceNode{T}" /> class using a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="uri">The storage URI containing Snowflake connection information.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    public SnowflakeSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        string query,
        Func<SnowflakeRow, T>? customMapper = null,
        SnowflakeConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(query);

        _storageProvider = provider;
        _storageUri = uri;
        _mapper = customMapper;
        _query = query;
        _parameters = parameters ?? [];
        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(customMapper, _configuration, _continueOnError);
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
        if (_storageUri != null)
        {
            var provider = _storageProvider ?? StorageProviderFactory.GetProviderOrThrow(
                _storageResolver ?? DefaultResolver.Value,
                _storageUri);

            if (provider is IDatabaseStorageProvider databaseProvider)
                return await databaseProvider.GetConnectionAsync(_storageUri, cancellationToken);

            throw new InvalidOperationException($"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        var connection = _connectionName is { Length: > 0 }
            ? await _connectionPool!.GetConnectionAsync(_connectionName, cancellationToken).ConfigureAwait(false)
            : await _connectionPool!.GetConnectionAsync(cancellationToken).ConfigureAwait(false);

        return new SnowflakeDatabaseConnection(connection);
    }

    /// <summary>
    ///     Executes query and returns a database reader.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override async Task<IDatabaseReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
    {
        var snowflakeConnection = (SnowflakeDatabaseConnection)connection;
        var command = await snowflakeConnection.CreateCommandAsync(cancellationToken).ConfigureAwait(false);

        command.CommandText = _query;
        command.CommandTimeout = _configuration.CommandTimeout;

        foreach (var param in _parameters)
        {
            command.AddParameter(param.Name, param.Value);
        }

        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return reader;
    }

    /// <summary>
    ///     Maps a database row to an object.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <returns>The mapped object.</returns>
    protected override T MapRow(IDatabaseReader reader)
    {
        var row = GetRow(reader);

        if (_mapper != null)
            return _mapper(row);

        if (_cachedMapper != null)
            return _cachedMapper(row);

        return MapConventionBased(row);
    }

    /// <summary>
    ///     Attempts to map a database row to an object.
    ///     Uses row-level error handling when configured.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <param name="item">The mapped item.</param>
    /// <returns>True when the row should be emitted; otherwise false.</returns>
    protected override bool TryMapRow(IDatabaseReader reader, out T item)
    {
        var row = GetRow(reader);

        try
        {
            if (_mapper != null)
            {
                item = _mapper(row);
                return true;
            }

            if (_cachedMapper != null)
            {
                item = _cachedMapper(row);
                return true;
            }

            item = MapConventionBased(row);
            return true;
        }
        catch (Exception ex)
        {
            if (_configuration.RowErrorHandler?.Invoke(ex, row) == true || _continueOnError)
            {
                item = default!;
                return false;
            }

            throw;
        }
    }

    /// <summary>
    ///     Maps a row using convention-based mapping.
    /// </summary>
    /// <param name="row">The Snowflake row.</param>
    /// <returns>The mapped object.</returns>
    protected virtual T MapConventionBased(SnowflakeRow row)
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

    private SnowflakeRow GetRow(IDatabaseReader reader)
    {
        var snowflakeReader = (SnowflakeDatabaseReader)reader;
        var dataReader = snowflakeReader.Reader;

        if (_cachedRow == null || !ReferenceEquals(_cachedReader, dataReader))
        {
            _cachedReader = dataReader;
            _cachedRow = new SnowflakeRow(reader, _configuration.CaseInsensitiveMapping);
        }

        return _cachedRow;
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
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var snowflakeAttribute = property.GetCustomAttribute<SnowflakeColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || snowflakeAttribute?.Ignore == true;
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
        var snowflakeAttr = property.GetCustomAttribute<SnowflakeColumnAttribute>();

        if (snowflakeAttr?.Name is { Length: > 0 } sfName)
            return sfName;

        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();

        return columnAttr?.Name is { Length: > 0 } commonName
            ? commonName
            : property.Name;
    }

    private static Func<SnowflakeRow, T>? ResolveDefaultMapper(
        Func<SnowflakeRow, T>? mapper,
        SnowflakeConfiguration configuration,
        bool continueOnError)
    {
        if (mapper != null)
            return mapper;

        return configuration.CacheMappingMetadata
            ? MapperCache.GetOrAdd(typeof(T), _ => SnowflakeMapperBuilder.BuildMapper<T>(configuration))
            : SnowflakeMapperBuilder.BuildMapper<T>(configuration);
    }

    private sealed record PropertyBinding(Type PropertyType, string ColumnName, Action<T, object?> Setter);
}
