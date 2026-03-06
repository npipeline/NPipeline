using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using MySqlConnector;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.Connectors.Nodes;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.Nodes;

/// <summary>
///     MySQL source node for reading data from a MySQL database.
/// </summary>
/// <typeparam name="T">The type of objects emitted by the source.</typeparam>
public class MySqlSourceNode<T> : DatabaseSourceNode<IDatabaseReader, T>
{
    private static readonly ConcurrentDictionary<Type, Func<MySqlRow, T>> MapperCache = new();
    private static readonly Lazy<IReadOnlyList<PropertyBinding>> CachedBindings = new(BuildBindings);
    private static readonly Lazy<Func<T>> CachedCreateInstance = new(() => BuildCreateInstanceDelegate());

    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        MySqlStorageResolverFactory.CreateResolver,
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly Func<MySqlRow, T>? _cachedMapper;
    private readonly MySqlConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly IMySqlConnectionPool? _connectionPool;
    private readonly bool _continueOnError;
    private readonly Func<MySqlRow, T>? _mapper;
    private readonly DatabaseParameter[] _parameters;
    private readonly string _query;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;

    // Reader caching to avoid repeated column-ordinal dictionary construction
    private MySqlDataReader? _cachedReader;
    private MySqlRow? _cachedRow;

    // ---------------------------------------------------------------------------
    // Constructors
    // ---------------------------------------------------------------------------

    /// <summary>
    ///     Initialises a <see cref="MySqlSourceNode{T}" /> from a connection string.
    /// </summary>
    public MySqlSourceNode(
        string connectionString,
        string query,
        MySqlConfiguration? configuration = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _connectionPool = new MySqlConnectionPool(connectionString);
        _mapper = null;
        _query = query;
        _parameters = [];
        _continueOnError = _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(null, _configuration);
    }

    /// <summary>
    ///     Initialises a <see cref="MySqlSourceNode{T}" /> from a connection string with a custom mapper.
    /// </summary>
    public MySqlSourceNode(
        string connectionString,
        string query,
        Func<MySqlRow, T>? customMapper,
        MySqlConfiguration? configuration = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _connectionPool = new MySqlConnectionPool(connectionString);
        _mapper = customMapper;
        _query = query;
        _parameters = [];
        _continueOnError = _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(customMapper, _configuration);
    }

    /// <summary>
    ///     Initialises a <see cref="MySqlSourceNode{T}" /> from a shared connection pool.
    /// </summary>
    public MySqlSourceNode(
        IMySqlConnectionPool connectionPool,
        string query,
        MySqlConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
        _mapper = null;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;

        _cachedMapper = ResolveDefaultMapper(null, _configuration);
    }

    /// <summary>
    ///     Initialises a <see cref="MySqlSourceNode{T}" /> from a shared connection pool with a custom mapper.
    /// </summary>
    public MySqlSourceNode(
        IMySqlConnectionPool connectionPool,
        string query,
        Func<MySqlRow, T>? customMapper,
        MySqlConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
        _mapper = customMapper;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;

        _cachedMapper = ResolveDefaultMapper(customMapper, _configuration);
    }

    /// <summary>
    ///     Initialises a <see cref="MySqlSourceNode{T}" /> from a <see cref="StorageUri" />.
    /// </summary>
    public MySqlSourceNode(
        StorageUri uri,
        string query,
        IStorageResolver? resolver = null,
        Func<MySqlRow, T>? customMapper = null,
        MySqlConfiguration? configuration = null,
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
        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(customMapper, _configuration);
    }

    /// <summary>
    ///     Initialises a <see cref="MySqlSourceNode{T}" /> from an explicit <see cref="IStorageProvider" /> and
    ///     <see cref="StorageUri" />.
    /// </summary>
    public MySqlSourceNode(
        IStorageProvider provider,
        StorageUri uri,
        string query,
        Func<MySqlRow, T>? customMapper = null,
        MySqlConfiguration? configuration = null,
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
        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _cachedMapper = ResolveDefaultMapper(customMapper, _configuration);
    }

    // ---------------------------------------------------------------------------
    // Base class overrides
    // ---------------------------------------------------------------------------

    /// <inheritdoc />
    protected override bool StreamResults => _configuration.StreamResults;

    /// <inheritdoc />
    protected override DeliverySemantic DeliverySemantic => _configuration.DeliverySemantic;

    /// <inheritdoc />
    protected override CheckpointStrategy CheckpointStrategy => _configuration.CheckpointStrategy;

    /// <inheritdoc />
    protected override async Task<IDatabaseConnection> GetConnectionAsync(
        CancellationToken cancellationToken)
    {
        if (_storageUri is not null)
        {
            var provider = _storageProvider ?? StorageProviderFactory.GetProviderOrThrow(
                _storageResolver ?? DefaultResolver.Value,
                _storageUri);

            if (provider is IDatabaseStorageProvider db)
                return await db.GetConnectionAsync(_storageUri, cancellationToken).ConfigureAwait(false);

            throw new InvalidOperationException(
                $"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        var connection = _connectionName is { Length: > 0 }
            ? await _connectionPool!.GetConnectionAsync(_connectionName, cancellationToken).ConfigureAwait(false)
            : await _connectionPool!.GetConnectionAsync(cancellationToken).ConfigureAwait(false);

        return new MySqlDatabaseConnection(connection);
    }

    /// <inheritdoc />
    protected override async Task<IDatabaseReader> ExecuteQueryAsync(
        IDatabaseConnection connection,
        CancellationToken cancellationToken)
    {
        var mysqlConnection = (MySqlDatabaseConnection)connection;
        var command = await mysqlConnection.CreateCommandAsync(cancellationToken).ConfigureAwait(false);

        command.CommandText = _query;
        command.CommandTimeout = _configuration.CommandTimeout;

        foreach (var param in _parameters)
        {
            command.AddParameter(param.Name, param.Value);
        }

        return await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override T MapRow(IDatabaseReader reader)
    {
        var row = GetRow(reader);

        if (_mapper is not null)
            return _mapper(row);

        if (_cachedMapper is not null)
            return _cachedMapper(row);

        return MapConventionBased(row);
    }

    /// <inheritdoc />
    protected override bool TryMapRow(IDatabaseReader reader, out T item)
    {
        var row = GetRow(reader);

        try
        {
            if (_mapper is not null)
            {
                item = _mapper(row);
                return true;
            }

            if (_cachedMapper is not null)
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

    // ---------------------------------------------------------------------------
    // Convention-based mapping
    // ---------------------------------------------------------------------------

    /// <summary>
    ///     Maps a row using convention-based property binding.
    /// </summary>
    protected virtual T MapConventionBased(MySqlRow row)
    {
        var instance = CachedCreateInstance.Value();

        foreach (var binding in CachedBindings.Value)
        {
            try
            {
                var value = row.GetValue(binding.ColumnName);

                if (value is null)
                    continue;

                var converted = ConvertValue(value, binding.PropertyType);

                if (converted is not null
                    || binding.PropertyType.IsClass
                    || Nullable.GetUnderlyingType(binding.PropertyType) is not null)
                    binding.Setter(instance, converted);
            }
            catch
            {
                if (!_continueOnError)
                    throw;
            }
        }

        return instance;
    }

    // ---------------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------------

    private MySqlRow GetRow(IDatabaseReader reader)
    {
        var mysqlReader = (MySqlDatabaseReader)reader;
        var dataReader = mysqlReader.Reader;

        if (_cachedRow is null || !ReferenceEquals(_cachedReader, dataReader))
        {
            _cachedReader = dataReader;
            _cachedRow = new MySqlRow(reader, _configuration.CaseInsensitiveMapping);
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
        var col = property.GetCustomAttribute<ColumnAttribute>();
        var mysqlCol = property.GetCustomAttribute<MySqlColumnAttribute>();

        return col?.Ignore == true || mysqlCol?.Ignore == true
                                   || property.IsDefined(typeof(IgnoreColumnAttribute), true);
    }

    private static Action<T, object?> BuildSetter(PropertyInfo property)
    {
        var inst = Expression.Parameter(typeof(T), "instance");
        var val = Expression.Parameter(typeof(object), "value");
        var converted = Expression.Convert(val, property.PropertyType);
        var setCall = Expression.Call(inst, property.SetMethod!, converted);
        return Expression.Lambda<Action<T, object?>>(setCall, inst, val).Compile();
    }

    private static Func<T> BuildCreateInstanceDelegate()
    {
        if (typeof(T).IsValueType)
            return Expression.Lambda<Func<T>>(Expression.Default(typeof(T))).Compile();

        var ctor = typeof(T).GetConstructor(Type.EmptyTypes)
                   ?? throw new InvalidOperationException(
                       $"Type '{typeof(T).FullName}' does not have a parameterless constructor.");

        return Expression.Lambda<Func<T>>(Expression.New(ctor)).Compile();
    }

    private static string GetColumnName(PropertyInfo property)
    {
        if (property.GetCustomAttribute<MySqlColumnAttribute>() is { } mySqlAttr
            && !string.IsNullOrEmpty(mySqlAttr.Name))
            return mySqlAttr.Name;

        if (property.GetCustomAttribute<ColumnAttribute>() is { } colAttr
            && !string.IsNullOrEmpty(colAttr.Name))
            return colAttr.Name;

        return property.Name;
    }

    private static Func<MySqlRow, T> ResolveDefaultMapper(
        Func<MySqlRow, T>? customMapper,
        MySqlConfiguration configuration)
    {
        if (customMapper is not null)
            return customMapper;

        return configuration.CacheMappingMetadata
            ? MapperCache.GetOrAdd(typeof(T), _ => MySqlMapperBuilder.BuildMapper<T>(configuration))
            : MySqlMapperBuilder.BuildMapper<T>(configuration);
    }

    private sealed record PropertyBinding(
        Type PropertyType,
        string ColumnName,
        Action<T, object?> Setter);
}
