using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Mapping;
using NPipeline.Connectors.Nodes;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.Nodes;

/// <summary>
///     Source node that reads data from an AWS Redshift cluster.
///     Supports streaming reads with configurable fetch size and checkpointing.
/// </summary>
/// <typeparam name="T">The type of objects emitted by source.</typeparam>
public class RedshiftSourceNode<T> : DatabaseSourceNode<IDatabaseReader, T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        RedshiftStorageResolverFactory.CreateResolver,
        LazyThreadSafetyMode.ExecutionAndPublication);

    private static readonly ConcurrentDictionary<Type, Func<RedshiftRow, T>> MapperCache = new();
    private static readonly Lazy<IReadOnlyList<PropertyBinding>> CachedBindings = new(BuildBindings);
    private static readonly Lazy<Func<T>> CachedCreateInstance = new(() => BuildCreateInstanceDelegate());

    private readonly Func<RedshiftRow, T>? _cachedMapper;
    private readonly RedshiftConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly IRedshiftConnectionPool? _connectionPool;
    private readonly bool _continueOnError;
    private readonly Func<RedshiftRow, T>? _mapper;
    private readonly DatabaseParameter[] _parameters;
    private readonly string _query;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;
    private IDatabaseReader? _cachedReader;
    private RedshiftRow? _cachedRow;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftSourceNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    public RedshiftSourceNode(
        string connectionString,
        string query,
        Func<RedshiftRow, T>? mapper = null,
        RedshiftConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new RedshiftConfiguration();
        _connectionPool = new RedshiftConnectionPool(connectionString);
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _storageProvider = null;
        _storageResolver = null;
        _storageUri = null;
        _cachedMapper = ResolveDefaultMapper(mapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftSourceNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public RedshiftSourceNode(
        IRedshiftConnectionPool connectionPool,
        string query,
        Func<RedshiftRow, T>? mapper = null,
        RedshiftConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _configuration = configuration ?? new RedshiftConfiguration();
        _connectionPool = connectionPool;
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;

        _storageProvider = null;
        _storageResolver = null;
        _storageUri = null;

        _cachedMapper = ResolveDefaultMapper(mapper, _configuration, _continueOnError);
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftSourceNode{T}" /> class using a <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing Redshift connection information.</param>
    /// <param name="query">The SQL query.</param>
    /// <param name="resolver">The storage resolver used to obtain storage provider.</param>
    /// <param name="mapper">Optional custom mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="continueOnError">Whether to continue on row-level errors.</param>
    public RedshiftSourceNode(
        StorageUri uri,
        string query,
        IStorageResolver? resolver = null,
        Func<RedshiftRow, T>? mapper = null,
        RedshiftConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        bool continueOnError = false)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (string.IsNullOrWhiteSpace(query))
            throw new ArgumentNullException(nameof(query));

        _storageUri = uri;
        _storageResolver = resolver;
        _storageProvider = null;
        _configuration = configuration ?? new RedshiftConfiguration();
        _mapper = mapper;
        _query = query;
        _parameters = parameters ?? [];
        _continueOnError = continueOnError || _configuration.ContinueOnError || !_configuration.ThrowOnMappingError;
        _connectionName = null;
        _connectionPool = null;
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
                return await databaseProvider.GetConnectionAsync(_storageUri, cancellationToken).ConfigureAwait(false);

            throw new InvalidOperationException($"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        var connection = _connectionName is { Length: > 0 }
            ? await _connectionPool!.GetConnectionAsync(_connectionName, cancellationToken)
            : await _connectionPool!.GetConnectionAsync(cancellationToken);

        return new RedshiftDatabaseConnection(connection);
    }

    /// <summary>
    ///     Executes query and returns a database reader.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override async Task<IDatabaseReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
    {
        var redshiftConnection = (RedshiftDatabaseConnection)connection;

        if (!string.IsNullOrWhiteSpace(_configuration.QueryGroup))
        {
            await using var setQueryGroupCommand = await redshiftConnection.CreateCommandAsync(cancellationToken);
            var escapedQueryGroup = _configuration.QueryGroup.Replace("'", "''", StringComparison.Ordinal);
            setQueryGroupCommand.CommandText = $"SET query_group TO '{escapedQueryGroup}'";
            setQueryGroupCommand.CommandTimeout = _configuration.CommandTimeout;
            await setQueryGroupCommand.ExecuteNonQueryAsync(cancellationToken);
        }

        var command = await redshiftConnection.CreateCommandAsync(cancellationToken);

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
            _cachedRow = new RedshiftRow(reader);
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
    ///     Attempts to map a database row to an object.
    ///     Override to skip rows on errors or apply custom row-level handling.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <param name="item">The mapped item.</param>
    /// <returns>True when the row should be emitted; otherwise false to skip.</returns>
    protected override bool TryMapRow(IDatabaseReader reader, out T item)
    {
        item = default!;

        try
        {
            item = MapRow(reader);
            return true;
        }
        catch
        {
            if (_continueOnError)
                return false;

            throw;
        }
    }

    /// <summary>
    ///     Maps a row using convention-based mapping.
    /// </summary>
    /// <param name="row">The Redshift row.</param>
    /// <returns>The mapped object.</returns>
    protected virtual T MapConventionBased(RedshiftRow row)
    {
        var instance = CachedCreateInstance.Value();

        foreach (var binding in CachedBindings.Value)
        {
            try
            {
                if (row.HasColumn(binding.ColumnName))
                {
                    var value = row.Get<object>(binding.ColumnName);

                    if (value != null)
                    {
                        var convertedValue = ConvertValue(value, binding.PropertyType);

                        if (convertedValue != null || binding.PropertyType.IsClass || Nullable.GetUnderlyingType(binding.PropertyType) != null)
                            binding.Setter(instance, convertedValue);
                    }
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
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var redshiftColumnAttribute = property.GetCustomAttribute<RedshiftColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || redshiftColumnAttribute?.Ignore == true;
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
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var columnAttr = property.GetCustomAttribute<RedshiftColumnAttribute>();

        if (columnAttribute is { Name.Length: > 0 })
            return columnAttribute.Name;

        return columnAttr?.Name is { Length: > 0 } name
            ? name
            : ConvertToSnakeCase(property.Name);
    }

    /// <summary>
    ///     Converts a PascalCase string to snake_case.
    /// </summary>
    /// <param name="str">The string to convert.</param>
    /// <returns>The snake_case string.</returns>
    protected static string ConvertToSnakeCase(string str)
    {
        return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x)
            ? "_" + x
            : x.ToString())).ToLowerInvariant();
    }

    private static Func<RedshiftRow, T>? ResolveDefaultMapper(
        Func<RedshiftRow, T>? mapper,
        RedshiftConfiguration configuration,
        bool continueOnError)
    {
        if (mapper != null || continueOnError)
            return mapper;

        return RedshiftMapperBuilder.Build<T>(configuration.NamingConvention);
    }

    private sealed record PropertyBinding(Type PropertyType, string ColumnName, Action<T, object?> Setter);
}
