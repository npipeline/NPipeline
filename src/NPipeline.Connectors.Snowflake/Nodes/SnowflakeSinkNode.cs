using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Nodes;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.Connectors.Snowflake.Writers;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.Connectors.Snowflake.Nodes;

/// <summary>
///     Snowflake sink node for writing data to Snowflake database.
/// </summary>
/// <typeparam name="T">The type of objects consumed by sink.</typeparam>
public class SnowflakeSinkNode<T> : DatabaseSinkNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => SnowflakeStorageResolverFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly SnowflakeConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly ISnowflakeConnectionPool? _connectionPool;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly string _schema;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;
    private readonly string _tableName;
    private readonly SnowflakeWriteStrategy _writeStrategy;

    /// <summary>
    ///     Initializes a new instance of <see cref="SnowflakeSinkNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    public SnowflakeSinkNode(
        string connectionString,
        string tableName,
        SnowflakeConfiguration? configuration = null,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        ArgumentNullException.ThrowIfNull(tableName);

        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _connectionPool = new SnowflakeConnectionPool(connectionString);
        _tableName = tableName;
        _writeStrategy = _configuration.WriteStrategy;
        _parameterMapper = customMapper;
        _schema = _configuration.Schema;
        _connectionName = null;

        if (_configuration.ValidateIdentifiers)
        {
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
            DatabaseIdentifierValidator.ValidateIdentifier(_schema, nameof(_schema));
        }
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="SnowflakeSinkNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public SnowflakeSinkNode(
        ISnowflakeConnectionPool connectionPool,
        string tableName,
        SnowflakeConfiguration? configuration = null,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);
        ArgumentNullException.ThrowIfNull(tableName);

        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
        _tableName = tableName;
        _writeStrategy = _configuration.WriteStrategy;
        _parameterMapper = customMapper;
        _schema = _configuration.Schema;

        _connectionName = string.IsNullOrWhiteSpace(connectionName)
            ? null
            : connectionName;

        if (_configuration.ValidateIdentifiers)
        {
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
            DatabaseIdentifierValidator.ValidateIdentifier(_schema, nameof(_schema));
        }
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="SnowflakeSinkNode{T}" /> class using a <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing Snowflake connection information.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="resolver">The storage resolver used to obtain storage provider.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: PUBLIC).</param>
    public SnowflakeSinkNode(
        StorageUri uri,
        string tableName,
        SnowflakeWriteStrategy writeStrategy = SnowflakeWriteStrategy.Batch,
        IStorageResolver? resolver = null,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
        SnowflakeConfiguration? configuration = null,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(tableName);

        _storageUri = uri;
        _storageResolver = resolver;
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _parameterMapper = customMapper;
        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _schema = schema ?? _configuration.Schema;
        _connectionName = null;

        if (_configuration.ValidateIdentifiers)
        {
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
            DatabaseIdentifierValidator.ValidateIdentifier(_schema, nameof(_schema));
        }
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="SnowflakeSinkNode{T}" /> class using a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="uri">The storage URI containing Snowflake connection information.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: PUBLIC).</param>
    public SnowflakeSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        string tableName,
        SnowflakeWriteStrategy writeStrategy = SnowflakeWriteStrategy.Batch,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
        SnowflakeConfiguration? configuration = null,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(tableName);

        _storageProvider = provider;
        _storageUri = uri;
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _parameterMapper = customMapper;
        _configuration = configuration ?? new SnowflakeConfiguration();
        _configuration.Validate();
        _schema = schema ?? _configuration.Schema;
        _connectionName = null;

        if (_configuration.ValidateIdentifiers)
        {
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
            DatabaseIdentifierValidator.ValidateIdentifier(_schema, nameof(_schema));
        }
    }

    /// <summary>
    ///     Gets whether to use transactions.
    /// </summary>
    protected override bool UseTransaction => _configuration.UseTransaction;

    /// <summary>
    ///     Gets batch size for batch writes.
    /// </summary>
    protected override int BatchSize => _configuration.BatchSize;

    /// <summary>
    ///     Gets delivery semantic.
    /// </summary>
    protected override DeliverySemantic DeliverySemantic => _configuration.DeliverySemantic;

    /// <summary>
    ///     Gets checkpoint strategy.
    /// </summary>
    protected override CheckpointStrategy CheckpointStrategy => _configuration.CheckpointStrategy;

    /// <summary>
    ///     Gets whether to continue on error.
    /// </summary>
    protected override bool ContinueOnError => _configuration.ContinueOnError;

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
    ///     Creates a database writer for the connection based on the configured write strategy.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override Task<IDatabaseWriter<T>> CreateWriterAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
    {
        var writer = _writeStrategy switch
        {
            SnowflakeWriteStrategy.PerRow => Task.FromResult<IDatabaseWriter<T>>(
                new SnowflakePerRowWriter<T>(connection, _schema, _tableName, _parameterMapper, _configuration)),
            SnowflakeWriteStrategy.Batch => Task.FromResult<IDatabaseWriter<T>>(
                new SnowflakeBatchWriter<T>(connection, _schema, _tableName, _parameterMapper, _configuration)),
            SnowflakeWriteStrategy.StagedCopy => Task.FromResult<IDatabaseWriter<T>>(
                new SnowflakeStagedCopyWriter<T>(connection, _schema, _tableName, _parameterMapper, _configuration)),
            _ => throw new NotSupportedException($"Write strategy '{_writeStrategy}' is not supported"),
        };

        return writer;
    }
}
