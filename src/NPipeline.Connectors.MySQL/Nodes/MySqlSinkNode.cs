using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Nodes;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Writers;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.Connectors.MySql.Nodes;

/// <summary>
///     MySQL sink node for writing data to a MySQL database table.
/// </summary>
/// <typeparam name="T">The type of objects consumed by the sink.</typeparam>
public class MySqlSinkNode<T> : DatabaseSinkNode<T>
{
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => MySqlStorageResolverFactory.CreateResolver(),
        LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly MySqlConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly IMySqlConnectionPool? _connectionPool;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private readonly StorageUri? _storageUri;
    private readonly string _tableName;
    private readonly MySqlWriteStrategy _writeStrategy;

    /// <summary>
    ///     Initialises a <see cref="MySqlSinkNode{T}" /> from a connection string.
    /// </summary>
    public MySqlSinkNode(
        string connectionString,
        string tableName,
        MySqlConfiguration? configuration = null,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        ArgumentNullException.ThrowIfNull(tableName);

        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _connectionPool = new MySqlConnectionPool(connectionString);
        _tableName = tableName;
        _writeStrategy = _configuration.WriteStrategy;
        _parameterMapper = customMapper;
        _connectionName = null;

        if (_configuration.ValidateIdentifiers)
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
    }

    /// <summary>
    ///     Initialises a <see cref="MySqlSinkNode{T}" /> from a shared connection pool.
    /// </summary>
    public MySqlSinkNode(
        IMySqlConnectionPool connectionPool,
        string tableName,
        MySqlConfiguration? configuration = null,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);
        ArgumentNullException.ThrowIfNull(tableName);

        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
        _tableName = tableName;
        _writeStrategy = _configuration.WriteStrategy;
        _parameterMapper = customMapper;
        _connectionName = string.IsNullOrWhiteSpace(connectionName) ? null : connectionName;

        if (_configuration.ValidateIdentifiers)
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
    }

    /// <summary>
    ///     Initialises a <see cref="MySqlSinkNode{T}" /> from a <see cref="StorageUri" />.
    /// </summary>
    public MySqlSinkNode(
        StorageUri uri,
        string tableName,
        MySqlWriteStrategy writeStrategy = MySqlWriteStrategy.Batch,
        IStorageResolver? resolver = null,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
        MySqlConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(tableName);

        _storageUri = uri;
        _storageResolver = resolver;
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _parameterMapper = customMapper;
        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _connectionName = null;

        if (_configuration.ValidateIdentifiers)
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
    }

    /// <summary>
    ///     Initialises a <see cref="MySqlSinkNode{T}" /> from an explicit <see cref="IStorageProvider" /> and
    ///     <see cref="StorageUri" />.
    /// </summary>
    public MySqlSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        string tableName,
        MySqlWriteStrategy writeStrategy = MySqlWriteStrategy.Batch,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
        MySqlConfiguration? configuration = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(tableName);

        _storageProvider = provider;
        _storageUri = uri;
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _parameterMapper = customMapper;
        _configuration = configuration ?? new MySqlConfiguration();
        _configuration.Validate();
        _connectionName = null;

        if (_configuration.ValidateIdentifiers)
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
    }

    /// <inheritdoc />
    protected override bool UseTransaction => _configuration.UseTransaction;

    /// <inheritdoc />
    protected override int BatchSize => _configuration.BatchSize;

    /// <inheritdoc />
    protected override DeliverySemantic DeliverySemantic => _configuration.DeliverySemantic;

    /// <inheritdoc />
    protected override CheckpointStrategy CheckpointStrategy => _configuration.CheckpointStrategy;

    /// <inheritdoc />
    protected override bool ContinueOnError => _configuration.ContinueOnError;

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
    protected override Task<IDatabaseWriter<T>> CreateWriterAsync(
        IDatabaseConnection connection,
        CancellationToken cancellationToken)
    {
        IDatabaseWriter<T> writer = _writeStrategy switch
        {
            MySqlWriteStrategy.PerRow =>
                new MySqlPerRowWriter<T>(connection, _tableName, _parameterMapper, _configuration),
            MySqlWriteStrategy.Batch =>
                new MySqlBatchWriter<T>(connection, _tableName, _parameterMapper, _configuration),
            MySqlWriteStrategy.BulkLoad =>
                new MySqlBulkLoadWriter<T>(connection, _tableName, _parameterMapper, _configuration),
            _ => throw new NotSupportedException(
                $"Write strategy '{_writeStrategy}' is not supported."),
        };

        return Task.FromResult(writer);
    }
}
