using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.Nodes;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.PostgreSQL.Writers;
using NPipeline.Connectors.Utilities;

namespace NPipeline.Connectors.PostgreSQL.Nodes;

/// <summary>
///     PostgreSQL sink node for writing data to PostgreSQL database.
/// </summary>
/// <typeparam name="T">The type of objects consumed by sink.</typeparam>
public class PostgresSinkNode<T> : DatabaseSinkNode<T>
{
    private readonly PostgresConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly IPostgresConnectionPool? _connectionPool;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly PostgresWriteStrategy _writeStrategy;
    private readonly StorageUri? _storageUri;
    private readonly IStorageProvider? _storageProvider;
    private readonly IStorageResolver? _storageResolver;
    private static readonly Lazy<IStorageResolver> DefaultResolver = new(
        () => PostgresStorageResolverFactory.CreateResolver(),
        System.Threading.LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>
    ///     Initializes a new instance of <see cref="PostgresSinkNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="parameterMapper">Optional parameter mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    public PostgresSinkNode(
        string connectionString,
        string tableName,
        PostgresWriteStrategy writeStrategy = PostgresWriteStrategy.Batch,
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null,
        PostgresConfiguration? configuration = null,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(connectionString);
        ArgumentNullException.ThrowIfNull(tableName);

        _configuration = configuration ?? new PostgresConfiguration();
        _configuration.Validate();
        _connectionPool = new PostgresConnectionPool(connectionString);
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _parameterMapper = parameterMapper;
        _schema = schema ?? _configuration.Schema;
        _connectionName = null;

        if (_configuration.ValidateIdentifiers)
        {
            DatabaseIdentifierValidator.ValidateIdentifier(_tableName, nameof(_tableName));
            DatabaseIdentifierValidator.ValidateIdentifier(_schema, nameof(_schema));
        }
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="PostgresSinkNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="parameterMapper">Optional parameter mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public PostgresSinkNode(
        IPostgresConnectionPool connectionPool,
        string tableName,
        PostgresWriteStrategy writeStrategy = PostgresWriteStrategy.Batch,
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null,
        PostgresConfiguration? configuration = null,
        string? schema = null,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));

        _configuration = configuration ?? new PostgresConfiguration();
        _configuration.Validate();
        _connectionPool = connectionPool;
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _parameterMapper = parameterMapper;
        _schema = schema ?? _configuration.Schema;

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
    ///     Initializes a new instance of <see cref="PostgresSinkNode{T}" /> class using a <see cref="StorageUri"/>.
    /// </summary>
    /// <param name="uri">The storage URI containing PostgreSQL connection information.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="resolver">
    /// The storage resolver used to obtain storage provider. If <c>null</c>, a default resolver
    /// created by <see cref="PostgresStorageResolverFactory.CreateResolver" /> is used.
    /// </param>
    /// <param name="parameterMapper">Optional parameter mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="uri" /> is <c>null</c>.</exception>
    public PostgresSinkNode(
        StorageUri uri,
        string tableName,
        PostgresWriteStrategy writeStrategy = PostgresWriteStrategy.Batch,
        IStorageResolver? resolver = null,
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null,
        PostgresConfiguration? configuration = null,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(tableName);

        _storageUri = uri;
        _storageResolver = resolver;
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _parameterMapper = parameterMapper;
        _configuration = configuration ?? new PostgresConfiguration();
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
    ///     Initializes a new instance of <see cref="PostgresSinkNode{T}" /> class using a specific storage provider.
    /// </summary>
    /// <param name="provider">The storage provider.</param>
    /// <param name="uri">The storage URI containing PostgreSQL connection information.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="writeStrategy">The write strategy.</param>
    /// <param name="parameterMapper">Optional parameter mapper function.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="schema">Optional schema name (default: public).</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="provider" /> or <paramref name="uri" /> is <c>null</c>.</exception>
    public PostgresSinkNode(
        IStorageProvider provider,
        StorageUri uri,
        string tableName,
        PostgresWriteStrategy writeStrategy = PostgresWriteStrategy.Batch,
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper = null,
        PostgresConfiguration? configuration = null,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentNullException.ThrowIfNull(tableName);

        _storageProvider = provider;
        _storageUri = uri;
        _tableName = tableName;
        _writeStrategy = writeStrategy;
        _parameterMapper = parameterMapper;
        _configuration = configuration ?? new PostgresConfiguration();
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
            {
                return await databaseProvider.GetConnectionAsync(_storageUri, cancellationToken);
            }

            throw new InvalidOperationException($"Storage provider must implement {nameof(IDatabaseStorageProvider)} to use StorageUri.");
        }

        // Original connection pool logic
        var connection = _connectionName is { Length: > 0 }
            ? await _connectionPool!.GetConnectionAsync(_connectionName, cancellationToken)
            : await _connectionPool!.GetConnectionAsync(cancellationToken);

        return new PostgresDatabaseConnection(connection);
    }

    /// <summary>
    ///     Creates a database writer for the connection.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override Task<IDatabaseWriter<T>> CreateWriterAsync(IDatabaseConnection connection, CancellationToken cancellationToken)
    {
        var writer = _writeStrategy switch
        {
            PostgresWriteStrategy.PerRow => Task.FromResult<IDatabaseWriter<T>>(new PostgresPerRowWriter<T>(connection, _schema, _tableName, _parameterMapper,
                _configuration)),
            PostgresWriteStrategy.Batch => Task.FromResult<IDatabaseWriter<T>>(new PostgresBatchWriter<T>(connection, _schema, _tableName, _parameterMapper,
                _configuration)),
            PostgresWriteStrategy.Copy => throw new NotSupportedException($"Write strategy '{_writeStrategy}' is not supported in the free version"),
            _ => throw new NotSupportedException($"Write strategy '{_writeStrategy}' is not supported"),
        };

        return writer;
    }
}
