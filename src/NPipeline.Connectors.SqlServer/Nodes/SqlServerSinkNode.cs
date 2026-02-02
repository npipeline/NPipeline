using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.Nodes;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Writers;
using NPipeline.Connectors.Utilities;

namespace NPipeline.Connectors.SqlServer.Nodes;

/// <summary>
///     SQL Server sink node for writing data to SQL Server database.
/// </summary>
/// <typeparam name="T">The type of objects consumed by sink.</typeparam>
public class SqlServerSinkNode<T> : DatabaseSinkNode<T>
{
    private readonly SqlServerConfiguration _configuration;
    private readonly string? _connectionName;
    private readonly ISqlServerConnectionPool _connectionPool;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly SqlServerWriteStrategy _writeStrategy;

    /// <summary>
    ///     Initializes a new instance of <see cref="SqlServerSinkNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    public SqlServerSinkNode(
        string connectionString,
        string tableName,
        SqlServerConfiguration? configuration = null,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentNullException(nameof(connectionString));

        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));

        _configuration = configuration ?? new SqlServerConfiguration();
        _configuration.Validate();
        _connectionPool = new SqlServerConnectionPool(connectionString);
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
    ///     Initializes a new instance of <see cref="SqlServerSinkNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <param name="customMapper">Optional custom parameter mapper function.</param>
    /// <param name="connectionName">Optional named connection when using a shared pool.</param>
    public SqlServerSinkNode(
        ISqlServerConnectionPool connectionPool,
        string tableName,
        SqlServerConfiguration? configuration = null,
        Func<T, IEnumerable<DatabaseParameter>>? customMapper = null,
        string? connectionName = null)
    {
        ArgumentNullException.ThrowIfNull(connectionPool);

        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentNullException(nameof(tableName));

        _configuration = configuration ?? new SqlServerConfiguration();
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
        var connection = _connectionName is { Length: > 0 }
            ? await _connectionPool.GetConnectionAsync(_connectionName, cancellationToken).ConfigureAwait(false)
            : await _connectionPool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);

        return new SqlServerDatabaseConnection(connection);
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
            SqlServerWriteStrategy.PerRow => Task.FromResult<IDatabaseWriter<T>>(new SqlServerPerRowWriter<T>(connection, _schema, _tableName, _parameterMapper,
                _configuration)),
            SqlServerWriteStrategy.Batch => Task.FromResult<IDatabaseWriter<T>>(new SqlServerBatchWriter<T>(connection, _schema, _tableName, _parameterMapper,
                _configuration)),
            SqlServerWriteStrategy.BulkCopy => throw new NotSupportedException($"Write strategy '{_writeStrategy}' is not supported in the free version"),
            _ => throw new NotSupportedException($"Write strategy '{_writeStrategy}' is not supported"),
        };

        return writer;
    }
}
