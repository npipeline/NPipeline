using System.Text;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Exceptions;
using NPipeline.Connectors.Aws.Redshift.Mapping;
using NPipeline.Connectors.Nodes;
using NPipeline.DataFlow;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift.Nodes;

/// <summary>
///     Source node for reading from Redshift Spectrum external tables.
///     Queries data directly in S3 without loading into Redshift.
/// </summary>
public sealed class RedshiftSpectrumSourceNode<T> : DatabaseSourceNode<IDatabaseReader, T>
{
    private readonly SpectrumConfiguration _config;
    private readonly IRedshiftConnectionPool _connectionPool;
    private readonly Func<RedshiftRow, T>? _customMapper;
    private readonly string _pipelineId;
    private readonly string _query;
    private IDatabaseReader? _cachedReader;
    private RedshiftRow? _cachedRow;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftSpectrumSourceNode{T}" /> class.
    /// </summary>
    /// <param name="connectionString">The Redshift connection string.</param>
    /// <param name="query">The SQL query to execute against the external table.</param>
    /// <param name="config">Optional Spectrum configuration.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    public RedshiftSpectrumSourceNode(
        string connectionString,
        string query,
        SpectrumConfiguration? config = null,
        Func<RedshiftRow, T>? customMapper = null)
        : this(
            new RedshiftConnectionPool((config ?? new SpectrumConfiguration { ConnectionString = connectionString }).BuildConnectionString()),
            query,
            config ?? new SpectrumConfiguration { ConnectionString = connectionString },
            customMapper)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftSpectrumSourceNode{T}" /> class with connection pool.
    /// </summary>
    /// <param name="connectionPool">The connection pool.</param>
    /// <param name="query">The SQL query to execute against the external table.</param>
    /// <param name="config">The Spectrum configuration.</param>
    /// <param name="customMapper">Optional custom mapper function.</param>
    public RedshiftSpectrumSourceNode(
        IRedshiftConnectionPool connectionPool,
        string query,
        SpectrumConfiguration config,
        Func<RedshiftRow, T>? customMapper = null)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _query = query ?? throw new ArgumentNullException(nameof(query));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _customMapper = customMapper;
        _pipelineId = Guid.NewGuid().ToString("N")[..8];
    }

    /// <summary>
    ///     Gets the unique node identifier.
    /// </summary>
    public string NodeId => $"RedshiftSpectrum_{typeof(T).Name}_{_pipelineId}";

    /// <summary>
    ///     Gets whether to stream results.
    /// </summary>
    protected override bool StreamResults => _config.StreamResults;

    /// <summary>
    ///     Gets fetch size for streaming.
    /// </summary>
    protected override int FetchSize => _config.FetchSize;

    /// <summary>
    ///     Initializes the node and validates configuration.
    /// </summary>
    /// <param name="serviceProvider">Optional service provider.</param>
    public void Initialize(IServiceProvider? serviceProvider = null)
    {
        _config.Validate();
    }

    /// <summary>
    ///     Initializes the source node and returns a data pipe.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data pipe containing the data.</returns>
    public override IDataPipe<T> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        _config.Validate();

        // Create external table if requested
        if (_config.CreateIfNotExists)
        {
            Task.Run(() => CreateExternalTableIfNotExistsAsync(cancellationToken), cancellationToken)
                .GetAwaiter()
                .GetResult();
        }

        return base.Initialize(context, cancellationToken);
    }

    /// <summary>
    ///     Gets a database connection asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected override async Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = await _connectionPool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
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
        var command = await redshiftConnection.CreateCommandAsync(cancellationToken).ConfigureAwait(false);

        command.CommandText = _query;
        command.CommandTimeout = _config.CommandTimeout;

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
        if (_cachedRow == null || !ReferenceEquals(_cachedReader, reader))
        {
            _cachedReader = reader;
            _cachedRow = new RedshiftRow(reader);
        }

        var row = _cachedRow;

        if (_customMapper != null)
            return _customMapper(row);

        var mapper = RedshiftMapperBuilder.Build<T>(_config.NamingConvention);
        return mapper(row);
    }

    /// <summary>
    ///     Attempts to map a database row to an object.
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
        catch (Exception ex) when (_config.ContinueOnError)
        {
            if (_config.ThrowOnMappingError)
                throw new RedshiftMappingException($"Failed to map row to {typeof(T).Name}", ex);

            return false;
        }
    }

    /// <summary>
    ///     Creates the external table if it doesn't exist.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task CreateExternalTableIfNotExistsAsync(CancellationToken cancellationToken)
    {
        var createSql = BuildCreateExternalTableSql();

        await using var connection = await _connectionPool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = createSql;
        command.CommandTimeout = _config.CommandTimeout;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Builds the CREATE EXTERNAL TABLE SQL statement.
    /// </summary>
    /// <returns>The SQL statement.</returns>
    private string BuildCreateExternalTableSql()
    {
        var sb = new StringBuilder();

        // Extract table name from query (simple heuristic)
        var tableName = ExtractTableNameFromQuery();

        sb.Append($"CREATE EXTERNAL TABLE IF NOT EXISTS \"{_config.ExternalSchema}\".\"{tableName}\"");
        sb.Append($" ({_config.ColumnDefinitions})");

        if (!string.IsNullOrWhiteSpace(_config.PartitionedBy))
            sb.Append($" {_config.PartitionedBy}");

        sb.Append($" ROW FORMAT SERDE '{_config.RowFormatSerde ?? "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"}'");
        sb.Append($" STORED AS {_config.FileFormat}");
        sb.Append($" LOCATION '{_config.S3Path}'");

        if (_config.UseManifest)
            sb.Append(" TABLE PROPERTIES ('manifest'='true')");
        else
            sb.Append(" TABLE PROPERTIES ('skip.header.line.count'='1')");

        return sb.ToString();
    }

    /// <summary>
    ///     Extracts the table name from the query using a simple heuristic.
    /// </summary>
    /// <returns>The extracted table name or a generated default.</returns>
    private string ExtractTableNameFromQuery()
    {
        // Simple heuristic: look for FROM clause
        var fromIndex = _query.IndexOf("FROM", StringComparison.OrdinalIgnoreCase);

        if (fromIndex < 0)
            return $"external_table_{_pipelineId}";

        var afterFrom = _query.Substring(fromIndex + 4).Trim();
        var parts = afterFrom.Split([' ', '\t', '\n', '\r', ';'], StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length == 0)
            return $"external_table_{_pipelineId}";

        // Remove schema prefix if present
        var tableName = parts[0].Trim('"');

        if (tableName.Contains('.'))
            tableName = tableName.Split('.').Last();

        return tableName;
    }
}
