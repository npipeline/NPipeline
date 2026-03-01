using System.Data;
using System.Text;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Exceptions;
using NPipeline.Connectors.Aws.Redshift.Mapping;

namespace NPipeline.Connectors.Aws.Redshift.Writers;

/// <summary>
///     Writes rows to Redshift one at a time using individual INSERT statements.
///     Suitable only for very low-volume or development use.
/// </summary>
/// <typeparam name="T">The type of row to write.</typeparam>
internal sealed class RedshiftPerRowWriter<T> : IAsyncDisposable
{
    private readonly IReadOnlyList<string> _columnNames;
    private readonly RedshiftConfiguration _configuration;
    private readonly IRedshiftConnectionPool _connectionPool;
    private readonly RedshiftExceptionHandler _exceptionHandler;
    private readonly string _insertSql;
    private readonly string _schema;
    private readonly string _table;
    private readonly Func<T, object?[]> _valueExtractor;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftPerRowWriter{T}" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool to use for database connections.</param>
    /// <param name="schema">The schema name of the target table.</param>
    /// <param name="table">The table name to write to.</param>
    /// <param name="configuration">The Redshift configuration.</param>
    public RedshiftPerRowWriter(
        IRedshiftConnectionPool connectionPool,
        string schema,
        string table,
        RedshiftConfiguration configuration)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

        _valueExtractor = RedshiftParameterMapper.BuildValueExtractor<T>(configuration.NamingConvention);
        _columnNames = RedshiftParameterMapper.GetColumnNames<T>(configuration.NamingConvention);
        _insertSql = BuildInsertSql();

        _exceptionHandler = new RedshiftExceptionHandler(
            configuration.MaxRetryAttempts,
            configuration.RetryDelay);
    }

    /// <summary>
    ///     Disposes the writer asynchronously.
    /// </summary>
    /// <returns>A value task.</returns>
    public ValueTask DisposeAsync()
    {
        if (_disposed)
            return ValueTask.CompletedTask;

        _disposed = true;
        return ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Writes a single row to Redshift.
    /// </summary>
    /// <param name="row">The row to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task WriteAsync(T row, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _exceptionHandler.ExecuteWithRetryAsync(async () =>
        {
            var npgsqlConnection = await _connectionPool.GetConnectionAsync(cancellationToken);
            await using var connection = new RedshiftDatabaseConnection(npgsqlConnection);

            await using var command = await connection.CreateCommandAsync(cancellationToken);
            command.CommandText = _insertSql;
            command.CommandType = CommandType.Text;

            var values = _valueExtractor(row);

            for (var i = 0; i < _columnNames.Count && i < values.Length; i++)
            {
                command.AddParameter(_columnNames[i], values[i]);
            }

            await command.ExecuteNonQueryAsync(cancellationToken);
        }, cancellationToken);
    }

    /// <summary>
    ///     Flushes any pending writes. For PerRowWriter, this is a no-op since writes are immediate.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A completed task.</returns>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return Task.CompletedTask;
    }

    private string BuildInsertSql()
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO \"{_schema}\".\"{_table}\" (");

        for (var i = 0; i < _columnNames.Count; i++)
        {
            if (i > 0)
                sb.Append(", ");

            sb.Append($"\"{_columnNames[i]}\"");
        }

        sb.Append(") VALUES (");

        for (var i = 0; i < _columnNames.Count; i++)
        {
            if (i > 0)
                sb.Append(", ");

            sb.Append($"@{_columnNames[i]}");
        }

        sb.Append(')');

        return sb.ToString();
    }
}
