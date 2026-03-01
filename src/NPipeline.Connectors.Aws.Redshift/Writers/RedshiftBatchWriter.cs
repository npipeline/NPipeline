using System.Data;
using System.Globalization;
using System.Text;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Exceptions;
using NPipeline.Connectors.Aws.Redshift.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift.Writers;

/// <summary>
///     Writes rows to Redshift using batched multi-row INSERT statements.
///     Supports upsert via staging table pattern.
/// </summary>
/// <typeparam name="T">The type of row to write.</typeparam>
internal sealed class RedshiftBatchWriter<T> : IAsyncDisposable
{
    private readonly List<T> _buffer;
    private readonly IReadOnlyList<string> _columnNames;
    private readonly RedshiftConfiguration _configuration;
    private readonly IRedshiftConnectionPool _connectionPool;
    private readonly RedshiftExceptionHandler _exceptionHandler;
    private readonly string _schema;
    private readonly string _table;
    private readonly Func<T, object?[]> _valueExtractor;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftBatchWriter{T}" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool to use for database connections.</param>
    /// <param name="schema">The schema name of the target table.</param>
    /// <param name="table">The table name to write to.</param>
    /// <param name="configuration">The Redshift configuration.</param>
    public RedshiftBatchWriter(
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

        _exceptionHandler = new RedshiftExceptionHandler(
            configuration.MaxRetryAttempts,
            configuration.RetryDelay);

        _buffer = new List<T>(configuration.BatchSize);
    }

    /// <summary>
    ///     Disposes the writer asynchronously, flushing any remaining rows.
    /// </summary>
    /// <returns>A value task.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        try
        {
            await FlushAsync();
        }
        catch
        {
            // Ignore flush errors during disposal
        }

        _disposed = true;
    }

    /// <summary>
    ///     Buffers a row for batch writing. Flushes automatically when batch size is reached.
    /// </summary>
    /// <param name="row">The row to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task WriteAsync(T row, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _buffer.Add(row);

        if (_buffer.Count >= _configuration.BatchSize)
            await FlushAsync(cancellationToken);
    }

    /// <summary>
    ///     Flushes all buffered rows to Redshift.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_buffer.Count == 0)
            return;

        var rowsToWrite = _buffer.ToList();
        _buffer.Clear();

        await _exceptionHandler.ExecuteWithRetryAsync(async () => { await WriteBatchAsync(rowsToWrite, cancellationToken); }, cancellationToken);
    }

    private async Task WriteBatchAsync(IReadOnlyList<T> rows, CancellationToken cancellationToken)
    {
        var npgsqlConnection = await _connectionPool.GetConnectionAsync(cancellationToken);
        await using var connection = new RedshiftDatabaseConnection(npgsqlConnection);

        IDatabaseTransaction? transaction = null;

        try
        {
            if (_configuration.UseTransaction)
                transaction = await connection.BeginTransactionAsync(cancellationToken);

            if (_configuration.UseUpsert)
            {
                if (_configuration.UseMergeSyntax)
                {
                    // Use direct MERGE with inline VALUES (no staging table)
                    await ExecuteDirectMergeUpsertAsync(connection, transaction, rows, cancellationToken);
                }
                else
                    await WriteWithUpsertAsync(connection, transaction, rows, cancellationToken);
            }
            else
                await WriteDirectAsync(connection, transaction, rows, cancellationToken);

            if (transaction != null)
                await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            if (transaction != null)
                await transaction.RollbackAsync(CancellationToken.None);

            throw;
        }
    }

    /// <summary>
    ///     Executes a MERGE statement with inline VALUES clause for upsert operations.
    ///     This bypasses the staging table pattern for newer Redshift clusters that support MERGE.
    /// </summary>
    private async Task ExecuteDirectMergeUpsertAsync(
        IDatabaseConnection connection,
        IDatabaseTransaction? transaction,
        IReadOnlyList<T> rows,
        CancellationToken cancellationToken)
    {
        var keyColumns = _configuration.UpsertKeyColumns ?? Array.Empty<string>();

        if (keyColumns.Length == 0)
            throw new RedshiftException("UpsertKeyColumns must be specified when UseUpsert is true");

        // Process in batches of 1000 to avoid query size limits
        const int mergeBatchSize = 1000;

        for (var offset = 0; offset < rows.Count; offset += mergeBatchSize)
        {
            var batchSize = Math.Min(mergeBatchSize, rows.Count - offset);
            var batchRows = new T[batchSize];

            for (var i = 0; i < batchSize; i++)
            {
                batchRows[i] = rows[offset + i];
            }

            var mergeSql = BuildDirectMergeStatement(batchRows, keyColumns);

            await using var command = await connection.CreateCommandAsync(cancellationToken);
            command.CommandText = mergeSql;
            command.CommandType = CommandType.Text;

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    /// <summary>
    ///     Builds a MERGE statement with inline VALUES clause.
    /// </summary>
    internal string BuildDirectMergeStatement(T[] rows, string[] keyColumns)
    {
        var sb = new StringBuilder();

        // Build VALUES clause
        var valuesList = new List<string>(rows.Length);

        foreach (var row in rows)
        {
            var values = _valueExtractor(row);
            var valueStrings = values.Select(FormatValueForMerge);
            valuesList.Add($"({string.Join(", ", valueStrings)})");
        }

        // Build column list for source subquery
        var columnList = string.Join(", ", _columnNames.Select(c => $"\"{c}\""));

        // Build MERGE statement
        sb.Append($"MERGE INTO \"{_schema}\".\"{_table}\" AS target");
        sb.Append($" USING (SELECT * FROM (VALUES {string.Join(", ", valuesList)}) AS t({columnList})) AS source");

        // ON clause for key matching
        var joinConditions = keyColumns.Select(k => $"target.\"{k}\" = source.\"{k}\"");
        sb.Append($" ON {string.Join(" AND ", joinConditions)}");

        if (_configuration.OnMergeAction == OnMergeAction.Update)
        {
            // UPDATE SET clause (exclude key columns from update)
            var updateColumns = _columnNames.Where(c => !keyColumns.Contains(c));
            var setClause = string.Join(", ", updateColumns.Select(c => $"\"{c}\" = source.\"{c}\""));
            sb.Append($" WHEN MATCHED THEN UPDATE SET {setClause}");
        }

        // INSERT clause
        var insertColumns = string.Join(", ", _columnNames.Select(c => $"\"{c}\""));
        var insertValues = string.Join(", ", _columnNames.Select(c => $"source.\"{c}\""));
        sb.Append($" WHEN NOT MATCHED THEN INSERT ({insertColumns}) VALUES ({insertValues})");

        return sb.ToString();
    }

    /// <summary>
    ///     Formats a value for inclusion in a MERGE statement's VALUES clause.
    /// </summary>
    private static string FormatValueForMerge(object? value)
    {
        return value switch
        {
            null => "NULL",
            DBNull => "NULL",
            string s => $"'{s.Replace("'", "''")}'",
            DateTime dt => $"'{dt:O}'",
            DateTimeOffset dto => $"'{dto:O}'",
            Guid g => $"'{g}'",
            decimal d => d.ToString(CultureInfo.InvariantCulture),
            double db => db.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString(CultureInfo.InvariantCulture),
            bool b => b
                ? "TRUE"
                : "FALSE",
            _ => value.ToString() ?? "NULL",
        };
    }

    private async Task WriteDirectAsync(
        IDatabaseConnection connection,
        IDatabaseTransaction? transaction,
        IReadOnlyList<T> rows,
        CancellationToken cancellationToken)
    {
        var sql = BuildBatchInsertSql(rows, _schema, _table);
        await using var command = await connection.CreateCommandAsync(cancellationToken);
        command.CommandText = sql;
        command.CommandType = CommandType.Text;

        AddParametersForRows(command, rows);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task WriteWithUpsertAsync(
        IDatabaseConnection connection,
        IDatabaseTransaction? transaction,
        IReadOnlyList<T> rows,
        CancellationToken cancellationToken)
    {
        var stagingTable = GenerateStagingTableName();
        var stagingSchema = _configuration.StagingSchema ?? _schema;

        try
        {
            // 1. Create staging table
            await CreateStagingTableAsync(connection, transaction, stagingSchema, stagingTable, cancellationToken);

            // 2. Insert into staging table
            await InsertIntoStagingAsync(connection, transaction, stagingSchema, stagingTable, rows, cancellationToken);

            // 3. Perform upsert (delete + insert or merge)
            if (_configuration.UseMergeSyntax)
                await MergeFromStagingAsync(connection, transaction, stagingSchema, stagingTable, cancellationToken);
            else
            {
                await DeleteFromStagingAsync(connection, transaction, stagingSchema, stagingTable, cancellationToken);
                await InsertFromStagingAsync(connection, transaction, stagingSchema, stagingTable, cancellationToken);
            }
        }
        finally
        {
            // 4. Drop staging table
            await DropStagingTableAsync(connection, stagingSchema, stagingTable, CancellationToken.None);
        }
    }

    private string GenerateStagingTableName()
    {
        return $"{_configuration.StagingTablePrefix}{_table}_{Guid.NewGuid():N}";
    }

    private async Task CreateStagingTableAsync(
        IDatabaseConnection connection,
        IDatabaseTransaction? transaction,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        var sql = _configuration.UseTempStagingTable
            ? $"CREATE TEMP TABLE \"{table}\" (LIKE \"{_schema}\".\"{_table}\")"
            : $"CREATE TABLE \"{schema}\".\"{table}\" (LIKE \"{_schema}\".\"{_table}\")";

        await using var command = await connection.CreateCommandAsync(cancellationToken);
        command.CommandText = sql;
        command.CommandType = CommandType.Text;

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task InsertIntoStagingAsync(
        IDatabaseConnection connection,
        IDatabaseTransaction? transaction,
        string schema,
        string table,
        IReadOnlyList<T> rows,
        CancellationToken cancellationToken)
    {
        var sql = BuildBatchInsertSql(rows, schema, table);
        await using var command = await connection.CreateCommandAsync(cancellationToken);
        command.CommandText = sql;
        command.CommandType = CommandType.Text;

        AddParametersForRows(command, rows);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task DeleteFromStagingAsync(
        IDatabaseConnection connection,
        IDatabaseTransaction? transaction,
        string stagingSchema,
        string stagingTable,
        CancellationToken cancellationToken)
    {
        var keyColumns = _configuration.UpsertKeyColumns ?? Array.Empty<string>();

        if (keyColumns.Length == 0)
            throw new RedshiftException("UpsertKeyColumns must be specified when UseUpsert is true");

        var joinConditions = string.Join(" AND ",
            keyColumns.Select(k => $"target.\"{k}\" = stage.\"{k}\""));

        var sql = $"""
                   DELETE FROM "{_schema}"."{_table}" AS target
                   USING "{stagingSchema}"."{stagingTable}" AS stage
                   WHERE {joinConditions}
                   """;

        await using var command = await connection.CreateCommandAsync(cancellationToken);
        command.CommandText = sql;
        command.CommandType = CommandType.Text;

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task InsertFromStagingAsync(
        IDatabaseConnection connection,
        IDatabaseTransaction? transaction,
        string stagingSchema,
        string stagingTable,
        CancellationToken cancellationToken)
    {
        var columns = string.Join(", ", _columnNames.Select(c => $"\"{c}\""));

        var sql = $"""
                   INSERT INTO "{_schema}"."{_table}" ({columns})
                   SELECT {columns} FROM "{stagingSchema}"."{stagingTable}"
                   """;

        await using var command = await connection.CreateCommandAsync(cancellationToken);
        command.CommandText = sql;
        command.CommandType = CommandType.Text;

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task MergeFromStagingAsync(
        IDatabaseConnection connection,
        IDatabaseTransaction? transaction,
        string stagingSchema,
        string stagingTable,
        CancellationToken cancellationToken)
    {
        var keyColumns = _configuration.UpsertKeyColumns ?? Array.Empty<string>();

        if (keyColumns.Length == 0)
            throw new RedshiftException("UpsertKeyColumns must be specified when UseUpsert is true");

        var joinConditions = string.Join(" AND ",
            keyColumns.Select(k => $"target.\"{k}\" = stage.\"{k}\""));

        var nonKeyColumns = _columnNames.Where(c => !keyColumns.Contains(c)).ToList();
        var insertColumns = string.Join(", ", _columnNames.Select(c => $"\"{c}\""));
        var insertValues = string.Join(", ", _columnNames.Select(c => $"stage.\"{c}\""));

        string mergeSql;

        if (_configuration.OnMergeAction == OnMergeAction.Skip)
        {
            mergeSql = $"""
                        MERGE INTO "{_schema}"."{_table}" AS target
                        USING "{stagingSchema}"."{stagingTable}" AS stage
                          ON {joinConditions}
                        WHEN NOT MATCHED THEN
                          INSERT ({insertColumns}) VALUES ({insertValues})
                        """;
        }
        else
        {
            var updateSet = string.Join(", ",
                nonKeyColumns.Select(c => $"\"{c}\" = stage.\"{c}\""));

            mergeSql = $"""
                        MERGE INTO "{_schema}"."{_table}" AS target
                        USING "{stagingSchema}"."{stagingTable}" AS stage
                          ON {joinConditions}
                        WHEN MATCHED THEN
                          UPDATE SET {updateSet}
                        WHEN NOT MATCHED THEN
                          INSERT ({insertColumns}) VALUES ({insertValues})
                        """;
        }

        await using var command = await connection.CreateCommandAsync(cancellationToken);
        command.CommandText = mergeSql;
        command.CommandType = CommandType.Text;

        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task DropStagingTableAsync(
        IDatabaseConnection connection,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        try
        {
            var sql = _configuration.UseTempStagingTable
                ? $"DROP TABLE IF EXISTS \"{table}\""
                : $"DROP TABLE IF EXISTS \"{schema}\".\"{table}\"";

            await using var command = await connection.CreateCommandAsync(cancellationToken);
            command.CommandText = sql;
            command.CommandType = CommandType.Text;

            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch
        {
            // Ignore errors when dropping staging table
        }
    }

    private string BuildBatchInsertSql(IReadOnlyList<T> rows, string schema, string table)
    {
        var sb = new StringBuilder();

        sb.Append($"INSERT INTO \"{schema}\".\"{table}\" (");

        for (var i = 0; i < _columnNames.Count; i++)
        {
            if (i > 0)
                sb.Append(", ");

            sb.Append($"\"{_columnNames[i]}\"");
        }

        sb.Append(") VALUES ");

        for (var rowIdx = 0; rowIdx < rows.Count; rowIdx++)
        {
            if (rowIdx > 0)
                sb.Append(", ");

            sb.Append('(');

            for (var colIdx = 0; colIdx < _columnNames.Count; colIdx++)
            {
                if (colIdx > 0)
                    sb.Append(", ");

                sb.Append($"@r{rowIdx}_{_columnNames[colIdx]}");
            }

            sb.Append(')');
        }

        return sb.ToString();
    }

    private void AddParametersForRows(IDatabaseCommand command, IReadOnlyList<T> rows)
    {
        for (var rowIdx = 0; rowIdx < rows.Count; rowIdx++)
        {
            var values = _valueExtractor(rows[rowIdx]);

            for (var colIdx = 0; colIdx < _columnNames.Count && colIdx < values.Length; colIdx++)
            {
                command.AddParameter($"r{rowIdx}_{_columnNames[colIdx]}", values[colIdx]);
            }
        }
    }
}
