using System.Globalization;
using System.IO.Compression;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.Connectors.Aws.Redshift.Exceptions;
using NPipeline.Connectors.Aws.Redshift.Mapping;

namespace NPipeline.Connectors.Aws.Redshift.Writers;

/// <summary>
///     High-performance writer that uploads data to S3 as compressed CSV files
///     and issues a Redshift COPY command for bulk loading.
/// </summary>
/// <typeparam name="T">The type of row to write.</typeparam>
internal sealed class RedshiftCopyFromS3Writer<T> : IAsyncDisposable
{
    private readonly List<T> _buffer;
    private readonly IReadOnlyList<string> _columnNames;
    private readonly RedshiftConfiguration _config;
    private readonly IRedshiftConnectionPool _connectionPool;
    private readonly bool _disposeS3Client;
    private readonly RedshiftExceptionHandler _exceptionHandler;
    private readonly string _pipelineId;
    private readonly IAmazonS3 _s3Client;
    private readonly string _schema;
    private readonly string _table;
    private readonly Func<T, object?[]> _valueExtractor;
    private bool _disposed;
    private int _fileCounter;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftCopyFromS3Writer{T}" /> class.
    /// </summary>
    /// <param name="connectionPool">The connection pool to use for database connections.</param>
    /// <param name="schema">The schema name of the target table.</param>
    /// <param name="table">The table name to write to.</param>
    /// <param name="config">The Redshift configuration.</param>
    /// <param name="s3Client">The S3 client for uploading staging files.</param>
    /// <param name="disposeS3Client">Whether to dispose the S3 client when writer is disposed.</param>
    public RedshiftCopyFromS3Writer(
        IRedshiftConnectionPool connectionPool,
        string schema,
        string table,
        RedshiftConfiguration config,
        IAmazonS3 s3Client,
        bool disposeS3Client = false)
    {
        _connectionPool = connectionPool ?? throw new ArgumentNullException(nameof(connectionPool));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _config = config ?? throw new ArgumentNullException(nameof(config));
        _s3Client = s3Client ?? throw new ArgumentNullException(nameof(s3Client));
        _disposeS3Client = disposeS3Client;

        _valueExtractor = RedshiftParameterMapper.BuildValueExtractor<T>(config.NamingConvention);
        _columnNames = RedshiftParameterMapper.GetColumnNames<T>(config.NamingConvention);
        _exceptionHandler = new RedshiftExceptionHandler(config.MaxRetryAttempts, config.RetryDelay);
        _buffer = new List<T>(config.BatchSize);
        _pipelineId = Guid.NewGuid().ToString("N")[..8];
        _fileCounter = 0;
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
            await FlushAsync().ConfigureAwait(false);
        }
        catch
        {
            // Swallow flush errors on dispose
        }

        _disposed = true;
        _buffer.Clear();

        if (_disposeS3Client)
            _s3Client.Dispose();
    }

    /// <summary>
    ///     Buffers a row for batch writing. Flushes automatically when batch size is reached.
    /// </summary>
    /// <param name="row">The row to write.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task WriteAsync(T row, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (row is null)
            throw new ArgumentNullException(nameof(row));

        _buffer.Add(row);

        if (_buffer.Count >= _config.BatchSize)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Flushes all buffered rows to Redshift via S3.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_buffer.Count == 0)
            return;

        var rows = _buffer.ToArray();
        _buffer.Clear();

        await FlushBatchAsync(rows, cancellationToken).ConfigureAwait(false);
    }

    private async Task FlushBatchAsync(T[] rows, CancellationToken cancellationToken)
    {
        var s3Key = $"{_config.S3KeyPrefix}{_pipelineId}/{DateTime.UtcNow:yyyyMMddHHmmss}_{Interlocked.Increment(ref _fileCounter):D6}.csv.gz";

        // 1. Serialize to CSV and upload to S3
        byte[] csvData;

        using (var memoryStream = new MemoryStream())
        {
            using (var gzipStream = new GZipStream(memoryStream, CompressionLevel.Optimal, true))
            using (var writer = new StreamWriter(gzipStream, Encoding.UTF8))
            {
                WriteCsv(writer, rows);
            }

            csvData = memoryStream.ToArray();
        }

        // Retry loop for S3 upload
        await _exceptionHandler.ExecuteWithRetryAsync(async () =>
        {
            using var uploadStream = new MemoryStream(csvData);

            var putRequest = new PutObjectRequest
            {
                BucketName = _config.S3BucketName,
                Key = s3Key,
                InputStream = uploadStream,
                ContentType = "application/gzip",
            };

            putRequest.Metadata.Add("x-amz-meta-pipeline-id", _pipelineId);

            await _s3Client.PutObjectAsync(putRequest, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);

        // 2. Issue COPY command
        try
        {
            await ExecuteCopyAsync(s3Key, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // 3. Optionally purge S3 file
            if (_config.PurgeS3FilesAfterCopy)
            {
                try
                {
                    await _s3Client.DeleteObjectAsync(_config.S3BucketName, s3Key, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    // Swallow deletion errors - not critical
                }
            }
        }
    }

    private void WriteCsv(StreamWriter writer, T[] rows)
    {
        if (rows.Length == 0)
            return;

        // Write header
        writer.WriteLine(string.Join(",", _columnNames.Select(EscapeCsvValue)));

        // Write data rows
        foreach (var row in rows)
        {
            var values = _valueExtractor(row);
            var csvValues = values.Select(FormatCsvValue);
            writer.WriteLine(string.Join(",", csvValues));
        }
    }

    private static string FormatCsvValue(object? value)
    {
        return value switch
        {
            null => "",
            DBNull => "",
            string s => EscapeCsvValue(s),
            DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture), // ISO 8601
            DateTimeOffset dto => dto.ToString("O", CultureInfo.InvariantCulture),
            Guid g => g.ToString(),
            decimal d => d.ToString(CultureInfo.InvariantCulture),
            double db => db.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString(CultureInfo.InvariantCulture),
            bool b => b
                ? "true"
                : "false",
            _ => EscapeCsvValue(value.ToString() ?? ""),
        };
    }

    private static string EscapeCsvValue(string value)
    {
        if (string.IsNullOrEmpty(value))
            return "";

        // If contains comma, newline, or quote, wrap in quotes and escape quotes
        if (value.Contains(',') || value.Contains('\n') || value.Contains('\r') || value.Contains('"'))
            return $"\"{value.Replace("\"", "\"\"")}\"";

        return value;
    }

    private async Task ExecuteCopyAsync(string s3Key, CancellationToken cancellationToken)
    {
        var s3Uri = $"s3://{_config.S3BucketName}/{s3Key}";

        if (_config.UseUpsert)
            await ExecuteUpsertCopyAsync(s3Uri, cancellationToken).ConfigureAwait(false);
        else
            await ExecuteDirectCopyAsync(s3Uri, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteDirectCopyAsync(string s3Uri, CancellationToken cancellationToken)
    {
        var copySql = BuildCopyCommand(s3Uri, $"\"{_schema}\".\"{_table}\"");

        await _exceptionHandler.ExecuteWithRetryAsync(async () =>
        {
            await using var connection = await _connectionPool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = copySql;
            command.CommandTimeout = _config.CommandTimeout;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteUpsertCopyAsync(string s3Uri, CancellationToken cancellationToken)
    {
        var stagingTable = _config.UseTempStagingTable
            ? $"\"{_config.StagingTablePrefix}{_table}_{_pipelineId}\""
            : $"\"{_config.StagingSchema ?? _schema}\".\"{_config.StagingTablePrefix}{_table}_{_pipelineId}\"";

        try
        {
            // 1. Create staging table
            await CreateStagingTableAsync(stagingTable, cancellationToken).ConfigureAwait(false);

            // 2. COPY into staging table
            var copySql = BuildCopyCommand(s3Uri, stagingTable);

            await _exceptionHandler.ExecuteWithRetryAsync(async () =>
            {
                await using var connection = await _connectionPool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
                await using var command = connection.CreateCommand();
                command.CommandText = copySql;
                command.CommandTimeout = _config.CommandTimeout;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);

            // 3. Execute upsert (DELETE + INSERT)
            await ExecuteUpsertMergeAsync(stagingTable, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // 4. Drop staging table
            await DropStagingTableAsync(stagingTable, cancellationToken).ConfigureAwait(false);
        }
    }

    private string BuildCopyCommand(string s3Uri, string targetTable)
    {
        var sb = new StringBuilder();
        sb.Append($"COPY {targetTable}");
        sb.Append($" FROM '{s3Uri}'");
        sb.Append($" IAM_ROLE '{_config.IamRoleArn}'");
        sb.Append($" FORMAT AS {_config.CopyFileFormat}");
        sb.Append($" {_config.CopyCompression}");
        sb.Append(" IGNOREHEADER 1");
        sb.Append(" EMPTYASNULL");
        sb.Append(" BLANKSASNULL");
        sb.Append(" TIMEFORMAT 'auto'");
        sb.Append(" DATEFORMAT 'auto'");
        sb.Append($" ON_ERROR {_config.CopyOnErrorAction}");

        return sb.ToString();
    }

    private async Task CreateStagingTableAsync(string stagingTable, CancellationToken cancellationToken)
    {
        var createSql = _config.UseTempStagingTable
            ? $"CREATE TEMP TABLE {stagingTable} (LIKE \"{_schema}\".\"{_table}\")"
            : $"CREATE TABLE {stagingTable} (LIKE \"{_schema}\".\"{_table}\")";

        await using var connection = await _connectionPool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = createSql;
        command.CommandTimeout = _config.CommandTimeout;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteUpsertMergeAsync(string stagingTable, CancellationToken cancellationToken)
    {
        var keyColumns = _config.UpsertKeyColumns ?? throw new RedshiftException("UpsertKeyColumns must be set when UseUpsert is true");
        var keyJoin = string.Join(" AND ", keyColumns.Select(k => $"target.\"{k}\" = stage.\"{k}\""));

        await using var connection = await _connectionPool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);

        if (_config.UseTransaction)
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                if (_config.OnMergeAction == OnMergeAction.Update)
                {
                    // DELETE matching rows from target
                    var deleteSql = $"DELETE FROM \"{_schema}\".\"{_table}\" AS target USING {stagingTable} AS stage WHERE {keyJoin}";

                    await using (var command = connection.CreateCommand())
                    {
                        command.CommandText = deleteSql;
                        command.CommandTimeout = _config.CommandTimeout;
                        command.Transaction = transaction;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                }

                // INSERT from staging
                var insertSql = $"INSERT INTO \"{_schema}\".\"{_table}\" SELECT * FROM {stagingTable}";

                await using (var command = connection.CreateCommand())
                {
                    command.CommandText = insertSql;
                    command.CommandTimeout = _config.CommandTimeout;
                    command.Transaction = transaction;
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }
        else
        {
            if (_config.OnMergeAction == OnMergeAction.Update)
            {
                var deleteSql = $"DELETE FROM \"{_schema}\".\"{_table}\" AS target USING {stagingTable} AS stage WHERE {keyJoin}";
                await using var deleteCommand = connection.CreateCommand();
                deleteCommand.CommandText = deleteSql;
                deleteCommand.CommandTimeout = _config.CommandTimeout;
                await deleteCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var insertSql = $"INSERT INTO \"{_schema}\".\"{_table}\" SELECT * FROM {stagingTable}";
            await using var insertCommand = connection.CreateCommand();
            insertCommand.CommandText = insertSql;
            insertCommand.CommandTimeout = _config.CommandTimeout;
            await insertCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task DropStagingTableAsync(string stagingTable, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _connectionPool.GetConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS {stagingTable}";
            command.CommandTimeout = _config.CommandTimeout;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Swallow errors on cleanup - temp tables auto-drop on session close
        }
    }
}
