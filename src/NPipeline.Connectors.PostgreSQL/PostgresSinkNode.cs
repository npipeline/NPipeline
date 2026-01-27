using System.Buffers;
using System.Text;
using Npgsql;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Exceptions;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Delivery = NPipeline.Connectors.Configuration.DeliverySemantic;

namespace NPipeline.Connectors.PostgreSQL;

/// <summary>
/// Sink node that writes pipeline items to PostgreSQL using Npgsql.
/// </summary>
/// <typeparam name="T">Item type to persist.</typeparam>
public class PostgresSinkNode<T> : SinkNode<T>
{
    private static readonly string[] CachedColumns = PostgresParameterMapper.GetColumnNames<T>();
    private static readonly Action<NpgsqlParameterCollection, T> CachedParameterMapper = PostgresParameterMapper.Build<T>();
    private static readonly Action<NpgsqlBinaryImporter, T> CachedCopyMapper = PostgresParameterMapper.BuildCopyMapper<T>();
    private static readonly SearchValues<char> InvalidIdentifierChars = SearchValues.Create([';', '\'', '"', '\r', '\n', '\t']);

    private readonly NpgsqlDataSource? _dataSource;
    private readonly Action<NpgsqlParameterCollection, T> _parameterMapper;
    private readonly Action<NpgsqlBinaryImporter, T> _copyMapper;
    private readonly string[] _columns;
    private readonly string _insertSql;
    private readonly string _normalizedTableName;
    private readonly PostgresConfiguration _configuration;

    /// <summary>
    /// Gets the active configuration.
    /// </summary>
    public PostgresConfiguration Configuration => _configuration;

    /// <summary>
    /// Initializes a new instance targeting a connection string.
    /// </summary>
    public PostgresSinkNode(string tableName, PostgresConfiguration configuration)
    {
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();

        _normalizedTableName = NormalizeTableName(tableName);
        _columns = NormalizeColumns(CachedColumns);
        _parameterMapper = CachedParameterMapper;
        _copyMapper = CachedCopyMapper;
        _insertSql = BuildInsertSql();
    }

    /// <summary>
    /// Initializes a new instance using a shared NpgsqlDataSource.
    /// </summary>
    public PostgresSinkNode(NpgsqlDataSource dataSource, string tableName, PostgresConfiguration? configuration = null)
        : this(tableName, configuration ?? new PostgresConfiguration())
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    /// <inheritdoc />
    public override Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        return ExecuteWriteAsync(input.WithCancellation(cancellationToken));
    }

    /// <summary>
    /// Writes the provided items using the configured delivery semantics.
    /// </summary>
    public async Task ExecuteWriteAsync(IAsyncEnumerable<T> data)
    {
        var replayableData = await EnsureReplayableDataAsync(data).ConfigureAwait(false);

        switch (_configuration.DeliverySemantic)
        {
            case Delivery.AtLeastOnce:
                await ExecuteWriteAtLeastOnceAsync(replayableData).ConfigureAwait(false);
                break;
            case Delivery.AtMostOnce:
                await ExecuteWriteAtMostOnceAsync(replayableData).ConfigureAwait(false);
                break;
            case Delivery.ExactlyOnce:
                await ExecuteWriteExactlyOnceAsync(replayableData).ConfigureAwait(false);
                break;
            default:
                throw new NotSupportedException($"Delivery semantic {_configuration.DeliverySemantic} is not supported.");
        }
    }

    private async Task<IAsyncEnumerable<T>> EnsureReplayableDataAsync(IAsyncEnumerable<T> data)
    {
        if (_configuration.DeliverySemantic == Delivery.AtLeastOnce && _configuration.MaxRetryAttempts > 1)
        {
            var buffer = new List<T>();
            await foreach (var item in data.ConfigureAwait(false))
            {
                buffer.Add(item);
            }

            return Replay(buffer);
        }

        return data;

        static async IAsyncEnumerable<T> Replay(IReadOnlyList<T> bufferedItems)
        {
            foreach (var item in bufferedItems)
            {
                yield return item;
            }

            await Task.CompletedTask;
        }
    }

    private async Task ExecuteWriteAtLeastOnceAsync(IAsyncEnumerable<T> data)
    {
        var attempts = 0;
        var maxAttempts = Math.Max(1, _configuration.MaxRetryAttempts);
        var retryDelay = _configuration.RetryDelay <= TimeSpan.Zero ? TimeSpan.FromSeconds(1) : _configuration.RetryDelay;

        while (true)
        {
            attempts++;

            try
            {
                await ExecuteWriteInternalAsync(data).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempts < maxAttempts && _configuration.UseTransaction && PostgresExceptionHandler.IsTransient(ex))
            {
                await Task.Delay(retryDelay).ConfigureAwait(false);
            }
        }
    }

    private async Task ExecuteWriteAtMostOnceAsync(IAsyncEnumerable<T> data)
    {
        try
        {
            await ExecuteWriteInternalAsync(data).ConfigureAwait(false);
        }
        catch
        {
            // AtMostOnce: swallow the error and don't retry
        }
    }

    private async Task ExecuteWriteExactlyOnceAsync(IAsyncEnumerable<T> data)
    {
        if (!_configuration.UseUpsert)
        {
            throw new InvalidOperationException("ExactlyOnce delivery semantics requires UseUpsert to be enabled with conflict columns.");
        }

        var dataSource = _dataSource;
        var ownsDataSource = false;

        if (dataSource == null)
        {
            dataSource = CreateDataSource();
            ownsDataSource = true;
        }

        try
        {
            await using var connection = await OpenConnectionAsync(dataSource).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);

            try
            {
                await ExecuteWriteWithTransactionAsync(connection, transaction, data).ConfigureAwait(false);
                await transaction.CommitAsync().ConfigureAwait(false);
            }
            catch
            {
                await transaction.RollbackAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            if (ownsDataSource && dataSource != null)
            {
                await dataSource.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    protected virtual async Task ExecuteWriteInternalAsync(IAsyncEnumerable<T> data)
    {
        var dataSource = _dataSource;
        var ownsDataSource = false;

        if (dataSource == null)
        {
            dataSource = CreateDataSource();
            ownsDataSource = true;
        }

        try
        {
            await using var connection = await OpenConnectionAsync(dataSource).ConfigureAwait(false);
            NpgsqlTransaction? transaction = null;

            if (_configuration.UseTransaction)
            {
                transaction = await connection.BeginTransactionAsync().ConfigureAwait(false);
            }

            try
            {
                await ExecuteWriteWithTransactionAsync(connection, transaction, data).ConfigureAwait(false);

                if (transaction != null)
                {
                    await transaction.CommitAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                if (transaction != null)
                {
                    try
                    {
                        await transaction.RollbackAsync().ConfigureAwait(false);
                    }
                    catch
                    {
                        // ignore rollback failures
                    }
                }

                throw;
            }
        }
        finally
        {
            if (ownsDataSource && dataSource != null)
            {
                await dataSource.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    protected virtual async Task ExecuteWriteWithTransactionAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, IAsyncEnumerable<T> data)
    {
        switch (_configuration.WriteStrategy)
        {
            case PostgresWriteStrategy.Batch:
                await ProcessBatchAsync(connection, transaction, data).ConfigureAwait(false);
                break;
            case PostgresWriteStrategy.Copy:
                await ProcessCopyAsync(connection, data).ConfigureAwait(false);
                break;
            case PostgresWriteStrategy.PerRow:
                await ProcessIndividualAsync(connection, transaction, data).ConfigureAwait(false);
                break;
            default:
                throw new NotSupportedException($"Write strategy {_configuration.WriteStrategy} is not supported.");
        }
    }

    private string BuildInsertSql()
    {
        var sb = new StringBuilder();
        sb.Append("INSERT INTO ");
        sb.Append(_normalizedTableName);
        sb.Append(" (");
        sb.Append(string.Join(", ", _columns));
        sb.Append(") VALUES (");
        sb.Append(string.Join(", ", _columns.Select(c => "@" + c)));
        sb.Append(')');

        var conflictColumns = NormalizeConflictColumns();
        if (_configuration.UseUpsert && conflictColumns.Length > 0)
        {
            sb.Append(" ON CONFLICT (");
            sb.Append(string.Join(", ", conflictColumns));
            sb.Append(')');

            if (_configuration.OnConflictAction == OnConflictAction.Update)
            {
                sb.Append(" DO UPDATE SET ");
                var updates = _columns
                    .Where(c => !conflictColumns.Contains(c, StringComparer.OrdinalIgnoreCase))
                    .Select(c => $"{c} = EXCLUDED.{c}");
                sb.Append(string.Join(", ", updates));
            }
            else
            {
                sb.Append(" DO NOTHING");
            }
        }

        return sb.ToString();
    }

    protected virtual async Task<NpgsqlConnection> OpenConnectionAsync(NpgsqlDataSource dataSource)
    {
        try
        {
            return await dataSource.OpenConnectionAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw PostgresExceptionHandler.Translate("Failed to open PostgreSQL connection.", ex);
        }
    }

    private async Task ProcessBatchAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, IAsyncEnumerable<T> data)
    {
        var batchSize = ClampBatchSize(_configuration.BatchSize, _configuration.MaxBatchSize);
        var currentBatch = new List<T>(batchSize);

        await foreach (var item in data.ConfigureAwait(false))
        {
            currentBatch.Add(item);

            if (currentBatch.Count >= batchSize)
            {
                await ExecuteBatchAsync(connection, transaction, currentBatch).ConfigureAwait(false);
                currentBatch.Clear();
            }
        }

        if (currentBatch.Count > 0)
        {
            await ExecuteBatchAsync(connection, transaction, currentBatch).ConfigureAwait(false);
        }
    }

    private async Task ExecuteBatchAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, List<T> batch)
    {
        var batchCommand = connection.CreateBatch();
        batchCommand.Transaction = transaction;

        foreach (var item in batch)
        {
            var command = batchCommand.CreateBatchCommand();
            command.CommandText = _insertSql;
            _parameterMapper(command.Parameters, item);
            batchCommand.BatchCommands.Add(command);
        }

        try
        {
            await batchCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
        catch (NpgsqlException ex)
        {
            var pgEx = PostgresExceptionHandler.Translate("Error executing PostgreSQL batch write.", ex);
            if (_configuration.ContinueOnError || _configuration.RowErrorHandler?.Invoke(pgEx, null) == true)
            {
                return;
            }

            throw pgEx;
        }
        finally
        {
            await batchCommand.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async Task ProcessCopyAsync(NpgsqlConnection connection, IAsyncEnumerable<T> data)
    {
        if (_configuration.UseUpsert)
        {
            throw new NotSupportedException("Upsert is not supported with the COPY write strategy. Use Batch or PerRow strategy instead.");
        }

        if (!_configuration.UseBinaryCopy)
        {
            throw new NotSupportedException("The Copy write strategy currently requires UseBinaryCopy to be enabled.");
        }

        var copySql = $"COPY {_normalizedTableName} ({string.Join(", ", _columns)}) FROM STDIN (FORMAT BINARY)";

        try
        {
            await using var writer = await connection.BeginBinaryImportAsync(copySql).ConfigureAwait(false);
            writer.Timeout = TimeSpan.FromSeconds(_configuration.CopyTimeout);

            await foreach (var item in data.ConfigureAwait(false))
            {
                try
                {
                    await writer.StartRowAsync().ConfigureAwait(false);
                    _copyMapper(writer, item);
                }
                catch (Exception ex)
                {
                    var pgEx = PostgresExceptionHandler.Translate("Error writing row to PostgreSQL COPY stream.", ex);
                    if (_configuration.ContinueOnError || _configuration.RowErrorHandler?.Invoke(pgEx, null) == true)
                    {
                        continue;
                    }

                    throw pgEx;
                }
            }

            await writer.CompleteAsync().ConfigureAwait(false);
        }
        catch (NpgsqlException ex)
        {
            throw PostgresExceptionHandler.Translate("Failed to complete PostgreSQL COPY operation.", ex);
        }
    }

    private async Task ProcessIndividualAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, IAsyncEnumerable<T> data)
    {
        await foreach (var item in data.ConfigureAwait(false))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = _insertSql;
            command.Transaction = transaction;
            command.CommandTimeout = _configuration.CommandTimeout;
            _parameterMapper(command.Parameters, item);

            try
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch (NpgsqlException ex)
            {
                var pgEx = PostgresExceptionHandler.Translate("Error executing PostgreSQL single row write.", ex);
                if (_configuration.ContinueOnError || _configuration.RowErrorHandler?.Invoke(pgEx, null) == true)
                {
                    continue;
                }

                throw pgEx;
            }
        }
    }

    private string[] NormalizeColumns(string[] columns)
    {
        var normalized = new string[columns.Length];
        for (var i = 0; i < columns.Length; i++)
        {
            normalized[i] = NormalizeIdentifier(columns[i], $"columns[{i}]");
        }

        return normalized;
    }

    private string[] NormalizeConflictColumns()
    {
        if (!_configuration.UseUpsert)
        {
            return Array.Empty<string>();
        }

        var conflictColumns = _configuration.UpsertConflictColumns;
        ArgumentNullException.ThrowIfNull(conflictColumns);

        if (conflictColumns.Length == 0)
        {
            throw new ArgumentException("UseUpsert is enabled but no UpsertConflictColumns were provided.", nameof(_configuration.UpsertConflictColumns));
        }

        return conflictColumns.Select((c, i) => NormalizeIdentifier(c, $"UpsertConflictColumns[{i}]"))
            .ToArray();
    }

    private string NormalizeTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name cannot be null, empty, or whitespace.", nameof(tableName));
        }

        var candidateName = tableName;

        if (!tableName.Contains('.', StringComparison.Ordinal) && !string.IsNullOrWhiteSpace(_configuration.Schema))
        {
            candidateName = $"{_configuration.Schema}.{tableName}";
        }

        if (!_configuration.ValidateIdentifiers)
        {
            return candidateName;
        }

        var parts = candidateName.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0)
        {
            throw new ArgumentException("Table name is invalid.", nameof(tableName));
        }

        var normalized = new string[parts.Length];
        for (var i = 0; i < parts.Length; i++)
        {
            normalized[i] = NormalizeIdentifier(parts[i], $"tableName[{i}]");
        }

        return string.Join('.', normalized);
    }

    private string NormalizeIdentifier(string identifier, string paramName)
    {
        if (!_configuration.ValidateIdentifiers)
        {
            return identifier;
        }

        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("Identifier cannot be null, empty, or whitespace.", paramName);
        }

        var trimmed = identifier.Trim();
        if (trimmed.AsSpan().IndexOfAny(InvalidIdentifierChars) != -1 || trimmed.Contains("--", StringComparison.Ordinal) || trimmed.Contains("/*", StringComparison.Ordinal))
        {
            throw new ArgumentException("Identifier contains invalid characters.", paramName);
        }

        return trimmed;
    }

    protected virtual NpgsqlDataSource CreateDataSource()
    {
        if (string.IsNullOrWhiteSpace(_configuration.ConnectionString))
        {
            throw new PostgresConnectionException("Connection string is required when no data source is provided.");
        }

        try
        {
            var builder = new NpgsqlConnectionStringBuilder(_configuration.ConnectionString)
            {
                CommandTimeout = _configuration.CommandTimeout,
                Timeout = _configuration.ConnectionTimeout,
                MinPoolSize = _configuration.MinPoolSize,
                MaxPoolSize = _configuration.MaxPoolSize,
                ReadBufferSize = _configuration.ReadBufferSize
            };

            if (_configuration.UseSslMode && _configuration.SslMode.HasValue)
            {
                builder.SslMode = _configuration.SslMode.Value;
            }
            else if (_configuration.SslMode.HasValue)
            {
                builder.SslMode = _configuration.SslMode.Value;
            }

            return NpgsqlDataSource.Create(builder.ConnectionString);
        }
        catch (Exception ex)
        {
            throw new PostgresConnectionException("Failed to create PostgreSQL data source.", ex);
        }
    }

    private static int ClampBatchSize(int batchSize, int maxBatchSize)
    {
        return Math.Clamp(batchSize <= 0 ? 1 : batchSize, 1, maxBatchSize <= 0 ? int.MaxValue : maxBatchSize);
    }
}
