using System.Data.Common;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Mapping;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Sql;

/// <summary>
///     A sink that writes records to a table in batches of <see cref="SqlSinkOptions.BatchSize" />, or smaller after
///     <see cref="SqlSinkOptions.BatchLinger" />. It reports each batch once it is committed (<see cref="IReportsWrites" />),
///     so messages written through <c>Acknowledging()</c> are acknowledged then. Each record's readable
///     members become columns through a plan compiled once per type; the connector's writer (row by row, multi-row
///     statements or its bulk API) writes each batch, inside the transaction <see cref="SqlSinkOptions.Transaction" />
///     asks for.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public abstract class SqlSinkNode<T> : SinkNode<T>, IReportsWrites
{
    private readonly SqlSinkOptions _options;
    private Func<long, CancellationToken, ValueTask>? _written;

    /// <summary>Creates the sink and validates <paramref name="options" />.</summary>
    /// <param name="options">The sink's options.</param>
    /// <param name="dialect">The database's dialect.</param>
    /// <param name="shape">How members map to columns: the connector's attributes and <paramref name="options" />' naming policy.</param>
    protected SqlSinkNode(SqlSinkOptions options, SqlDialect dialect, RecordShapeOptions shape)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(dialect);
        ArgumentNullException.ThrowIfNull(shape);
        options.Validate();

        SqlShape.ThrowIfUnsupported(RecordShape.For<T>(shape));

        if (options.ValidateIdentifiers)
        {
            dialect.Validate(options.Table, nameof(options.Table));

            if (options.Schema is { Length: > 0 } schema)
                dialect.Validate(schema, nameof(options.Schema));
        }

        _options = options;
        Target = new SqlWriteTarget<T>(dialect, options, SqlWritePlan<T>.For(shape));

        if (Target.Plan.ColumnNames.Count == 0)
            throw new InvalidOperationException($"{typeof(T).Name} has no readable members to write.");
    }

    /// <summary>The table, columns and plan the sink writes.</summary>
    protected SqlWriteTarget<T> Target { get; }

    /// <summary>The connector's name in metrics and messages.</summary>
    protected string ConnectorName => Target.Dialect.Name;

    /// <summary>Creates an unopened connection for <paramref name="connectionString" />.</summary>
    protected abstract DbConnection CreateConnection(string connectionString);

    /// <summary>The resolver used for a URI when the options name neither a provider nor a resolver.</summary>
    protected abstract IStorageResolver DefaultResolver { get; }

    /// <summary>
    ///     Opens a connection for the options' connection string or URI. Connectors with a connection pool override this to
    ///     take one from the pool when the options name it.
    /// </summary>
    protected virtual Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken) =>
        SqlConnections.OpenAsync(_options, CreateConnection, () => DefaultResolver, cancellationToken);

    /// <summary>The writer for the options' write strategy.</summary>
    protected abstract SqlWriter<T> CreateWriter();

    /// <summary>
    ///     Runs one batch's write in its own transaction, retrying it if the connector retries transient failures. Only
    ///     called for <see cref="SqlTransactionMode.PerBatch" />, where a failed attempt left nothing behind. The default
    ///     runs it once.
    /// </summary>
    protected virtual Task ExecuteAsync(DbConnection connection, Func<CancellationToken, Task> write, CancellationToken cancellationToken) => write(cancellationToken);

    /// <summary>Runs once the connection is open, before anything is written: DuckDB creates or empties its table here.</summary>
    protected virtual Task PrepareAsync(DbConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Runs after every batch is written and committed: DuckDB exports its table to a file here.</summary>
    protected virtual Task CompleteAsync(DbConnection connection, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public void ReportWritesTo(Func<long, CancellationToken, ValueTask> written) => _written = written ?? throw new ArgumentNullException(nameof(written));

    /// <inheritdoc />
    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);

        var deadLetters = OpenDeadLetterChannel(context);
        var writer = CreateWriter();
        var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        long written = 0;

        await using (connection.ConfigureAwait(false))
        {
            await PrepareAsync(connection, cancellationToken).ConfigureAwait(false);

            if (Target.Dialect.NeedsColumnTypes)
                writer.DatabaseTypes = await DescribeColumnsAsync(connection, cancellationToken).ConfigureAwait(false);

            DbTransaction? run = null;

            try
            {
                if (_options.Transaction == SqlTransactionMode.WholeRun)
                    run = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

                long handled = 0;

                await foreach (var items in input.BatchAsync(_options.BatchSize, _options.BatchLinger, cancellationToken).ConfigureAwait(false))
                {
                    var batch = items as IReadOnlyList<T> ?? [.. items];
                    written += await WriteBatchAsync(connection, run, writer, batch, deadLetters, cancellationToken).ConfigureAwait(false);
                    handled += batch.Count;

                    // A whole-run transaction makes nothing durable until it commits.
                    if (run is null && _written is not null)
                        await _written(handled, cancellationToken).ConfigureAwait(false);
                }

                if (run is not null)
                {
                    await run.CommitAsync(cancellationToken).ConfigureAwait(false);

                    if (_written is not null)
                        await _written(handled, cancellationToken).ConfigureAwait(false);
                }

                await CompleteAsync(connection, cancellationToken).ConfigureAwait(false);
            }
            catch when (run is not null)
            {
                await RollbackAsync(run).ConfigureAwait(false);
                throw;
            }
            finally
            {
                if (run is not null)
                    await run.DisposeAsync().ConfigureAwait(false);

                ConnectorDiagnostics.RecordRowsWritten(ConnectorName, ConnectorName, written);
            }
        }
    }

    /// <summary>The target table's column types, in plan order, from an empty query of the table.</summary>
    private async Task<IReadOnlyList<string?>> DescribeColumnsAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();

        await using (command.ConfigureAwait(false))
        {
            command.CommandText = $"SELECT {string.Join(", ", Target.QuotedColumns)} FROM {Target.QualifiedTable} WHERE 1 = 0";
            command.CommandTimeout = Target.CommandTimeout;

            var reader = await command.ExecuteReaderAsync(System.Data.CommandBehavior.SchemaOnly, cancellationToken).ConfigureAwait(false);

            await using (reader.ConfigureAwait(false))
            {
                return [.. Enumerable.Range(0, reader.FieldCount).Select(reader.GetDataTypeName)];
            }
        }
    }

    private async Task<long> WriteBatchAsync(DbConnection connection, DbTransaction? run, SqlWriter<T> writer, IReadOnlyList<T> batch, DeadLetterChannel deadLetters,
        CancellationToken cancellationToken)
    {
        try
        {
            if (run is not null)
                await writer.WriteAsync(connection, run, batch, cancellationToken).ConfigureAwait(false);
            else if (_options.Transaction == SqlTransactionMode.PerBatch)
                await ExecuteAsync(connection, ct => InBatchTransactionAsync(connection, writer, batch, ct), cancellationToken).ConfigureAwait(false);
            else
                await writer.WriteAsync(connection, null, batch, cancellationToken).ConfigureAwait(false);

            return batch.Count;
        }
        catch (Exception ex) when (_options.FailedBatches == FailedBatchAction.DeadLetter && ex is not OperationCanceledException)
        {
            await deadLetters.SendAsync(new SqlBatchFailure<T>(Target.Table, batch), ex, cancellationToken).ConfigureAwait(false);
            ConnectorDiagnostics.RecordRowError(ConnectorName, ConnectorName, "deadletter");
            return 0;
        }
    }

    private static async Task InBatchTransactionAsync(DbConnection connection, SqlWriter<T> writer, IReadOnlyList<T> batch, CancellationToken cancellationToken)
    {
        var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        await using (transaction.ConfigureAwait(false))
        {
            try
            {
                await writer.WriteAsync(connection, transaction, batch, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await RollbackAsync(transaction).ConfigureAwait(false);
                throw;
            }
        }
    }

    /// <summary>Rolls back without the caller's token, so a cancelled write still undoes what it wrote; a failed rollback does not hide the original error.</summary>
    private static async Task RollbackAsync(DbTransaction transaction)
    {
        try
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is DbException or InvalidOperationException)
        {
            // The connection may be broken, in which case the server discards the transaction anyway.
        }
    }
}
