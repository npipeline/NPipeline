using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Sql;

/// <summary>How a SQL sink uses transactions.</summary>
public enum SqlTransactionMode
{
    /// <summary>
    ///     Each batch of <see cref="SqlSinkOptions.BatchSize" /> rows is written in its own transaction, so a batch lands
    ///     whole or not at all, and a transient failure can be retried. The default.
    /// </summary>
    PerBatch,

    /// <summary>
    ///     No transaction of the sink's own; each statement commits on its own. A batch split into several statements can
    ///     land partly, so failed batches are not retried.
    /// </summary>
    None,

    /// <summary>The whole write is one transaction, committed after the last row, and rolled back if anything fails.</summary>
    WholeRun,
}

/// <summary>What a SQL sink does with a batch that fails to write.</summary>
public enum FailedBatchAction
{
    /// <summary>Fail the write.</summary>
    Fail,

    /// <summary>
    ///     Send a <see cref="SqlBatchFailure{T}" /> holding the batch's items to the pipeline's dead-letter sink and continue.
    ///     The write fails with <c>DeadLetterSinkNotConfiguredException</c> when the pipeline has no dead-letter sink. Not
    ///     allowed with <see cref="SqlTransactionMode.WholeRun" />, whose transaction cannot continue after a failure.
    /// </summary>
    DeadLetter,
}

/// <summary>A batch a SQL sink could not write, as sent to the dead-letter sink.</summary>
/// <param name="Table">The table written to.</param>
/// <param name="Items">The batch's items.</param>
/// <typeparam name="T">The record type.</typeparam>
public sealed record SqlBatchFailure<T>(string Table, IReadOnlyList<T> Items);

/// <summary>What an upsert does with a row whose key already exists.</summary>
public enum SqlUpsertAction
{
    /// <summary>Update the existing row's other columns.</summary>
    Update,

    /// <summary>Keep the existing row.</summary>
    Ignore,
}

/// <summary>Writes rows as an upsert on <paramref name="Keys" /> instead of a plain insert.</summary>
/// <param name="Keys">The key columns, as named in the table (after the naming policy).</param>
/// <param name="OnMatch">What to do with a row whose key exists.</param>
public sealed record SqlUpsert(IReadOnlyList<string> Keys, SqlUpsertAction OnMatch = SqlUpsertAction.Update)
{
    /// <summary>An upsert that updates on <paramref name="keys" />.</summary>
    public static SqlUpsert On(params string[] keys) => new(keys);
}

/// <summary>Options shared by SQL sources and sinks. Each connector derives its own options records from the two below.</summary>
public abstract record SqlNodeOptions
{
    /// <summary>The default command timeout, 30 seconds.</summary>
    public const int DefaultCommandTimeout = 30;

    /// <summary>The connection string. One of this, <see cref="Uri" /> or the connector's connection pool is required.</summary>
    public string? ConnectionString { get; init; }

    /// <summary>A storage URI naming the database, resolved through <see cref="Provider" /> or <see cref="Resolver" />.</summary>
    public StorageUri? Uri { get; init; }

    /// <summary>The database storage provider for <see cref="Uri" />.</summary>
    public IStorageProvider? Provider { get; init; }

    /// <summary>The resolver for <see cref="Uri" /> when <see cref="Provider" /> is <c>null</c>.</summary>
    public IStorageResolver? Resolver { get; init; }

    /// <summary>The command timeout in seconds. Defaults to 30; 0 waits indefinitely.</summary>
    public int CommandTimeout { get; init; } = DefaultCommandTimeout;

    /// <summary>How member names become column names. Defaults to the connector's convention.</summary>
    public ColumnNamingPolicy Naming { get; init; } = ColumnNamingPolicy.AsIs;

    /// <summary>Whether the connector supplies the connection itself (a connection pool, or DuckDB's database path), for <see cref="Validate" />.</summary>
    protected virtual bool HasConnectorConnection => false;

    /// <summary>Throws if the options are inconsistent.</summary>
    public virtual void Validate()
    {
        var connections = (ConnectionString is not null ? 1 : 0) + (Uri is not null ? 1 : 0) + (HasConnectorConnection ? 1 : 0);

        if (connections != 1)
            throw new ArgumentException("Set exactly one connection: ConnectionString, Uri or the connector's own (such as a connection pool).", nameof(ConnectionString));

        ArgumentOutOfRangeException.ThrowIfNegative(CommandTimeout, nameof(CommandTimeout));
        ArgumentNullException.ThrowIfNull(Naming, nameof(Naming));
    }
}

/// <summary>Options every SQL source has.</summary>
public abstract record SqlSourceOptions : SqlNodeOptions
{
    /// <summary>The query.</summary>
    public required string Query { get; init; }

    /// <summary>The query's parameters, bound by name.</summary>
    public IReadOnlyList<DatabaseParameter> Parameters { get; init; } = [];

    /// <summary>What a member without a column means. Defaults to <see cref="MissingColumnBehavior.ThrowForRequired" />.</summary>
    public MissingColumnBehavior MissingColumns { get; init; } = MissingColumnBehavior.ThrowForRequired;

    /// <summary>Decides what happens to a row that fails to map. When <c>null</c>, the read fails.</summary>
    public RowErrorHandler? RowErrorHandler { get; init; }

    /// <summary>The most characters of a failed row's values kept in <see cref="RowError.RawExcerpt" />; 0 keeps none.</summary>
    public int RawExcerptLength { get; init; } = FileSourceOptions.DefaultRawExcerptLength;

    /// <summary>
    ///     Checkpointing: <see cref="CheckpointStrategy.None" /> (the default), <see cref="CheckpointStrategy.InMemory" />, or
    ///     <see cref="CheckpointStrategy.Offset" />, which records how many rows were read and skips that many when the read
    ///     restarts. The query needs a stable <c>ORDER BY</c> for an offset to mean the same rows.
    /// </summary>
    public CheckpointStrategy CheckpointStrategy { get; init; } = CheckpointStrategy.None;

    /// <summary>Where checkpoints are stored. Required for <see cref="CheckpointStrategy.Offset" />.</summary>
    public ICheckpointStorage? CheckpointStorage { get; init; }

    /// <summary>How often a checkpoint is saved.</summary>
    public CheckpointIntervalConfiguration CheckpointInterval { get; init; } = new();

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(Query, nameof(Query));
        ArgumentNullException.ThrowIfNull(Parameters, nameof(Parameters));
        ArgumentOutOfRangeException.ThrowIfNegative(RawExcerptLength, nameof(RawExcerptLength));

        switch (CheckpointStrategy)
        {
            case CheckpointStrategy.None or CheckpointStrategy.InMemory:
                break;
            case CheckpointStrategy.Offset when CheckpointStorage is null:
                throw new ArgumentException("CheckpointStrategy.Offset needs a CheckpointStorage.", nameof(CheckpointStorage));
            case CheckpointStrategy.Offset:
                break;
            default:
                throw new NotSupportedException(
                    $"SQL sources support CheckpointStrategy None, InMemory and Offset, not {CheckpointStrategy}.");
        }
    }
}

/// <summary>Options every SQL sink has.</summary>
public abstract record SqlSinkOptions : SqlNodeOptions
{
    /// <summary>The default batch size, 1,000 rows.</summary>
    public const int DefaultBatchSize = 1_000;

    /// <summary>The table to write.</summary>
    public required string Table { get; init; }

    /// <summary>The table's schema; <c>null</c> uses the connection's default.</summary>
    public string? Schema { get; init; }

    /// <summary>Rows written per batch. Batch statements are split further to stay under the database's parameter limit.</summary>
    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>
    ///     The longest a batch waits to fill: a partial batch is written once this long has passed since its first row, so
    ///     a slow stream (from a message queue, say) is written promptly. Defaults to one second;
    ///     <see cref="Timeout.InfiniteTimeSpan" /> waits for full batches.
    /// </summary>
    public TimeSpan BatchLinger { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>How the sink uses transactions. Defaults to <see cref="SqlTransactionMode.PerBatch" />.</summary>
    public SqlTransactionMode Transaction { get; init; } = SqlTransactionMode.PerBatch;

    /// <summary>What happens to a batch that fails to write. Defaults to <see cref="FailedBatchAction.Fail" />.</summary>
    public FailedBatchAction FailedBatches { get; init; } = FailedBatchAction.Fail;

    /// <summary>Writes an upsert on these keys instead of a plain insert.</summary>
    public SqlUpsert? Upsert { get; init; }

    /// <summary>
    ///     Whether table, schema and key names must be plain identifiers (letters, digits and underscores). Defaults to
    ///     <c>true</c>. Every identifier is quoted and escaped either way.
    /// </summary>
    public bool ValidateIdentifiers { get; init; } = true;

    /// <inheritdoc />
    public override void Validate()
    {
        base.Validate();
        ArgumentException.ThrowIfNullOrWhiteSpace(Table, nameof(Table));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(BatchSize, nameof(BatchSize));

        if (BatchLinger < TimeSpan.Zero && BatchLinger != Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(BatchLinger), "BatchLinger must be zero or more, or Timeout.InfiniteTimeSpan.");

        if (FailedBatches == FailedBatchAction.DeadLetter && Transaction == SqlTransactionMode.WholeRun)
            throw new ArgumentException("FailedBatchAction.DeadLetter cannot be used with SqlTransactionMode.WholeRun.", nameof(FailedBatches));

        if (Upsert is { Keys.Count: 0 })
            throw new ArgumentException("An upsert needs at least one key column.", nameof(Upsert));
    }
}
