using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.MySql.Mapping;

namespace NPipeline.Connectors.MySql.Configuration;

/// <summary>
///     Configuration settings for MySQL connector operations.
/// </summary>
public class MySqlConfiguration
{
    private const int DefaultBatchSize = 100;
    private const int DefaultMaxBatchSize = 1_000;
    private const int DefaultCommandTimeoutSeconds = 30;
    private const int DefaultConnectionTimeoutSeconds = 15;
    private const int DefaultBulkLoadTimeoutSeconds = 300;
    private const int DefaultMinPoolSize = 1;
    private const int DefaultMaxPoolSize = 100;
    private const int DefaultBulkLoadBatchSize = 5_000;
    private const int DefaultBulkLoadNotifyAfter = 1_000;
    private const int DefaultMaxRetryAttempts = 3;

    // Connection Settings

    /// <summary>
    ///     Gets or sets the MySQL connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the command timeout in seconds. Default is 30.
    /// </summary>
    public int CommandTimeout { get; set; } = DefaultCommandTimeoutSeconds;

    /// <summary>
    ///     Gets or sets the connection timeout in seconds. Default is 15.
    /// </summary>
    public int ConnectionTimeout { get; set; } = DefaultConnectionTimeoutSeconds;

    /// <summary>
    ///     Gets or sets the default database name.
    ///     MySQL databases are the namespace (no separate schema concept).
    /// </summary>
    public string? DefaultDatabase { get; set; }

    /// <summary>
    ///     Gets or sets the character set for the connection. Default is "utf8mb4".
    /// </summary>
    public string CharacterSet { get; set; } = "utf8mb4";

    /// <summary>
    ///     Gets or sets whether to allow user-defined variables in queries.
    /// </summary>
    public bool AllowUserVariables { get; set; }

    /// <summary>
    ///     Gets or sets whether to convert MySQL zero date values to <see cref="DateTime.MinValue" />.
    /// </summary>
    public bool ConvertZeroDateTime { get; set; } = true;

    // Pool Settings

    /// <summary>
    ///     Gets or sets the minimum pool size for connection pooling. Default is 1.
    /// </summary>
    public int MinPoolSize { get; set; } = DefaultMinPoolSize;

    /// <summary>
    ///     Gets or sets the maximum pool size for connection pooling. Default is 100.
    /// </summary>
    public int MaxPoolSize { get; set; } = DefaultMaxPoolSize;

    // Write Settings

    /// <summary>
    ///     Gets or sets the write strategy used by the sink. Default is <see cref="MySqlWriteStrategy.Batch" />.
    /// </summary>
    public MySqlWriteStrategy WriteStrategy { get; set; } = MySqlWriteStrategy.Batch;

    /// <summary>
    ///     Gets or sets the batch size for batched writes. Default is 100.
    /// </summary>
    public int BatchSize { get; set; } = DefaultBatchSize;

    /// <summary>
    ///     Gets or sets the maximum batch size to avoid unbounded memory usage. Default is 1,000.
    /// </summary>
    public int MaxBatchSize { get; set; } = DefaultMaxBatchSize;

    /// <summary>
    ///     Gets or sets whether to wrap writes in a transaction. Default is <c>true</c>.
    /// </summary>
    public bool UseTransaction { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to use prepared statements for repeated executions.
    /// </summary>
    public bool UsePreparedStatements { get; set; } = true;

    // Upsert Settings

    /// <summary>
    ///     Gets or sets whether to use upsert semantics for writes.
    /// </summary>
    public bool UseUpsert { get; set; }

    /// <summary>
    ///     Gets or sets the action to take when a duplicate key is encountered.
    ///     Used when <see cref="UseUpsert" /> is <c>true</c>.
    /// </summary>
    public OnDuplicateKeyAction OnDuplicateKeyAction { get; set; } = OnDuplicateKeyAction.Update;

    /// <summary>
    ///     Gets or sets the key columns for upsert matching.
    ///     Required when <see cref="UseUpsert" /> is <c>true</c> and
    ///     <see cref="OnDuplicateKeyAction" /> is <see cref="OnDuplicateKeyAction.Update" />.
    /// </summary>
    public string[] UpsertKeyColumns { get; set; } = [];

    // Bulk Load Settings

    /// <summary>
    ///     Gets or sets the number of rows per bulk load batch. Default is 5,000.
    /// </summary>
    public int BulkLoadBatchSize { get; set; } = DefaultBulkLoadBatchSize;

    /// <summary>
    ///     Gets or sets the bulk load notification interval (rows). Default is 1,000.
    /// </summary>
    public int BulkLoadNotifyAfter { get; set; } = DefaultBulkLoadNotifyAfter;

    /// <summary>
    ///     Gets or sets the bulk load timeout in seconds. Default is 300.
    /// </summary>
    public int BulkLoadTimeout { get; set; } = DefaultBulkLoadTimeoutSeconds;

    /// <summary>
    ///     Gets or sets whether to allow <c>LOAD DATA LOCAL INFILE</c>.
    ///     Must be enabled on both client and server for <see cref="MySqlWriteStrategy.BulkLoad" /> to work.
    /// </summary>
    public bool AllowLoadLocalInfile { get; set; }

    /// <summary>
    ///     Gets or sets the field terminator character for bulk load. Default is <c>','</c>.
    /// </summary>
    public char FieldTerminator { get; set; } = ',';

    /// <summary>
    ///     Gets or sets the line terminator character for bulk load. Default is <c>'\n'</c>.
    /// </summary>
    public char LineTerminator { get; set; } = '\n';

    /// <summary>
    ///     Gets or sets the escape character for bulk load. Default is <c>'\\'</c>.
    /// </summary>
    public char EscapeCharacter { get; set; } = '\\';

    // Read Settings

    /// <summary>
    ///     Gets or sets whether to stream results using <c>CommandBehavior.SequentialAccess</c>.
    ///     Keeps memory flat regardless of result-set size. Default is <c>true</c>.
    /// </summary>
    public bool StreamResults { get; set; } = true;

    // Error Handling

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts for transient errors. Default is 3.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = DefaultMaxRetryAttempts;

    /// <summary>
    ///     Gets or sets the delay between retry attempts. Default is 2 seconds.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    ///     Gets or sets whether to continue processing when a row-level error occurs.
    /// </summary>
    public bool ContinueOnError { get; set; }

    /// <summary>
    ///     Gets or sets a handler for row-level errors. Return <c>true</c> to swallow the exception.
    /// </summary>
    public Func<Exception, MySqlRow?, bool>? RowErrorHandler { get; set; }

    // Mapping Options

    /// <summary>
    ///     Gets or sets whether to throw on mapping errors. Default is <c>true</c>.
    /// </summary>
    public bool ThrowOnMappingError { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to validate identifiers to reduce injection risk. Default is <c>true</c>.
    /// </summary>
    public bool ValidateIdentifiers { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to perform case-insensitive column matching. Default is <c>true</c>.
    /// </summary>
    public bool CaseInsensitiveMapping { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to cache mapping metadata and compiled delegates. Default is <c>true</c>.
    /// </summary>
    public bool CacheMappingMetadata { get; set; } = true;

    // Delivery &amp; Checkpointing

    /// <summary>
    ///     Gets or sets the delivery semantic for retry behavior.
    /// </summary>
    public DeliverySemantic DeliverySemantic { get; set; } = DeliverySemantic.AtLeastOnce;

    /// <summary>
    ///     Gets or sets the checkpoint strategy. Default is <see cref="CheckpointStrategy.None" />.
    /// </summary>
    public CheckpointStrategy CheckpointStrategy { get; set; } = CheckpointStrategy.None;

    /// <summary>
    ///     Gets or sets the checkpoint storage backend.
    ///     Required when <see cref="CheckpointStrategy" /> is not None or InMemory.
    /// </summary>
    public ICheckpointStorage? CheckpointStorage { get; set; }

    /// <summary>
    ///     Gets or sets the checkpoint interval configuration.
    /// </summary>
    public CheckpointIntervalConfiguration CheckpointInterval { get; set; } = new();

    /// <summary>
    ///     Gets or sets the file path for file-based checkpoint storage.
    /// </summary>
    public string? CheckpointFilePath { get; set; }

    /// <summary>
    ///     Gets or sets the table name for database checkpoint storage. Default is "pipeline_checkpoints".
    /// </summary>
    public string CheckpointTableName { get; set; } = "pipeline_checkpoints";

    /// <summary>
    ///     Gets or sets the offset column name for offset-based checkpointing.
    /// </summary>
    public string? CheckpointOffsetColumn { get; set; }

    /// <summary>
    ///     Gets or sets the key columns for key-based checkpointing.
    /// </summary>
    public string[]? CheckpointKeyColumns { get; set; }

    /// <summary>
    ///     Gets or sets whether deterministic ordering is required for checkpointing.
    ///     When <c>true</c>, a missing ORDER BY clause in the query raises a warning or error.
    /// </summary>
    public bool RequireDeterministicOrderingForCheckpointing { get; set; } = true;

    // CDC (Binlog / GTID) Checkpointing

    /// <summary>
    ///     Gets or sets whether to enable CDC (binlog / GTID position) checkpointing.
    ///     Default is <c>false</c>.
    /// </summary>
    public bool EnableCdcCheckpointing { get; set; }

    /// <summary>
    ///     Gets or sets the CDC tracking mode (binlog file position or GTID set).
    /// </summary>
    public CdcMode CdcMode { get; set; } = CdcMode.BinlogFile;

    /// <summary>
    ///     Gets or sets whether to prefer GTID mode when both a binlog position and GTID are available.
    /// </summary>
    public bool PreferGtidWhenAvailable { get; set; } = true;

    /// <summary>
    ///     Gets or sets the initial binlog file name for CDC resumption (BinlogFile mode).
    /// </summary>
    public string? BinlogFileName { get; set; }

    /// <summary>
    ///     Gets or sets the initial binlog byte position for CDC resumption (BinlogFile mode).
    /// </summary>
    public ulong? BinlogPosition { get; set; }

    /// <summary>
    ///     Gets or sets the initial GTID set for CDC resumption (Gtid mode).
    /// </summary>
    public string? GtidSet { get; set; }

    /// <summary>
    ///     Gets or sets the CDC read timeout in seconds. Default is 30.
    /// </summary>
    public int CdcReadTimeoutSeconds { get; set; } = 30;

    /// <summary>
    ///     Gets or sets whether to emit verbose binlog-read log messages. Default is <c>false</c>.
    /// </summary>
    public bool LogBinlogRead { get; set; }

    // Observability

    /// <summary>
    ///     Gets or sets whether to enable metrics collection. Default is <c>true</c>.
    /// </summary>
    public bool EnableMetrics { get; set; } = true;

    /// <summary>
    ///     Validates the configuration.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any configuration value is invalid.</exception>
    public virtual void Validate()
    {
        ValidateConnectionSettings();

        if (BatchSize <= 0)
            throw new ArgumentException("BatchSize must be greater than zero.", nameof(BatchSize));

        if (MaxBatchSize <= 0)
            throw new ArgumentException("MaxBatchSize must be greater than zero.", nameof(MaxBatchSize));

        if (BatchSize > MaxBatchSize)
            throw new ArgumentException("BatchSize cannot exceed MaxBatchSize.", nameof(BatchSize));

        if (MaxRetryAttempts < 0)
            throw new ArgumentException("MaxRetryAttempts cannot be negative.", nameof(MaxRetryAttempts));

        if (RetryDelay < TimeSpan.Zero)
            throw new ArgumentException("RetryDelay cannot be negative.", nameof(RetryDelay));

        if (BulkLoadTimeout <= 0)
            throw new ArgumentException("BulkLoadTimeout must be greater than zero.", nameof(BulkLoadTimeout));

        if (UseUpsert && OnDuplicateKeyAction == OnDuplicateKeyAction.Update
                      && (UpsertKeyColumns == null || UpsertKeyColumns.Length == 0))
        {
            throw new ArgumentException(
                "UpsertKeyColumns must be provided when UseUpsert is enabled with OnDuplicateKeyAction.Update.",
                nameof(UpsertKeyColumns));
        }

        ValidateCheckpointSettings();
    }

    /// <summary>
    ///     Validates connection pool and transport-related settings.
    /// </summary>
    internal void ValidateConnectionSettings()
    {
        if (CommandTimeout <= 0)
            throw new ArgumentException("CommandTimeout must be greater than zero.", nameof(CommandTimeout));

        if (ConnectionTimeout <= 0)
            throw new ArgumentException("ConnectionTimeout must be greater than zero.", nameof(ConnectionTimeout));

        if (MinPoolSize < 0)
            throw new ArgumentException("MinPoolSize cannot be negative.", nameof(MinPoolSize));

        if (MaxPoolSize <= 0)
            throw new ArgumentException("MaxPoolSize must be greater than zero.", nameof(MaxPoolSize));

        if (MinPoolSize > MaxPoolSize)
            throw new ArgumentException("MinPoolSize cannot exceed MaxPoolSize.", nameof(MinPoolSize));
    }

    /// <summary>
    ///     Validates checkpoint-related settings.
    /// </summary>
    internal void ValidateCheckpointSettings()
    {
        if (CheckpointStrategy != CheckpointStrategy.None &&
            CheckpointStrategy != CheckpointStrategy.InMemory &&
            CheckpointStorage == null &&
            string.IsNullOrWhiteSpace(CheckpointFilePath))
        {
            throw new ArgumentException(
                $"CheckpointStorage or CheckpointFilePath must be provided when CheckpointStrategy is {CheckpointStrategy}.",
                nameof(CheckpointStorage));
        }

        if (CheckpointStrategy == CheckpointStrategy.Offset &&
            string.IsNullOrWhiteSpace(CheckpointOffsetColumn))
        {
            throw new ArgumentException(
                "CheckpointOffsetColumn must be provided when using Offset checkpoint strategy.",
                nameof(CheckpointOffsetColumn));
        }

        if (CheckpointStrategy == CheckpointStrategy.KeyBased &&
            (CheckpointKeyColumns == null || CheckpointKeyColumns.Length == 0))
        {
            throw new ArgumentException(
                "CheckpointKeyColumns must be provided when using KeyBased checkpoint strategy.",
                nameof(CheckpointKeyColumns));
        }
    }
}
