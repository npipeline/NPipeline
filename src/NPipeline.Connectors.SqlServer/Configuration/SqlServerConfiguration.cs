using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.SqlServer.Mapping;

namespace NPipeline.Connectors.SqlServer.Configuration;

/// <summary>
///     Configuration settings for SQL Server connector operations.
/// </summary>
public class SqlServerConfiguration
{
    private const int DefaultBatchSize = 100;
    private const int DefaultMaxBatchSize = 1_000;
    private const int DefaultCommandTimeoutSeconds = 30;
    private const int DefaultConnectionTimeoutSeconds = 15;
    private const int DefaultBulkCopyTimeoutSeconds = 300;
    private const int DefaultMinPoolSize = 1;
    private const int DefaultMaxPoolSize = 100;
    private const int DefaultBulkCopyBatchSize = 5_000;
    private const int DefaultBulkCopyNotifyAfter = 1_000;
    private const int DefaultFetchSize = 1_000;

    // Connection Settings

    /// <summary>
    ///     Gets or sets the SQL Server connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the default schema name for SQL Server operations.
    ///     Default is "dbo".
    /// </summary>
    public string Schema { get; set; } = "dbo";

    /// <summary>
    ///     Gets or sets the command timeout in seconds.
    /// </summary>
    public int CommandTimeout { get; set; } = DefaultCommandTimeoutSeconds;

    /// <summary>
    ///     Gets or sets the connection timeout in seconds.
    /// </summary>
    public int ConnectionTimeout { get; set; } = DefaultConnectionTimeoutSeconds;

    /// <summary>
    ///     Gets or sets the bulk copy timeout in seconds.
    /// </summary>
    public int BulkCopyTimeout { get; set; } = DefaultBulkCopyTimeoutSeconds;

    // Pool Settings

    /// <summary>
    ///     Gets or sets the minimum pool size for connection pooling.
    /// </summary>
    public int MinPoolSize { get; set; } = DefaultMinPoolSize;

    /// <summary>
    ///     Gets or sets the maximum pool size for connection pooling.
    /// </summary>
    public int MaxPoolSize { get; set; } = DefaultMaxPoolSize;

    /// <summary>
    ///     Gets or sets the pool connection timeout in seconds.
    /// </summary>
    public int ConnectTimeout { get; set; } = DefaultConnectionTimeoutSeconds;

    // Write Settings

    /// <summary>
    ///     Gets or sets the write strategy used by the sink.
    /// </summary>
    public SqlServerWriteStrategy WriteStrategy { get; set; } = SqlServerWriteStrategy.Batch;

    /// <summary>
    ///     Gets or sets the batch size for batched writes.
    /// </summary>
    public int BatchSize { get; set; } = DefaultBatchSize;

    /// <summary>
    ///     Gets or sets the maximum batch size to avoid unbounded memory usage.
    /// </summary>
    public int MaxBatchSize { get; set; } = DefaultMaxBatchSize;

    /// <summary>
    ///     Gets or sets whether to wrap writes in a transaction.
    /// </summary>
    public bool UseTransaction { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to use prepared statements for writes.
    /// </summary>
    public bool UsePreparedStatements { get; set; } = true;

    // Upsert Settings

    /// <summary>
    ///     Gets or sets whether to use MERGE-based upserts.
    /// </summary>
    public bool UseUpsert { get; set; }

    /// <summary>
    ///     Gets or sets the key columns for MERGE matching.
    ///     Required when <see cref="UseUpsert" /> is enabled.
    /// </summary>
    public string[]? UpsertKeyColumns { get; set; }

    /// <summary>
    ///     Gets or sets the action to take when a MERGE statement encounters a match.
    /// </summary>
    public OnMergeAction OnMergeAction { get; set; } = OnMergeAction.Update;

    // Bulk Copy Settings

    /// <summary>
    ///     Gets or sets the number of rows per bulk copy batch.
    /// </summary>
    public int BulkCopyBatchSize { get; set; } = DefaultBulkCopyBatchSize;

    /// <summary>
    ///     Gets or sets the number of rows to process before generating a notification event.
    /// </summary>
    public int BulkCopyNotifyAfter { get; set; } = DefaultBulkCopyNotifyAfter;

    /// <summary>
    ///     Gets or sets whether to enable streaming for bulk copy operations.
    /// </summary>
    public bool EnableStreaming { get; set; } = true;

    // Read Settings

    /// <summary>
    ///     Gets or sets whether to stream results when reading.
    /// </summary>
    public bool StreamResults { get; set; } = true;

    /// <summary>
    ///     Gets or sets the number of rows to fetch per round trip when streaming.
    /// </summary>
    public int FetchSize { get; set; } = DefaultFetchSize;

    // Error Handling

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts for transient errors.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    ///     Gets or sets the delay between retry attempts.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets whether to continue when a row-level error occurs.
    /// </summary>
    public bool ContinueOnError { get; set; }

    /// <summary>
    ///     Gets or sets a handler for row-level errors. Return true to swallow the exception.
    /// </summary>
    public Func<Exception, SqlServerRow?, bool>? RowErrorHandler { get; set; }

    // Mapping Options

    /// <summary>
    ///     Gets or sets whether to validate identifiers to reduce injection risk.
    /// </summary>
    public bool ValidateIdentifiers { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to perform case-insensitive column matching.
    /// </summary>
    public bool CaseInsensitiveMapping { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to cache mapping metadata and compiled delegates.
    /// </summary>
    public bool CacheMappingMetadata { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to throw on mapping errors.
    /// </summary>
    public bool ThrowOnMappingError { get; set; } = true;

    // Delivery & Checkpointing

    /// <summary>
    ///     Gets or sets the delivery semantic for retry behavior.
    /// </summary>
    public DeliverySemantic DeliverySemantic { get; set; } = DeliverySemantic.AtLeastOnce;

    /// <summary>
    ///     Gets or sets the checkpoint strategy. CDC is reserved for future use.
    /// </summary>
    public CheckpointStrategy CheckpointStrategy { get; set; } = CheckpointStrategy.None;

    /// <summary>
    ///     Gets or sets the checkpoint storage backend.
    ///     Required when CheckpointStrategy is not None or InMemory.
    /// </summary>
    public ICheckpointStorage? CheckpointStorage { get; set; }

    /// <summary>
    ///     Gets or sets the checkpoint interval configuration.
    ///     Controls how often checkpoints are saved during processing.
    /// </summary>
    public CheckpointIntervalConfiguration CheckpointInterval { get; set; } = new();

    /// <summary>
    ///     Gets or sets the file path for file-based checkpoint storage.
    ///     Used when CheckpointStorage is a FileCheckpointStorage created automatically.
    /// </summary>
    public string? CheckpointFilePath { get; set; }

    /// <summary>
    ///     Gets or sets the table name for database checkpoint storage.
    ///     Used when using database-based checkpoint storage.
    /// </summary>
    public string CheckpointTableName { get; set; } = "pipeline_checkpoints";

    /// <summary>
    ///     Gets or sets the offset column name for offset-based checkpointing.
    ///     Used to generate WHERE clauses for resumable queries.
    /// </summary>
    public string? CheckpointOffsetColumn { get; set; }

    /// <summary>
    ///     Gets or sets the key columns for key-based checkpointing.
    ///     Used for composite key tracking.
    /// </summary>
    public string[]? CheckpointKeyColumns { get; set; }

    /// <summary>
    ///     Gets or sets the CDC capture instance name for CDC checkpointing.
    ///     Required when CheckpointStrategy is CDC.
    /// </summary>
    public string? CdcCaptureInstance { get; set; }

    // SQL Server Specific

    /// <summary>
    ///     Gets or sets whether to enable Multiple Active Result Sets (MARS).
    /// </summary>
    public bool EnableMARS { get; set; }

    /// <summary>
    ///     Gets or sets the application name for monitoring and connection tracking.
    /// </summary>
    public string? ApplicationName { get; set; }

    /// <summary>
    ///     Validates the configuration.
    /// </summary>
    public virtual void Validate()
    {
        if (string.IsNullOrWhiteSpace(Schema))
        {
            throw new ArgumentException("Schema cannot be empty.", nameof(Schema));
        }

        ValidateConnectionSettings();

        if (BulkCopyTimeout <= 0)
        {
            throw new ArgumentException("BulkCopyTimeout must be greater than zero.", nameof(BulkCopyTimeout));
        }

        if (BatchSize <= 0)
        {
            throw new ArgumentException("BatchSize must be greater than zero.", nameof(BatchSize));
        }

        if (MaxBatchSize <= 0)
        {
            throw new ArgumentException("MaxBatchSize must be greater than zero.", nameof(MaxBatchSize));
        }

        if (BatchSize > MaxBatchSize)
        {
            throw new ArgumentException("BatchSize cannot exceed MaxBatchSize.", nameof(BatchSize));
        }

        if (MaxRetryAttempts < 0)
        {
            throw new ArgumentException("MaxRetryAttempts cannot be negative.", nameof(MaxRetryAttempts));
        }

        if (RetryDelay < TimeSpan.Zero)
        {
            throw new ArgumentException("RetryDelay cannot be negative.", nameof(RetryDelay));
        }

        if (FetchSize <= 0)
        {
            throw new ArgumentException("FetchSize must be greater than zero.", nameof(FetchSize));
        }

        if (UseUpsert && (UpsertKeyColumns == null || UpsertKeyColumns.Length == 0))
        {
            throw new ArgumentException("UpsertKeyColumns must be provided when UseUpsert is enabled.", nameof(UpsertKeyColumns));
        }

        ValidateCheckpointSettings();
    }

    /// <summary>
    ///     Validates connection pool and transport-related settings.
    /// </summary>
    internal void ValidateConnectionSettings()
    {
        if (CommandTimeout <= 0)
        {
            throw new ArgumentException("CommandTimeout must be greater than zero.", nameof(CommandTimeout));
        }

        if (ConnectionTimeout <= 0)
        {
            throw new ArgumentException("ConnectionTimeout must be greater than zero.", nameof(ConnectionTimeout));
        }

        if (ConnectTimeout <= 0)
        {
            throw new ArgumentException("ConnectTimeout must be greater than zero.", nameof(ConnectTimeout));
        }

        if (MinPoolSize < 0)
        {
            throw new ArgumentException("MinPoolSize cannot be negative.", nameof(MinPoolSize));
        }

        if (MaxPoolSize <= 0)
        {
            throw new ArgumentException("MaxPoolSize must be greater than zero.", nameof(MaxPoolSize));
        }

        if (MinPoolSize > MaxPoolSize)
        {
            throw new ArgumentException("MinPoolSize cannot exceed MaxPoolSize.", nameof(MinPoolSize));
        }
    }

    /// <summary>
    ///     Validates checkpoint-related settings.
    /// </summary>
    internal void ValidateCheckpointSettings()
    {
        // Validate checkpoint storage requirements
        if (CheckpointStrategy != CheckpointStrategy.None &&
            CheckpointStrategy != CheckpointStrategy.InMemory &&
            CheckpointStorage == null &&
            string.IsNullOrWhiteSpace(CheckpointFilePath))
        {
            throw new ArgumentException(
                $"CheckpointStorage or CheckpointFilePath must be provided when CheckpointStrategy is {CheckpointStrategy}.",
                nameof(CheckpointStorage));
        }

        // Validate offset checkpoint settings
        if (CheckpointStrategy == CheckpointStrategy.Offset &&
            string.IsNullOrWhiteSpace(CheckpointOffsetColumn))
        {
            throw new ArgumentException(
                "CheckpointOffsetColumn must be provided when using Offset checkpoint strategy.",
                nameof(CheckpointOffsetColumn));
        }

        // Validate key-based checkpoint settings
        if (CheckpointStrategy == CheckpointStrategy.KeyBased &&
            (CheckpointKeyColumns == null || CheckpointKeyColumns.Length == 0))
        {
            throw new ArgumentException(
                "CheckpointKeyColumns must be provided when using KeyBased checkpoint strategy.",
                nameof(CheckpointKeyColumns));
        }

        // Validate CDC checkpoint settings
        if (CheckpointStrategy == CheckpointStrategy.CDC &&
            string.IsNullOrWhiteSpace(CdcCaptureInstance))
        {
            throw new ArgumentException(
                "CdcCaptureInstance must be provided when using CDC checkpoint strategy.",
                nameof(CdcCaptureInstance));
        }
    }
}
