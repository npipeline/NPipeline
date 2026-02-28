using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.Snowflake.Mapping;

namespace NPipeline.Connectors.Snowflake.Configuration;

/// <summary>
///     Configuration settings for Snowflake connector operations.
/// </summary>
public class SnowflakeConfiguration
{
    private const int DefaultBatchSize = 1_000;
    private const int DefaultMaxBatchSize = 16_384;
    private const int DefaultCommandTimeoutSeconds = 300;
    private const int DefaultConnectionTimeoutSeconds = 30;
    private const int DefaultMinPoolSize = 1;
    private const int DefaultMaxPoolSize = 10;
    private const int DefaultFetchSize = 10_000;

    // Connection Settings

    /// <summary>
    ///     Gets or sets the Snowflake connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Snowflake account identifier.
    /// </summary>
    public string Account { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Snowflake user name.
    /// </summary>
    public string User { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Snowflake password.
    /// </summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Snowflake role to use.
    /// </summary>
    public string Role { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Snowflake warehouse to use.
    /// </summary>
    public string Warehouse { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Snowflake database to use.
    /// </summary>
    public string Database { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the default schema name for Snowflake operations.
    ///     Default is "PUBLIC".
    /// </summary>
    public string Schema { get; set; } = "PUBLIC";

    /// <summary>
    ///     Gets or sets the authenticator type.
    ///     Default is "snowflake" (password-based). Use "snowflake_jwt" for key-pair authentication.
    /// </summary>
    public string Authenticator { get; set; } = "snowflake";

    /// <summary>
    ///     Gets or sets the private key file path for key-pair authentication.
    /// </summary>
    public string? PrivateKeyPath { get; set; }

    /// <summary>
    ///     Gets or sets the private key passphrase for key-pair authentication.
    /// </summary>
    public string? PrivateKeyPassphrase { get; set; }

    /// <summary>
    ///     Gets or sets the command timeout in seconds.
    ///     Default is 300 (5 minutes) since Snowflake queries can be long-running.
    /// </summary>
    public int CommandTimeout { get; set; } = DefaultCommandTimeoutSeconds;

    /// <summary>
    ///     Gets or sets the connection timeout in seconds.
    /// </summary>
    public int ConnectionTimeout { get; set; } = DefaultConnectionTimeoutSeconds;

    // Pool Settings

    /// <summary>
    ///     Gets or sets the minimum pool size for connection pooling.
    /// </summary>
    public int MinPoolSize { get; set; } = DefaultMinPoolSize;

    /// <summary>
    ///     Gets or sets the maximum pool size for connection pooling.
    ///     Default is 10 since Snowflake has high connection setup cost.
    /// </summary>
    public int MaxPoolSize { get; set; } = DefaultMaxPoolSize;

    // Read Settings

    /// <summary>
    ///     Gets or sets whether to stream results when reading.
    /// </summary>
    public bool StreamResults { get; set; } = true;

    /// <summary>
    ///     Gets or sets the number of rows to fetch per round trip when streaming.
    ///     Default is 10,000 (higher than local databases due to cloud network latency).
    /// </summary>
    public int FetchSize { get; set; } = DefaultFetchSize;

    // Write Settings

    /// <summary>
    ///     Gets or sets the write strategy used by the sink.
    /// </summary>
    public SnowflakeWriteStrategy WriteStrategy { get; set; } = SnowflakeWriteStrategy.Batch;

    /// <summary>
    ///     Gets or sets the batch size for batched writes.
    /// </summary>
    public int BatchSize { get; set; } = DefaultBatchSize;

    /// <summary>
    ///     Gets or sets the maximum batch size. Snowflake multi-insert limit is 16,384 rows.
    /// </summary>
    public int MaxBatchSize { get; set; } = DefaultMaxBatchSize;

    /// <summary>
    ///     Gets or sets whether to wrap writes in a transaction.
    /// </summary>
    public bool UseTransaction { get; set; } = true;

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

    // Staged Copy Settings

    /// <summary>
    ///     Gets or sets the stage name for staged copy operations.
    ///     Default is "~" (user stage).
    /// </summary>
    public string StageName { get; set; } = "~";

    /// <summary>
    ///     Gets or sets the file format for staged copy operations.
    ///     Default is "CSV".
    /// </summary>
    public string FileFormat { get; set; } = "CSV";

    /// <summary>
    ///     Gets or sets the compression for staged copy operations.
    ///     Default is "GZIP".
    /// </summary>
    public string CopyCompression { get; set; } = "GZIP";

    /// <summary>
    ///     Gets or sets the file prefix for staged copy operations.
    /// </summary>
    public string StageFilePrefix { get; set; } = "npipeline_";

    /// <summary>
    ///     Gets or sets whether to purge staged files after a successful COPY INTO.
    /// </summary>
    public bool PurgeAfterCopy { get; set; } = true;

    /// <summary>
    ///     Gets or sets the ON_ERROR action for COPY INTO.
    ///     Valid values: ABORT_STATEMENT, CONTINUE, SKIP_FILE.
    /// </summary>
    public string OnErrorAction { get; set; } = "ABORT_STATEMENT";

    // Error Handling

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts for transient errors.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    ///     Gets or sets the delay between retry attempts.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    ///     Gets or sets whether to continue when a row-level error occurs.
    /// </summary>
    public bool ContinueOnError { get; set; }

    /// <summary>
    ///     Gets or sets a handler for row-level errors. Return true to swallow the exception.
    /// </summary>
    public Func<Exception, SnowflakeRow?, bool>? RowErrorHandler { get; set; }

    // Mapping Options

    /// <summary>
    ///     Gets or sets whether to validate identifiers to reduce injection risk.
    /// </summary>
    public bool ValidateIdentifiers { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to perform case-insensitive column matching.
    ///     Default is true since Snowflake returns uppercase column names.
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
    ///     Gets or sets the checkpoint strategy.
    /// </summary>
    public CheckpointStrategy CheckpointStrategy { get; set; } = CheckpointStrategy.None;

    /// <summary>
    ///     Gets or sets the checkpoint storage backend.
    ///     Required when CheckpointStrategy is not None or InMemory.
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
    ///     Gets or sets the table name for database checkpoint storage.
    /// </summary>
    public string CheckpointTableName { get; set; } = "PIPELINE_CHECKPOINTS";

    /// <summary>
    ///     Gets or sets the offset column name for offset-based checkpointing.
    /// </summary>
    public string? CheckpointOffsetColumn { get; set; }

    /// <summary>
    ///     Gets or sets the key columns for key-based checkpointing.
    /// </summary>
    public string[]? CheckpointKeyColumns { get; set; }

    // Snowflake-Specific Settings

    /// <summary>
    ///     Gets or sets whether to use schema-qualified names (e.g., DB.SCHEMA."TABLE").
    /// </summary>
    public bool UseSchemaQualifiedNames { get; set; } = true;

    /// <summary>
    ///     Gets or sets the query tag for Snowflake query history observability.
    ///     Default is "NPipeline".
    /// </summary>
    public string? QueryTag { get; set; } = "NPipeline";

    /// <summary>
    ///     Gets or sets whether to keep the Snowflake session alive.
    /// </summary>
    public bool KeepSessionAlive { get; set; }

    /// <summary>
    ///     Validates the configuration.
    /// </summary>
    public virtual void Validate()
    {
        if (string.IsNullOrWhiteSpace(Schema))
            throw new ArgumentException("Schema cannot be empty.", nameof(Schema));

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

        if (FetchSize <= 0)
            throw new ArgumentException("FetchSize must be greater than zero.", nameof(FetchSize));

        if (UseUpsert && (UpsertKeyColumns == null || UpsertKeyColumns.Length == 0))
            throw new ArgumentException("UpsertKeyColumns must be provided when UseUpsert is enabled.", nameof(UpsertKeyColumns));

        if (WriteStrategy == SnowflakeWriteStrategy.StagedCopy && string.IsNullOrWhiteSpace(StageName))
            throw new ArgumentException("StageName must be provided when using StagedCopy write strategy.", nameof(StageName));

        if (WriteStrategy == SnowflakeWriteStrategy.StagedCopy && DeliverySemantic == DeliverySemantic.ExactlyOnce)
        {
            throw new ArgumentException(
                "Snowflake StagedCopy strategy does not support ExactlyOnce delivery semantics. Use PerRow or Batch for ExactlyOnce.",
                nameof(WriteStrategy));
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
