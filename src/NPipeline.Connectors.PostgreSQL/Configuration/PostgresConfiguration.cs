using Npgsql;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.PostgreSQL.Mapping;

namespace NPipeline.Connectors.PostgreSQL.Configuration;

/// <summary>
///     Configuration settings for PostgreSQL connector operations.
/// </summary>
public class PostgresConfiguration
{
    private const int DefaultBatchSize = 100;
    private const int DefaultMaxBatchSize = 1_000;
    private const int DefaultCommandTimeoutSeconds = 30;
    private const int DefaultConnectionTimeoutSeconds = 15;
    private const int DefaultCopyTimeoutSeconds = 300;
    private const int DefaultReadBufferSize = 8_192;
    private const int DefaultMinPoolSize = 1;
    private const int DefaultMaxPoolSize = 100;

    /// <summary>
    ///     Gets or sets the PostgreSQL connection string.
    ///     Optional when an <see cref="Npgsql.NpgsqlDataSource" /> is provided directly.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the default schema name for PostgreSQL operations.
    ///     Default is "public".
    /// </summary>
    public string Schema { get; set; } = "public";

    /// <summary>
    ///     Gets or sets the command timeout in seconds.
    /// </summary>
    public int CommandTimeout { get; set; } = DefaultCommandTimeoutSeconds;

    /// <summary>
    ///     Gets or sets the COPY timeout in seconds.
    /// </summary>
    public int CopyTimeout { get; set; } = DefaultCopyTimeoutSeconds;

    /// <summary>
    ///     Gets or sets the connection timeout in seconds.
    /// </summary>
    public int ConnectionTimeout { get; set; } = DefaultConnectionTimeoutSeconds;

    /// <summary>
    ///     Gets or sets the minimum pool size for connection pooling.
    /// </summary>
    public int MinPoolSize { get; set; } = DefaultMinPoolSize;

    /// <summary>
    ///     Gets or sets the maximum pool size for connection pooling.
    /// </summary>
    public int MaxPoolSize { get; set; } = DefaultMaxPoolSize;

    /// <summary>
    ///     Gets or sets whether to enable SSL mode.
    /// </summary>
    public bool UseSslMode { get; set; }

    /// <summary>
    ///     Gets or sets the SSL mode to use when <see cref="UseSslMode" /> is true.
    /// </summary>
    public SslMode? SslMode { get; set; }

    /// <summary>
    ///     Gets or sets the read buffer size in bytes for streaming operations.
    /// </summary>
    public int ReadBufferSize { get; set; } = DefaultReadBufferSize;

    /// <summary>
    ///     Gets or sets the write strategy used by the sink.
    /// </summary>
    public PostgresWriteStrategy WriteStrategy { get; set; } = PostgresWriteStrategy.Batch;

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
    ///     Gets or sets whether to use INSERT ... ON CONFLICT upsert semantics.
    /// </summary>
    public bool UseUpsert { get; set; }

    /// <summary>
    ///     Gets or sets the conflict target columns for upsert.
    /// </summary>
    public string[]? UpsertConflictColumns { get; set; }

    /// <summary>
    ///     Gets or sets the conflict resolution action.
    /// </summary>
    public OnConflictAction OnConflictAction { get; set; } = OnConflictAction.Update;

    /// <summary>
    ///     Gets or sets whether to continue when a row-level error occurs.
    /// </summary>
    public bool ContinueOnError { get; set; }

    /// <summary>
    ///     Gets or sets a handler for row-level errors. Return true to swallow the exception.
    /// </summary>
    public Func<Exception, PostgresRow?, bool>? RowErrorHandler { get; set; }

    /// <summary>
    ///     Gets or sets whether to validate identifiers to reduce injection risk.
    /// </summary>
    public bool ValidateIdentifiers { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts for transient errors.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    ///     Gets or sets the delay between retry attempts.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets whether to perform case-insensitive column matching.
    /// </summary>
    public bool CaseInsensitiveMapping { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to cache mapping metadata and compiled delegates.
    /// </summary>
    public bool CacheMappingMetadata { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to use binary format for COPY writes.
    /// </summary>
    public bool UseBinaryCopy { get; set; }

    /// <summary>
    ///     Gets or sets whether to stream results when reading.
    /// </summary>
    public bool StreamResults { get; set; } = true;

    /// <summary>
    ///     Gets or sets the number of rows to fetch per round trip when streaming (driver may ignore).
    /// </summary>
    public int FetchSize { get; set; } = 1_000;

    /// <summary>
    ///     Gets or sets whether to throw on mapping errors.
    /// </summary>
    public bool ThrowOnMappingError { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to use prepared statements for writes.
    /// </summary>
    public bool UsePreparedStatements { get; set; } = true;

    /// <summary>
    ///     Gets or sets the delivery semantic for retry behavior.
    /// </summary>
    public DeliverySemantic DeliverySemantic { get; set; } = DeliverySemantic.AtLeastOnce;

    /// <summary>
    ///     Gets or sets the checkpoint strategy. CDC is reserved for future use.
    /// </summary>
    public CheckpointStrategy CheckpointStrategy { get; set; } = CheckpointStrategy.None;

    /// <summary>
    ///     Validates the configuration.
    /// </summary>
    public virtual void Validate()
    {
        if (string.IsNullOrWhiteSpace(Schema))
            throw new ArgumentException("Schema cannot be empty.", nameof(Schema));

        ValidateConnectionSettings();

        if (CopyTimeout <= 0)
            throw new ArgumentException("CopyTimeout must be greater than zero.", nameof(CopyTimeout));

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

        ValidateFeatureSupport();

        if (UseUpsert && (UpsertConflictColumns == null || UpsertConflictColumns.Length == 0))
            throw new ArgumentException("UpsertConflictColumns must be provided when UseUpsert is enabled.", nameof(UpsertConflictColumns));
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

        if (ReadBufferSize <= 0)
            throw new ArgumentException("ReadBufferSize must be greater than zero.", nameof(ReadBufferSize));
    }

    /// <summary>
    ///     Validates whether requested features are supported by this connector edition.
    ///     Override in commercial editions to enable additional features.
    /// </summary>
    protected virtual void ValidateFeatureSupport()
    {
        if (WriteStrategy == PostgresWriteStrategy.Copy)
            throw new NotSupportedException("PostgresWriteStrategy.Copy is available in the commercial PostgreSQL connector.");

        if (UseBinaryCopy)
            throw new NotSupportedException("Binary COPY is available in the commercial PostgreSQL connector.");

        if (UseUpsert)
            throw new NotSupportedException("Upsert support is available in the commercial PostgreSQL connector.");

        if (DeliverySemantic == DeliverySemantic.ExactlyOnce)
            throw new NotSupportedException("Exactly-once delivery semantics are available in the commercial PostgreSQL connector.");

        if (CheckpointStrategy != CheckpointStrategy.None && CheckpointStrategy != CheckpointStrategy.InMemory)
            throw new NotSupportedException("Advanced checkpointing is available in the commercial PostgreSQL connector.");
    }
}
