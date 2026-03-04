using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.MongoDB.Configuration;

/// <summary>
///     Configuration settings for MongoDB connector operations.
/// </summary>
public class MongoConfiguration
{
    private const int DefaultBatchSize = 1_000;
    private const int DefaultCommandTimeoutSeconds = 30;

    /// <summary>
    ///     Gets or sets the MongoDB connection string.
    ///     Optional when an <see cref="IMongoClient" /> is provided directly.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the database name.
    /// </summary>
    public string DatabaseName { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the collection name.
    /// </summary>
    public string CollectionName { get; set; } = string.Empty;

    /// <summary>
    ///     Validates the configuration.
    /// </summary>
    public virtual void Validate()
    {
        if (string.IsNullOrWhiteSpace(DatabaseName))
            throw new InvalidOperationException("DatabaseName cannot be empty.");

        if (string.IsNullOrWhiteSpace(CollectionName))
            throw new InvalidOperationException("CollectionName cannot be empty.");

        ValidateReadSettings();
        ValidateWriteSettings();
        ValidateResilienceSettings();
        ValidateCheckpointSettings();
    }

    /// <summary>
    ///     Validates read-related settings.
    /// </summary>
    internal void ValidateReadSettings()
    {
        if (BatchSize <= 0)
            throw new ArgumentException("BatchSize must be greater than zero.", nameof(BatchSize));

        if (CommandTimeoutSeconds <= 0)
            throw new ArgumentException("CommandTimeoutSeconds must be greater than zero.", nameof(CommandTimeoutSeconds));
    }

    /// <summary>
    ///     Validates write-related settings.
    /// </summary>
    internal void ValidateWriteSettings()
    {
        if (WriteBatchSize <= 0)
            throw new ArgumentException("WriteBatchSize must be greater than zero.", nameof(WriteBatchSize));

        if (WriteStrategy == MongoWriteStrategy.Upsert &&
            UpsertKeyFields.Length == 0)
        {
            throw new ArgumentException(
                "UpsertKeyFields must be provided when WriteStrategy is Upsert.",
                nameof(UpsertKeyFields));
        }

        if (OnDuplicate == OnDuplicateAction.Overwrite &&
            UpsertKeyFields.Length == 0)
        {
            throw new ArgumentException(
                "UpsertKeyFields must be provided when OnDuplicate is Overwrite.",
                nameof(UpsertKeyFields));
        }
    }

    /// <summary>
    ///     Validates resilience-related settings.
    /// </summary>
    internal void ValidateResilienceSettings()
    {
        if (MaxRetryAttempts < 0)
            throw new ArgumentException("MaxRetryAttempts cannot be negative.", nameof(MaxRetryAttempts));

        if (RetryDelay < TimeSpan.Zero)
            throw new ArgumentException("RetryDelay cannot be negative.", nameof(RetryDelay));
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
            string.IsNullOrWhiteSpace(CheckpointOffsetField))
        {
            throw new ArgumentException(
                "CheckpointOffsetField must be provided when using Offset checkpoint strategy.",
                nameof(CheckpointOffsetField));
        }

        if (CheckpointStrategy == CheckpointStrategy.KeyBased &&
            (CheckpointKeyFields == null || CheckpointKeyFields.Length == 0))
        {
            throw new ArgumentException(
                "CheckpointKeyFields must be provided when using KeyBased checkpoint strategy.",
                nameof(CheckpointKeyFields));
        }
    }

    #region Read Properties

    /// <summary>
    ///     Gets or sets the batch size for reading documents.
    ///     Default is 1000.
    /// </summary>
    public int BatchSize { get; set; } = DefaultBatchSize;

    /// <summary>
    ///     Gets or sets whether to disable cursor timeout for long-running queries.
    ///     Default is false.
    /// </summary>
    public bool NoCursorTimeout { get; set; }

    /// <summary>
    ///     Gets or sets the read preference for MongoDB queries.
    ///     Default is null, which uses the server's default.
    /// </summary>
    public ReadPreferenceMode? ReadPreference { get; set; }

    /// <summary>
    ///     Gets or sets the command timeout in seconds.
    ///     Default is 30 seconds.
    /// </summary>
    public int CommandTimeoutSeconds { get; set; } = DefaultCommandTimeoutSeconds;

    /// <summary>
    ///     Gets or sets whether to stream results when reading.
    ///     Default is true.
    /// </summary>
    public bool StreamResults { get; set; } = true;

    #endregion

    #region Write Properties

    /// <summary>
    ///     Gets or sets the write strategy used by the sink.
    ///     Default is BulkWrite.
    /// </summary>
    public MongoWriteStrategy WriteStrategy { get; set; } = MongoWriteStrategy.BulkWrite;

    /// <summary>
    ///     Gets or sets the batch size for batched writes.
    ///     Default is 1000.
    /// </summary>
    public int WriteBatchSize { get; set; } = DefaultBatchSize;

    /// <summary>
    ///     Gets or sets whether writes should be executed in order.
    ///     When false (the default), MongoDB may reorder writes for better throughput.
    /// </summary>
    public bool OrderedWrites { get; set; }

    /// <summary>
    ///     Gets or sets the action to take when a duplicate key is encountered.
    ///     Default is Fail.
    /// </summary>
    public OnDuplicateAction OnDuplicate { get; set; } = OnDuplicateAction.Fail;

    /// <summary>
    ///     Gets or sets the key fields to use for upsert operations.
    ///     Required when <see cref="WriteStrategy" /> is Upsert or <see cref="OnDuplicate" /> is Overwrite.
    ///     Defaults to ["_id"].
    /// </summary>
    public string[] UpsertKeyFields { get; set; } = ["_id"];

    #endregion

    #region Resilience Properties

    /// <summary>
    ///     Gets or sets the maximum number of retry attempts for transient errors.
    ///     Default is 3.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    ///     Gets or sets the delay between retry attempts.
    ///     Default is 1 second.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     Gets or sets whether to continue when a document-level error occurs.
    ///     Default is false.
    /// </summary>
    public bool ContinueOnError { get; set; }

    /// <summary>
    ///     Gets or sets a handler for document-level errors. Return true to swallow the exception.
    /// </summary>
    public Func<Exception, BsonDocument?, bool>? DocumentErrorHandler { get; set; }

    #endregion

    #region Mapping Properties

    /// <summary>
    ///     Gets or sets whether to perform case-insensitive field matching.
    ///     Default is true.
    /// </summary>
    public bool CaseInsensitiveMapping { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to cache mapping metadata and compiled delegates.
    ///     Default is true.
    /// </summary>
    public bool CacheMappingMetadata { get; set; } = true;

    /// <summary>
    ///     Gets or sets whether to throw on mapping errors.
    ///     Default is true.
    /// </summary>
    public bool ThrowOnMappingError { get; set; } = true;

    #endregion

    #region Checkpoint Properties

    /// <summary>
    ///     Gets or sets the delivery semantic for retry behavior.
    ///     Default is AtLeastOnce.
    /// </summary>
    public DeliverySemantic DeliverySemantic { get; set; } = DeliverySemantic.AtLeastOnce;

    /// <summary>
    ///     Gets or sets the checkpoint strategy.
    ///     Default is None.
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
    ///     Gets or sets the collection name for database checkpoint storage.
    /// </summary>
    public string CheckpointCollectionName { get; set; } = "pipeline_checkpoints";

    /// <summary>
    ///     Gets or sets the field name for offset-based checkpointing.
    /// </summary>
    public string? CheckpointOffsetField { get; set; }

    /// <summary>
    ///     Gets or sets the key fields for key-based checkpointing.
    /// </summary>
    public string[]? CheckpointKeyFields { get; set; }

    #endregion
}
