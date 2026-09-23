using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Reliability;

namespace NPipeline.Connectors.MongoDB.ChangeStream;

/// <summary>
///     Configuration for MongoDB change stream sources.
/// </summary>
public class MongoChangeStreamConfiguration
{
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
    ///     If null or empty, watches the entire database.
    /// </summary>
    public string? CollectionName { get; set; }

    /// <summary>
    ///     Validates the configuration.
    /// </summary>
    public virtual void Validate()
    {
        if (string.IsNullOrWhiteSpace(DatabaseName))
            throw new ArgumentException("DatabaseName cannot be empty.", nameof(DatabaseName));

        ValidateChangeStreamSettings();
        ValidateResilienceSettings();
    }

    /// <summary>
    ///     Validates change stream-related settings.
    /// </summary>
    internal void ValidateChangeStreamSettings()
    {
        if (BatchSize.HasValue && BatchSize.Value <= 0)
            throw new ArgumentException("BatchSize must be greater than zero.", nameof(BatchSize));

        if (MaxAwaitTime <= TimeSpan.Zero)
            throw new ArgumentException("MaxAwaitTime must be greater than zero.", nameof(MaxAwaitTime));
    }

    /// <summary>
    ///     Validates resilience-related settings.
    /// </summary>
    internal void ValidateResilienceSettings()
    {
        ArgumentNullException.ThrowIfNull(Resilience);
        Resilience.Validate();
    }

    /// <summary>
    ///     Creates a copy of this configuration.
    /// </summary>
    /// <returns>A new <see cref="MongoChangeStreamConfiguration" /> with the same values.</returns>
    public MongoChangeStreamConfiguration Clone()
    {
        return new MongoChangeStreamConfiguration
        {
            ConnectionString = ConnectionString,
            DatabaseName = DatabaseName,
            CollectionName = CollectionName,
            OperationTypes = OperationTypes,
            ResumeToken = ResumeToken,
            FullDocument = FullDocument,
            FullDocumentOption = FullDocumentOption,
            BatchSize = BatchSize,
            MaxAwaitTime = MaxAwaitTime,
            StartAtOperationTime = StartAtOperationTime,
            Resilience = Resilience,
            ContinueOnError = ContinueOnError,
            DocumentErrorHandler = DocumentErrorHandler,
            CaseInsensitiveMapping = CaseInsensitiveMapping,
            ThrowOnMappingError = ThrowOnMappingError,
        };
    }

    #region Change Stream Options

    /// <summary>
    ///     Gets or sets the operation types to include in the change stream.
    ///     If null, all operation types are included.
    /// </summary>
    public MongoChangeStreamOperationType[]? OperationTypes { get; set; }

    /// <summary>
    ///     Gets or sets the resume token to start from.
    ///     If null, starts from the current position.
    /// </summary>
    public BsonDocument? ResumeToken { get; set; }

    /// <summary>
    ///     Gets or sets whether to include the full document for update operations.
    ///     Default is true.
    /// </summary>
    public bool FullDocument { get; set; } = true;

    /// <summary>
    ///     Gets or sets the full document option for the change stream.
    ///     Default is UpdateLookup, which fetches the full document on updates.
    /// </summary>
    public ChangeStreamFullDocumentOption FullDocumentOption { get; set; } = ChangeStreamFullDocumentOption.UpdateLookup;

    /// <summary>
    ///     Gets or sets the batch size for the change stream.
    ///     If null, uses the server default.
    /// </summary>
    public int? BatchSize { get; set; }

    /// <summary>
    ///     Gets or sets the maximum await time for each batch.
    ///     Default is 5 seconds.
    /// </summary>
    public TimeSpan MaxAwaitTime { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     Gets or sets the start at operation time for the change stream.
    ///     If null, starts from the current position or resume token.
    /// </summary>
    public BsonTimestamp? StartAtOperationTime { get; set; }

    #endregion

    #region Resilience Properties

    /// <summary>
    ///     Gets or sets how opening the change stream is retried. Defaults to
    ///     <see cref="MongoConnectorResilience.ChangeStream" />: four attempts with jittered exponential backoff from two
    ///     seconds up to 30 seconds. Use <see cref="NResilience.Resilience.None" /> to turn retries off.
    /// </summary>
    /// <remarks>
    ///     Once the stream is open, the driver resumes it by itself after a resumable error (a network error, a primary
    ///     step-down, and similar), from the last change it returned. A failure that the driver can't resume ends the
    ///     stream with that exception. <see cref="Nodes.MongoChangeStreamSourceNode{T}.ResumeToken" /> then holds the
    ///     token of the last change the node emitted, and opening the same node again resumes after it.
    /// </remarks>
    public NResilience.Resilience Resilience { get; set; } = MongoConnectorResilience.ChangeStream;

    /// <summary>
    ///     Gets or sets whether to continue when an error occurs.
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
    ///     Gets or sets whether to throw on mapping errors.
    ///     Default is true.
    /// </summary>
    public bool ThrowOnMappingError { get; set; } = true;

    #endregion
}
