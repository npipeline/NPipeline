using MongoDB.Bson;

namespace NPipeline.Connectors.MongoDB.ChangeStream;

/// <summary>
///     Wraps a MongoDB change stream event with strongly-typed access.
/// </summary>
/// <typeparam name="TDocument">The type of the document.</typeparam>
public sealed class MongoChangeStreamEvent<TDocument>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoChangeStreamEvent{TDocument}" /> class.
    /// </summary>
    /// <param name="operationType">The type of operation that triggered the event.</param>
    /// <param name="fullDocument">The full document (for insert, update, replace operations).</param>
    /// <param name="resumeToken">The resume token for this event.</param>
    /// <param name="collectionName">The name of the collection.</param>
    /// <param name="databaseName">The name of the database.</param>
    /// <param name="clusterTime">The cluster time of the event.</param>
    /// <param name="updateDescription">The update description (for update events - contains updated fields).</param>
    /// <param name="documentKey">The document key (for delete events - contains _id).</param>
    public MongoChangeStreamEvent(
        MongoChangeStreamOperationType operationType,
        TDocument? fullDocument,
        BsonDocument resumeToken,
        string? collectionName,
        string? databaseName,
        DateTime clusterTime,
        BsonDocument? updateDescription,
        BsonDocument? documentKey)
    {
        OperationType = operationType;
        FullDocument = fullDocument;
        ResumeToken = resumeToken ?? throw new ArgumentNullException(nameof(resumeToken));
        CollectionName = collectionName;
        DatabaseName = databaseName;
        ClusterTime = clusterTime;
        UpdateDescription = updateDescription;
        DocumentKey = documentKey;
    }

    /// <summary>
    ///     Gets the type of operation that triggered this change stream event.
    /// </summary>
    public MongoChangeStreamOperationType OperationType { get; }

    /// <summary>
    ///     Gets the full document for insert, update, and replace operations.
    ///     Will be null for delete operations.
    /// </summary>
    public TDocument? FullDocument { get; }

    /// <summary>
    ///     Gets the resume token that can be used to resume the change stream from this point.
    /// </summary>
    public BsonDocument ResumeToken { get; }

    /// <summary>
    ///     Gets the name of the collection where the change occurred.
    /// </summary>
    public string? CollectionName { get; }

    /// <summary>
    ///     Gets the name of the database where the change occurred.
    /// </summary>
    public string? DatabaseName { get; }

    /// <summary>
    ///     Gets the cluster time when the event occurred.
    /// </summary>
    public DateTime ClusterTime { get; }

    /// <summary>
    ///     Gets the update description for update events, containing the updated fields and their values.
    ///     Null for non-update operations.
    /// </summary>
    public BsonDocument? UpdateDescription { get; }

    /// <summary>
    ///     Gets the document key (containing _id) for delete events.
    ///     Null for non-delete operations.
    /// </summary>
    public BsonDocument? DocumentKey { get; }

    /// <summary>
    ///     Gets the document identifier from the DocumentKey, if available.
    /// </summary>
    public BsonValue? DocumentId => DocumentKey?.GetValue("_id", null);

    /// <summary>
    ///     Gets a value indicating whether this event has a full document.
    /// </summary>
    public bool HasFullDocument => FullDocument is not null;

    /// <summary>
    ///     Maps this change stream event to a different document type using the provided mapper function.
    /// </summary>
    /// <typeparam name="TOther">The target type to map to.</typeparam>
    /// <param name="mapper">The mapping function.</param>
    /// <returns>A new <see cref="MongoChangeStreamEvent{TOther}" /> with the mapped document.</returns>
    public MongoChangeStreamEvent<TOther> Map<TOther>(Func<TDocument?, TOther?> mapper)
    {
        ArgumentNullException.ThrowIfNull(mapper);

        var mappedDocument = mapper(FullDocument);

        return new MongoChangeStreamEvent<TOther>(
            OperationType,
            mappedDocument,
            ResumeToken,
            CollectionName,
            DatabaseName,
            ClusterTime,
            UpdateDescription,
            DocumentKey);
    }

    /// <summary>
    ///     Returns a string representation of this change stream event.
    /// </summary>
    /// <returns>A string describing the event.</returns>
    public override string ToString()
    {
        return $"[{OperationType}] {DatabaseName}.{CollectionName} - ClusterTime: {ClusterTime:O}";
    }
}
