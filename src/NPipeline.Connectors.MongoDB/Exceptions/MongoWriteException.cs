using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.MongoDB.Exceptions;

/// <summary>
///     Exception thrown when a MongoDB batch write operation fails.
/// </summary>
public class MongoWriteException : DatabaseException
{
    /// <summary>
    ///     Initializes a new instance of the MongoWriteException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public MongoWriteException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the MongoWriteException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public MongoWriteException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the MongoWriteException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="collectionName">The collection name where the error occurred.</param>
    /// <param name="failedBatchCount">The number of documents in the failed batch.</param>
    public MongoWriteException(string message, string? collectionName, int failedBatchCount)
        : base(message)
    {
        CollectionName = collectionName;
        FailedBatchCount = failedBatchCount;
    }

    /// <summary>
    ///     Initializes a new instance of the MongoWriteException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="collectionName">The collection name where the error occurred.</param>
    /// <param name="failedBatchCount">The number of documents in the failed batch.</param>
    /// <param name="innerException">The inner exception.</param>
    public MongoWriteException(string message, string? collectionName, int failedBatchCount, Exception innerException)
        : base(message, innerException)
    {
        CollectionName = collectionName;
        FailedBatchCount = failedBatchCount;
    }

    /// <summary>
    ///     Gets the collection name where the write error occurred.
    /// </summary>
    public string? CollectionName { get; }

    /// <summary>
    ///     Gets the number of documents in the failed batch.
    /// </summary>
    public int FailedBatchCount { get; }

    /// <summary>
    ///     Gets the number of documents that were successfully written before the failure.
    /// </summary>
    public int SuccessfullyWrittenCount { get; init; }

    /// <summary>
    ///     Gets the write error code from MongoDB, if available.
    /// </summary>
    public int? WriteErrorCode { get; init; }
}
