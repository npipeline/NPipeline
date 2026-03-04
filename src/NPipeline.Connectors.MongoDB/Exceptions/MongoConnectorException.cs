using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.MongoDB.Exceptions;

/// <summary>
///     MongoDB-specific exception for database operations.
/// </summary>
public class MongoConnectorException : DatabaseException
{
    /// <summary>
    ///     Initializes a new instance of the MongoConnectorException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public MongoConnectorException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the MongoConnectorException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public MongoConnectorException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the MongoConnectorException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="collectionName">The collection name where the error occurred.</param>
    /// <param name="operationContext">The operation context (e.g., "Read", "Write", "Update").</param>
    public MongoConnectorException(string message, string? collectionName, string? operationContext)
        : base(message)
    {
        CollectionName = collectionName;
        OperationContext = operationContext;
    }

    /// <summary>
    ///     Initializes a new instance of the MongoConnectorException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="collectionName">The collection name where the error occurred.</param>
    /// <param name="operationContext">The operation context (e.g., "Read", "Write", "Update").</param>
    /// <param name="innerException">The inner exception.</param>
    public MongoConnectorException(string message, string? collectionName, string? operationContext, Exception innerException)
        : base(message, innerException)
    {
        CollectionName = collectionName;
        OperationContext = operationContext;
    }

    /// <summary>
    ///     Gets the collection name where the error occurred.
    /// </summary>
    public string? CollectionName { get; }

    /// <summary>
    ///     Gets the operation context (e.g., "Read", "Write", "Update").
    /// </summary>
    public string? OperationContext { get; }

    /// <summary>
    ///     Gets a value indicating whether the error is considered transient.
    /// </summary>
    public bool IsTransient { get; init; }
}
