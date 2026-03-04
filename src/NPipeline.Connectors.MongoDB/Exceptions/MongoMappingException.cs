using MongoDB.Bson;
using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.MongoDB.Exceptions;

/// <summary>
///     Exception thrown when a MongoDB document cannot be mapped to a CLR type.
/// </summary>
public class MongoMappingException : DatabaseMappingException
{
    /// <summary>
    ///     Initializes a new instance of the MongoMappingException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public MongoMappingException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the MongoMappingException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public MongoMappingException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the MongoMappingException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="fieldName">The field name that caused the error.</param>
    public MongoMappingException(string message, string? fieldName)
        : base(message, fieldName)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the MongoMappingException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="fieldName">The field name that caused the error.</param>
    /// <param name="innerException">The inner exception.</param>
    public MongoMappingException(string message, string? fieldName, Exception innerException)
        : base(message, fieldName, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the MongoMappingException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="fieldName">The field name that caused the error.</param>
    /// <param name="offendingDocument">The BSON document that could not be mapped.</param>
    public MongoMappingException(string message, string? fieldName, BsonDocument? offendingDocument)
        : base(message, fieldName)
    {
        OffendingDocument = offendingDocument;
    }

    /// <summary>
    ///     Initializes a new instance of the MongoMappingException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="fieldName">The field name that caused the error.</param>
    /// <param name="offendingDocument">The BSON document that could not be mapped.</param>
    /// <param name="innerException">The inner exception.</param>
    public MongoMappingException(string message, string? fieldName, BsonDocument? offendingDocument, Exception innerException)
        : base(message, fieldName, innerException)
    {
        OffendingDocument = offendingDocument;
    }

    /// <summary>
    ///     Gets the BSON document that could not be mapped.
    /// </summary>
    public BsonDocument? OffendingDocument { get; }
}
