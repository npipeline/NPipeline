namespace NPipeline.Connectors.Exceptions;

/// <summary>
/// Exception thrown when a database operation error occurs.
/// </summary>
public class DatabaseOperationException : DatabaseExceptionBase
{
    /// <summary>
    /// Initializes a new instance of the DatabaseOperationException.
    /// </summary>
    /// <param name="message">The error message.</param>
    public DatabaseOperationException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the DatabaseOperationException with an inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public DatabaseOperationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance of the DatabaseOperationException with error code and SQL state.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The database error code.</param>
    /// <param name="sqlState">The SQL state.</param>
    public DatabaseOperationException(string message, string? errorCode, int? sqlState)
        : base(message, errorCode, sqlState)
    {
    }

    /// <summary>
    /// Initializes a new instance of the DatabaseOperationException with error code, SQL state, and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The database error code.</param>
    /// <param name="sqlState">The SQL state.</param>
    /// <param name="innerException">The inner exception.</param>
    public DatabaseOperationException(string message, string? errorCode, int? sqlState, Exception innerException)
        : base(message, errorCode, sqlState, innerException)
    {
    }
}
