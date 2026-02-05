namespace NPipeline.StorageProviders.Exceptions;

/// <summary>
///     Generic database exception for general database errors.
/// </summary>
public class DatabaseException : DatabaseExceptionBase
{
    /// <summary>
    ///     Initializes a new instance of the DatabaseException.
    /// </summary>
    /// <param name="message">The error message.</param>
    public DatabaseException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseException with an inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public DatabaseException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseException with error code and SQL state.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The database error code.</param>
    /// <param name="sqlState">The SQL state.</param>
    public DatabaseException(string message, string? errorCode, int? sqlState)
        : base(message, errorCode, sqlState)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseException with error code, SQL state, and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The database error code.</param>
    /// <param name="sqlState">The SQL state.</param>
    /// <param name="innerException">The inner exception.</param>
    public DatabaseException(string message, string? errorCode, int? sqlState, Exception innerException)
        : base(message, errorCode, sqlState, innerException)
    {
    }
}
