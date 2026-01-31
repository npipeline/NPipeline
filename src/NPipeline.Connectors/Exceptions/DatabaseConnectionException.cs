namespace NPipeline.Connectors.Exceptions;

/// <summary>
///     Exception thrown when a database connection error occurs.
/// </summary>
public class DatabaseConnectionException : DatabaseExceptionBase
{
    /// <summary>
    ///     Initializes a new instance of the DatabaseConnectionException.
    /// </summary>
    /// <param name="message">The error message.</param>
    public DatabaseConnectionException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseConnectionException with an inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public DatabaseConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseConnectionException with error code and SQL state.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The database error code.</param>
    /// <param name="sqlState">The SQL state.</param>
    public DatabaseConnectionException(string message, string? errorCode, int? sqlState)
        : base(message, errorCode, sqlState)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseConnectionException with error code, SQL state, and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The database error code.</param>
    /// <param name="sqlState">The SQL state.</param>
    /// <param name="innerException">The inner exception.</param>
    public DatabaseConnectionException(string message, string? errorCode, int? sqlState, Exception innerException)
        : base(message, errorCode, sqlState, innerException)
    {
    }
}
