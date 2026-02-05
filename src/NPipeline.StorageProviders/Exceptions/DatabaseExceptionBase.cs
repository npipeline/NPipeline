namespace NPipeline.StorageProviders.Exceptions;

/// <summary>
///     Base exception class for database-related errors.
///     Designed to be inherited by database-specific exception classes.
/// </summary>
public abstract class DatabaseExceptionBase : Exception
{
    /// <summary>
    ///     Initializes a new instance of the DatabaseExceptionBase.
    /// </summary>
    /// <param name="message">The error message.</param>
    protected DatabaseExceptionBase(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseExceptionBase with an inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    protected DatabaseExceptionBase(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseExceptionBase with error code and SQL state.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The database error code.</param>
    /// <param name="sqlState">The SQL state.</param>
    protected DatabaseExceptionBase(string message, string? errorCode, int? sqlState)
        : base(message)
    {
        ErrorCode = errorCode;
        SqlState = sqlState;
    }

    /// <summary>
    ///     Initializes a new instance of the DatabaseExceptionBase with error code, SQL state, and inner exception.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The database error code.</param>
    /// <param name="sqlState">The SQL state.</param>
    /// <param name="innerException">The inner exception.</param>
    protected DatabaseExceptionBase(string message, string? errorCode, int? sqlState, Exception innerException)
        : base(message, innerException)
    {
        ErrorCode = errorCode;
        SqlState = sqlState;
    }

    /// <summary>
    ///     Gets the error code associated with this exception.
    /// </summary>
    public string? ErrorCode { get; }

    /// <summary>
    ///     Gets the SQL state associated with this exception.
    /// </summary>
    public int? SqlState { get; }
}