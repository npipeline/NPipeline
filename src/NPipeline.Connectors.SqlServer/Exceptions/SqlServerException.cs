using NPipeline.Connectors.Exceptions;

namespace NPipeline.Connectors.SqlServer.Exceptions;

/// <summary>
///     SQL Server-specific exception for database operations.
/// </summary>
public class SqlServerException : DatabaseException
{
    /// <summary>
    ///     Initializes a new instance of the SqlServerException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public SqlServerException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SqlServerException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public SqlServerException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SqlServerException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The SQL Server error code.</param>
    /// <param name="isTransient">Whether the error is transient.</param>
    public SqlServerException(string message, string? errorCode, bool isTransient = false)
        : base(message, errorCode, null)
    {
        IsTransient = isTransient;
    }

    /// <summary>
    ///     Initializes a new instance of the SqlServerException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The SQL Server error code.</param>
    /// <param name="isTransient">Whether the error is transient.</param>
    /// <param name="innerException">The inner exception.</param>
    public SqlServerException(string message, string? errorCode, bool isTransient, Exception innerException)
        : base(message, errorCode, null, innerException)
    {
        IsTransient = isTransient;
    }

    /// <summary>
    ///     Gets a value indicating whether the error is considered transient (retryable).
    /// </summary>
    public bool IsTransient { get; }
}
