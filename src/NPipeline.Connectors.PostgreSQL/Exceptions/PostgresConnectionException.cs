using NPipeline.Connectors.Exceptions;

namespace NPipeline.Connectors.PostgreSQL.Exceptions;

/// <summary>
///     PostgreSQL connection exception.
/// </summary>
public class PostgresConnectionException : DatabaseConnectionException
{
    /// <summary>
    ///     Initializes a new instance of the PostgresConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public PostgresConnectionException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the PostgresConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public PostgresConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the PostgresConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The PostgreSQL error code.</param>
    public PostgresConnectionException(string message, string? errorCode)
        : base(message, errorCode, null)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the PostgresConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The PostgreSQL error code.</param>
    /// <param name="innerException">The inner exception.</param>
    public PostgresConnectionException(string message, string? errorCode, Exception innerException)
        : base(message, errorCode, null, innerException)
    {
    }
}
