using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.SqlServer.Exceptions;

/// <summary>
///     SQL Server connection exception.
/// </summary>
public class SqlServerConnectionException : DatabaseConnectionException
{
    /// <summary>
    ///     Initializes a new instance of the SqlServerConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public SqlServerConnectionException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SqlServerConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public SqlServerConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SqlServerConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The SQL Server error code.</param>
    public SqlServerConnectionException(string message, string? errorCode)
        : base(message, errorCode, null)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SqlServerConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The SQL Server error code.</param>
    /// <param name="innerException">The inner exception.</param>
    public SqlServerConnectionException(string message, string? errorCode, Exception innerException)
        : base(message, errorCode, null, innerException)
    {
    }
}
