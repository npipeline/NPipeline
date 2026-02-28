using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.Snowflake.Exceptions;

/// <summary>
///     Snowflake connection exception.
/// </summary>
public class SnowflakeConnectionException : DatabaseConnectionException
{
    /// <summary>
    ///     Initializes a new instance of the SnowflakeConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public SnowflakeConnectionException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SnowflakeConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public SnowflakeConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SnowflakeConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The Snowflake error code.</param>
    public SnowflakeConnectionException(string message, string? errorCode)
        : base(message, errorCode, null)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SnowflakeConnectionException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The Snowflake error code.</param>
    /// <param name="innerException">The inner exception.</param>
    public SnowflakeConnectionException(string message, string? errorCode, Exception innerException)
        : base(message, errorCode, null, innerException)
    {
    }
}
