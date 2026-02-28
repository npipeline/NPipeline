using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.Snowflake.Exceptions;

/// <summary>
///     Snowflake-specific exception for database operations.
/// </summary>
public class SnowflakeException : DatabaseException
{
    /// <summary>
    ///     Initializes a new instance of the SnowflakeException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    public SnowflakeException(string message)
        : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SnowflakeException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public SnowflakeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SnowflakeException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The Snowflake error code.</param>
    /// <param name="isTransient">Whether the error is transient.</param>
    public SnowflakeException(string message, string? errorCode, bool isTransient = false)
        : base(message, errorCode, null)
    {
        IsTransient = isTransient;
    }

    /// <summary>
    ///     Initializes a new instance of the SnowflakeException class.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="errorCode">The Snowflake error code.</param>
    /// <param name="isTransient">Whether the error is transient.</param>
    /// <param name="innerException">The inner exception.</param>
    public SnowflakeException(string message, string? errorCode, bool isTransient, Exception innerException)
        : base(message, errorCode, null, innerException)
    {
        IsTransient = isTransient;
    }

    /// <summary>
    ///     Gets a value indicating whether the error is considered transient (retryable).
    /// </summary>
    public bool IsTransient { get; }
}
