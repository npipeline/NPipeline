using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.MySql.Exceptions;

/// <summary>
///     MySQL-specific exception for database operations.
/// </summary>
public class MySqlException : DatabaseException
{
    /// <summary>Initialises a new <see cref="MySqlException" /> with a message.</summary>
    public MySqlException(string message) : base(message)
    {
    }

    /// <summary>Initialises a new <see cref="MySqlException" /> with a message and inner exception.</summary>
    public MySqlException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initialises a new <see cref="MySqlException" /> with an error code.</summary>
    public MySqlException(string message, string? errorCode, bool isTransient = false)
        : base(message, errorCode, null)
    {
        IsTransient = isTransient;
    }

    /// <summary>Initialises a new <see cref="MySqlException" /> with an error code and inner exception.</summary>
    public MySqlException(string message, string? errorCode, bool isTransient, Exception innerException)
        : base(message, errorCode, null, innerException)
    {
        IsTransient = isTransient;
    }

    /// <summary>
    ///     Gets a value indicating whether the error is transient (retryable).
    /// </summary>
    public bool IsTransient { get; }
}
