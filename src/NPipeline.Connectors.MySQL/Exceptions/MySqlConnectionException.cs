using NPipeline.StorageProviders.Exceptions;

namespace NPipeline.Connectors.MySql.Exceptions;

/// <summary>
///     MySQL connection exception.
/// </summary>
public class MySqlConnectionException : DatabaseConnectionException
{
    /// <summary>Initialises a new <see cref="MySqlConnectionException" /> with a message.</summary>
    public MySqlConnectionException(string message) : base(message)
    {
    }

    /// <summary>Initialises a new <see cref="MySqlConnectionException" /> with a message and inner exception.</summary>
    public MySqlConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initialises a new <see cref="MySqlConnectionException" /> with an error code.</summary>
    public MySqlConnectionException(string message, string? errorCode)
        : base(message, errorCode, null)
    {
    }

    /// <summary>Initialises a new <see cref="MySqlConnectionException" /> with an error code and inner exception.</summary>
    public MySqlConnectionException(string message, string? errorCode, Exception innerException)
        : base(message, errorCode, null, innerException)
    {
    }
}
