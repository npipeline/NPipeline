namespace NPipeline.StorageProviders.Exceptions;

/// <summary>
///     Base exception type for connector-related failures.
/// </summary>
public class ConnectorException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ConnectorException" /> class.
    /// </summary>
    public ConnectorException()
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConnectorException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public ConnectorException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConnectorException" /> class with a specified error message and a reference to the inner exception that is the
    ///     cause of this exception.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    /// <param name="innerException">The exception that is the cause of the current exception, or a null reference if no inner exception is specified.</param>
    public ConnectorException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
