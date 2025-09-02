namespace NPipeline.Connectors.Exceptions;

/// <summary>
///     Base exception type for connector-related failures.
/// </summary>
public class ConnectorException : Exception
{
    public ConnectorException()
    {
    }

    public ConnectorException(string message) : base(message)
    {
    }

    public ConnectorException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
