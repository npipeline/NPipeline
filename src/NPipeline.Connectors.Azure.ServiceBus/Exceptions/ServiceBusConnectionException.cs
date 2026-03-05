namespace NPipeline.Connectors.Azure.ServiceBus.Exceptions;

/// <summary>
///     Exception thrown when a connection to Azure Service Bus cannot be established.
/// </summary>
public sealed class ServiceBusConnectionException : Exception
{
    /// <summary>
    ///     Initializes a new instance of <see cref="ServiceBusConnectionException" />.
    /// </summary>
    public ServiceBusConnectionException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="ServiceBusConnectionException" /> with an inner exception.
    /// </summary>
    public ServiceBusConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
