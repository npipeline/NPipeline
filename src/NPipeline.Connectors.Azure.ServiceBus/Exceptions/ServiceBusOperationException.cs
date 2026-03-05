namespace NPipeline.Connectors.Azure.ServiceBus.Exceptions;

/// <summary>
///     Exception thrown when an Azure Service Bus send or receive operation fails.
/// </summary>
public sealed class ServiceBusOperationException : Exception
{
    /// <summary>
    ///     Initializes a new instance of <see cref="ServiceBusOperationException" />.
    /// </summary>
    public ServiceBusOperationException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of <see cref="ServiceBusOperationException" /> with an inner exception.
    /// </summary>
    public ServiceBusOperationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    ///     Gets the name of the queue or topic involved in the failed operation, if known.
    /// </summary>
    public string? EntityName { get; init; }
}
