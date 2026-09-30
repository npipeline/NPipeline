using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace NPipeline.Connectors.Azure.ServiceBus;

[ExcludeFromCodeCoverage]
internal static partial class ServiceBusLogMessages
{
    [LoggerMessage(1, LogLevel.Information, "Receiving from Service Bus entity '{EntityName}'")]
    public static partial void ReceiverStarted(ILogger logger, string entityName);

    [LoggerMessage(5, LogLevel.Warning, "The body of message '{MessageId}' from entity '{EntityName}' does not deserialize")]
    public static partial void DeserializationFailed(ILogger logger, Exception exception, string messageId, string entityName);

    [LoggerMessage(9, LogLevel.Error, "Service Bus could not send {Count} message(s) to entity '{EntityName}'")]
    public static partial void SendFailed(ILogger logger, Exception exception, int count, string entityName);

    [LoggerMessage(10, LogLevel.Warning, "Renewing a Service Bus lock failed; the message will be delivered again")]
    public static partial void LockRenewalFailed(ILogger logger, Exception exception);

    [LoggerMessage(11, LogLevel.Information, "Accepted Service Bus session '{SessionId}' on entity '{EntityName}'")]
    public static partial void SessionAccepted(ILogger logger, string sessionId, string entityName);

    [LoggerMessage(12, LogLevel.Warning, "Closing a Service Bus receiver failed")]
    public static partial void CloseFailed(ILogger logger, Exception exception);
}
