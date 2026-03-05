using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace NPipeline.Connectors.Azure.ServiceBus;

[ExcludeFromCodeCoverage]
internal static partial class ServiceBusLogMessages
{
    [LoggerMessage(1, LogLevel.Information, "Service Bus processor started for entity '{EntityName}'")]
    public static partial void ProcessorStarted(ILogger logger, string entityName);

    [LoggerMessage(2, LogLevel.Information, "Service Bus processor stopped for entity '{EntityName}'")]
    public static partial void ProcessorStopped(ILogger logger, string entityName);

    [LoggerMessage(3, LogLevel.Warning, "Service Bus processor failed to stop gracefully")]
    public static partial void ProcessorStopFailed(ILogger logger, Exception exception);

    [LoggerMessage(4, LogLevel.Error, "Service Bus processor error on entity '{EntityPath}', source: {ErrorSource}")]
    public static partial void ProcessorError(ILogger logger, Exception exception,
        string entityPath, string errorSource);

    [LoggerMessage(5, LogLevel.Warning, "Failed to deserialize message '{MessageId}' from entity '{EntityName}'")]
    public static partial void DeserializationFailed(ILogger logger, Exception exception,
        string messageId, string entityName);

    [LoggerMessage(6, LogLevel.Debug, "Message '{MessageId}' written to internal channel")]
    public static partial void MessageEnqueued(ILogger logger, string messageId);

    [LoggerMessage(7, LogLevel.Debug, "Sending {Count} message(s) to entity '{EntityName}'")]
    public static partial void SendingMessages(ILogger logger, int count, string entityName);

    [LoggerMessage(8, LogLevel.Debug, "Successfully sent {Count} message(s) to entity '{EntityName}'")]
    public static partial void MessagesSent(ILogger logger, int count, string entityName);

    [LoggerMessage(9, LogLevel.Warning, "Failed to send messages to entity '{EntityName}'. ContinueOnError is enabled.")]
    public static partial void SendFailed(ILogger logger, Exception exception, string entityName);

    [LoggerMessage(10, LogLevel.Warning, "Message too large to fit in batch for entity '{EntityName}'; attempting individual send.")]
    public static partial void MessageTooLargeForBatch(ILogger logger, string entityName);

    [LoggerMessage(11, LogLevel.Information, "Session processor started for entity '{EntityName}'")]
    public static partial void SessionProcessorStarted(ILogger logger, string entityName);

    [LoggerMessage(12, LogLevel.Warning, "Session processor error on entity '{EntityPath}', source: {ErrorSource}")]
    public static partial void SessionProcessorError(ILogger logger, Exception exception,
        string entityPath, string errorSource);
}
