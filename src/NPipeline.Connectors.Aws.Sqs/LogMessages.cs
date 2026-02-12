using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace NPipeline.Connectors.Aws.Sqs;

/// <summary>
///     Source-generated logging methods for SQS sink node operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class SqsSinkNodeLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Processing IAcknowledgableMessage, MessageType={MessageType}")]
    public static partial void ProcessingAcknowledgableMessage(ILogger logger, string? messageType);

    [LoggerMessage(2, LogLevel.Debug, "Processing regular message, MessageType={MessageType}")]
    public static partial void ProcessingRegularMessage(ILogger logger, string? messageType);

    [LoggerMessage(3, LogLevel.Debug, "Sending message, ItemType={ItemType}")]
    public static partial void SendingMessage(ILogger logger, string itemType);

    [LoggerMessage(4, LogLevel.Debug, "Message sent successfully")]
    public static partial void MessageSent(ILogger logger);

    [LoggerMessage(5, LogLevel.Warning, "Failed to send message to SQS. Continuing due to ContinueOnError setting.")]
    public static partial void SendMessageFailed(ILogger logger, Exception exception);

    [LoggerMessage(6, LogLevel.Warning, "Failed to serialize message for batch. Skipping.")]
    public static partial void BatchSerializationFailed(ILogger logger, Exception exception);

    [LoggerMessage(7, LogLevel.Warning, "Failed to send message {Id} to SQS: {Message}")]
    public static partial void BatchMessageFailed(ILogger logger, string id, string message);

    [LoggerMessage(8, LogLevel.Warning, "Failed to send message batch to SQS. Continuing due to ContinueOnError setting.")]
    public static partial void SendMessageBatchFailed(ILogger logger, Exception exception);

    [LoggerMessage(9, LogLevel.Warning, "One or more delayed acknowledgment tasks failed during disposal")]
    public static partial void DelayedAcknowledgmentFailed(ILogger logger, Exception exception);

    [LoggerMessage(10, LogLevel.Error, "Error flushing acknowledgment batch on dispose")]
    public static partial void FlushBatchOnDisposeFailed(ILogger logger, Exception exception);

    [LoggerMessage(11, LogLevel.Error, "Error flushing acknowledgment batch")]
    public static partial void FlushBatchFailed(ILogger logger, Exception exception);

    [LoggerMessage(12, LogLevel.Warning, "Failed to delete message {MessageId}: {ErrorMessage}")]
    public static partial void DeleteMessageFailed(ILogger logger, string messageId, string errorMessage);
}
