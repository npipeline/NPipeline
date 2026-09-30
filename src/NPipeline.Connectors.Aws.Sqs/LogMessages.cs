using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace NPipeline.Connectors.Aws.Sqs;

[ExcludeFromCodeCoverage]
internal static partial class SqsLogMessages
{
    [LoggerMessage(1, LogLevel.Warning, "SQS could not delete an acknowledged message ({Code}: {Message}); it will be delivered again.")]
    public static partial void DeleteFailed(ILogger logger, string code, string message);

    [LoggerMessage(2, LogLevel.Warning, "SQS could not delete {Count} acknowledged messages; they will be delivered again.")]
    public static partial void DeleteBatchFailed(ILogger logger, Exception exception, int count);

    [LoggerMessage(3, LogLevel.Warning, "The body of message {MessageId} from {QueueUrl} does not deserialize.")]
    public static partial void DeserializationFailed(ILogger logger, Exception exception, string messageId, string queueUrl);

    [LoggerMessage(4, LogLevel.Error, "SQS rejected message {Index} of a batch sent to {QueueUrl} ({Code}: {Message}).")]
    public static partial void SendFailed(ILogger logger, int index, string queueUrl, string code, string message);
}
