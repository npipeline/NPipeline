using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace NPipeline.Connectors.RabbitMQ;

/// <summary>
///     Source-generated high-performance logging methods for the RabbitMQ connector.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class LogMessages
{
    // Source

    [LoggerMessage(1, LogLevel.Information,
        "RabbitMQ consumer started on queue '{QueueName}' with prefetch {PrefetchCount}")]
    public static partial void ConsumerStarted(ILogger logger, string queueName, ushort prefetchCount);

    [LoggerMessage(2, LogLevel.Debug,
        "Consumed message {DeliveryTag} from queue '{QueueName}'")]
    public static partial void MessageConsumed(ILogger logger, ulong deliveryTag, string queueName);

    [LoggerMessage(3, LogLevel.Warning,
        "Deserialization failed for message {DeliveryTag} from queue '{QueueName}'")]
    public static partial void DeserializationFailed(ILogger logger, Exception exception, ulong deliveryTag, string queueName);

    [LoggerMessage(4, LogLevel.Error,
        "Consumer channel shutdown on queue '{QueueName}': {Reason}")]
    public static partial void ConsumerShutdown(ILogger logger, string queueName, string reason);

    [LoggerMessage(5, LogLevel.Warning,
        "Poison message detected (delivery tag {DeliveryTag}, attempt {AttemptCount}/{MaxAttempts}) — rejecting without requeue")]
    public static partial void PoisonMessageRejected(ILogger logger, ulong deliveryTag, int attemptCount, int maxAttempts);

    // Sink

    [LoggerMessage(10, LogLevel.Debug,
        "Published message to exchange '{Exchange}' with routing key '{RoutingKey}'")]
    public static partial void MessagePublished(ILogger logger, string exchange, string routingKey);

    [LoggerMessage(11, LogLevel.Warning,
        "Message returned from exchange '{Exchange}' with routing key '{RoutingKey}': {ReplyText}")]
    public static partial void MessageReturned(ILogger logger, string exchange, string routingKey, string replyText);

    [LoggerMessage(12, LogLevel.Error,
        "Publish failed to exchange '{Exchange}': {Error}")]
    public static partial void PublishFailed(ILogger logger, Exception exception, string exchange, string error);

    [LoggerMessage(13, LogLevel.Information,
        "Batch published {Count} messages to exchange '{Exchange}'")]
    public static partial void BatchPublished(ILogger logger, int count, string exchange);

    [LoggerMessage(14, LogLevel.Warning,
        "Publish retrying in {DelayMs}ms (attempt {Attempt}/{MaxRetries}) to exchange '{Exchange}'")]
    public static partial void PublishRetrying(ILogger logger, int delayMs, int attempt, int maxRetries, string exchange);

    // Connection

    [LoggerMessage(20, LogLevel.Information,
        "RabbitMQ connection established to {HostName}:{Port}/{VirtualHost}")]
    public static partial void ConnectionEstablished(ILogger logger, string hostName, int port, string virtualHost);

    [LoggerMessage(21, LogLevel.Warning,
        "RabbitMQ connection recovery initiated")]
    public static partial void ConnectionRecovery(ILogger logger);

    [LoggerMessage(22, LogLevel.Debug, "RabbitMQ channel created")]
    public static partial void ChannelCreated(ILogger logger);

    [LoggerMessage(23, LogLevel.Debug, "RabbitMQ channel closed")]
    public static partial void ChannelClosed(ILogger logger);

    // Topology

    [LoggerMessage(30, LogLevel.Information,
        "Declared queue '{QueueName}' (type: {QueueType}, durable: {Durable})")]
    public static partial void QueueDeclared(ILogger logger, string queueName, string queueType, bool durable);

    [LoggerMessage(31, LogLevel.Information,
        "Declared exchange '{ExchangeName}' (type: {ExchangeType}, durable: {Durable})")]
    public static partial void ExchangeDeclared(ILogger logger, string exchangeName, string exchangeType, bool durable);

    // Dead letter

    [LoggerMessage(40, LogLevel.Warning,
        "Publishing failed item to dead-letter exchange '{Exchange}' from node '{NodeId}'")]
    public static partial void DeadLetterPublishing(ILogger logger, string exchange, string nodeId);

    [LoggerMessage(41, LogLevel.Error,
        "Failed to publish dead-letter message to exchange '{Exchange}'")]
    public static partial void DeadLetterPublishFailed(ILogger logger, Exception exception, string exchange);
}
