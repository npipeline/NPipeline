namespace NPipeline.Connectors.Kafka.Models;

/// <summary>
///     Envelope for dead-letter messages containing the original item and error details.
/// </summary>
public sealed class DeadLetterEnvelope
{
    /// <summary>
    ///     Gets or sets the ID of the node that failed.
    /// </summary>
    public string NodeId { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the original item that failed processing.
    /// </summary>
    public object? OriginalItem { get; set; }

    /// <summary>
    ///     Gets or sets the full type name of the exception.
    /// </summary>
    public string? ExceptionType { get; set; }

    /// <summary>
    ///     Gets or sets the exception message.
    /// </summary>
    public string? ExceptionMessage { get; set; }

    /// <summary>
    ///     Gets or sets the exception stack trace.
    /// </summary>
    public string? StackTrace { get; set; }

    /// <summary>
    ///     Gets or sets the UTC timestamp when the failure occurred.
    /// </summary>
    public DateTime Timestamp { get; set; }

    /// <summary>
    ///     Gets or sets the pipeline correlation ID.
    /// </summary>
    public string? CorrelationId { get; set; }

    // Kafka-specific metadata (optional - null if original item is not from Kafka)

    /// <summary>
    ///     Gets or sets the original Kafka topic (null if not from Kafka).
    /// </summary>
    public string? OriginalTopic { get; set; }

    /// <summary>
    ///     Gets or sets the Kafka partition (null if not from Kafka).
    /// </summary>
    public int? Partition { get; set; }

    /// <summary>
    ///     Gets or sets the Kafka offset (null if not from Kafka).
    /// </summary>
    public long? Offset { get; set; }
}
