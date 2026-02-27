namespace NPipeline.Connectors.Configuration;

/// <summary>
///     Configuration options for batch acknowledgment.
/// </summary>
public class BatchAcknowledgmentOptions
{
    /// <summary>
    ///     Gets or sets the maximum number of messages to acknowledge in a single batch operation.
    ///     Default is 10 (SQS maximum).
    /// </summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>
    ///     Gets or sets the maximum allowed batch size for validation.
    ///     Connectors override this to match their backend's limits.
    ///     Default is 10 (backward-compatible with SQS). RabbitMQ sets 10,000.
    /// </summary>
    public int MaxBatchSize { get; set; } = 10;

    /// <summary>
    ///     Gets or sets the maximum time to wait before flushing a partial batch, in milliseconds.
    ///     Default is 1000ms.
    /// </summary>
    public int FlushTimeoutMs { get; set; } = 1000;

    /// <summary>
    ///     Gets or sets a value indicating whether to enable automatic batch acknowledgment.
    ///     When true, messages are accumulated and acknowledged in batches.
    ///     When false, messages are acknowledged individually or via explicit batch calls.
    ///     Default is true.
    /// </summary>
    public bool EnableAutomaticBatching { get; set; } = true;

    /// <summary>
    ///     Gets or sets the maximum number of concurrent batch acknowledgment operations.
    ///     Default is 3.
    /// </summary>
    public int MaxConcurrentBatches { get; set; } = 3;

    /// <summary>
    ///     Validates the batch acknowledgment options.
    /// </summary>
    public void Validate()
    {
        if (BatchSize < 1 || BatchSize > MaxBatchSize)
            throw new InvalidOperationException(
                $"BatchSize must be between 1 and {MaxBatchSize}.");

        if (FlushTimeoutMs < 0)
            throw new InvalidOperationException("FlushTimeoutMs must be non-negative.");

        if (MaxConcurrentBatches < 1)
            throw new InvalidOperationException("MaxConcurrentBatches must be at least 1.");
    }
}
