namespace NPipeline.Connectors.Configuration
{
    /// <summary>
    /// Configuration options for batch acknowledgment.
    /// </summary>
    public class BatchAcknowledgmentOptions
    {
        /// <summary>
        /// Gets or sets the maximum number of messages to acknowledge in a single batch operation.
        /// Default is 10 (SQS maximum).
        /// </summary>
        public int BatchSize { get; set; } = 10;

        /// <summary>
        /// Gets or sets the maximum time to wait before flushing a partial batch, in milliseconds.
        /// Default is 1000ms.
        /// </summary>
        public int FlushTimeoutMs { get; set; } = 1000;

        /// <summary>
        /// Gets or sets a value indicating whether to enable automatic batch acknowledgment.
        /// When true, messages are accumulated and acknowledged in batches.
        /// When false, messages are acknowledged individually or via explicit batch calls.
        /// Default is true.
        /// </summary>
        public bool EnableAutomaticBatching { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum number of concurrent batch acknowledgment operations.
        /// Default is 3.
        /// </summary>
        public int MaxConcurrentBatches { get; set; } = 3;

        /// <summary>
        /// Validates the batch acknowledgment options.
        /// </summary>
        public void Validate()
        {
            if (BatchSize < 1 || BatchSize > 10)
                throw new InvalidOperationException("BatchSize must be between 1 and 10.");

            if (FlushTimeoutMs < 0)
                throw new InvalidOperationException("FlushTimeoutMs must be non-negative.");

            if (MaxConcurrentBatches < 1)
                throw new InvalidOperationException("MaxConcurrentBatches must be at least 1.");
        }
    }
}
