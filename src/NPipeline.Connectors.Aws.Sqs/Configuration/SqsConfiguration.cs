using Amazon.SQS.Model;
using NPipeline.Connectors.AwsSqs.Models;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.AwsSqs.Configuration;

/// <summary>
///     Configuration for AWS SQS connector operations with enhanced acknowledgment support.
/// </summary>
public class SqsConfiguration
{
    // AWS Credentials
    /// <summary>
    ///     Gets or sets the AWS access key ID.
    /// </summary>
    public string? AccessKeyId { get; set; }

    /// <summary>
    ///     Gets or sets the AWS secret access key.
    /// </summary>
    public string? SecretAccessKey { get; set; }

    /// <summary>
    ///     Gets or sets the AWS region.
    /// </summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    ///     Gets or sets the AWS profile name (from ~/.aws/credentials).
    /// </summary>
    public string? ProfileName { get; set; }

    // Queue Configuration
    /// <summary>
    ///     Gets or sets the SQS queue URL for the source.
    /// </summary>
    public string SourceQueueUrl { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the SQS queue URL for the sink.
    /// </summary>
    public string SinkQueueUrl { get; set; } = string.Empty;

    // Polling Configuration (Source)
    /// <summary>
    ///     Gets or sets the maximum number of messages to receive in a single call (1-10).
    ///     Default is 10.
    /// </summary>
    public int MaxNumberOfMessages { get; set; } = 10;

    /// <summary>
    ///     Gets or sets the wait time for long polling in seconds (0-20).
    ///     Default is 20 (maximum for cost efficiency).
    /// </summary>
    public int WaitTimeSeconds { get; set; } = 20;

    /// <summary>
    ///     Gets or sets the visibility timeout for messages in seconds.
    ///     Default is 30.
    /// </summary>
    public int VisibilityTimeout { get; set; } = 30;

    /// <summary>
    ///     Gets or sets the polling interval when queue is empty, in milliseconds.
    ///     Default is 1000.
    /// </summary>
    public int PollingIntervalMs { get; set; } = 1000;

    // Batching Configuration (Sink)
    /// <summary>
    ///     Gets or sets the batch size for sending messages (1-10).
    ///     Default is 10.
    /// </summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>
    ///     Gets or sets the delay for sending messages, in seconds.
    ///     Default is 0.
    /// </summary>
    public int DelaySeconds { get; set; }

    // Message Attributes (Sink)
    /// <summary>
    ///     Gets or sets message attributes to add to all outgoing messages.
    /// </summary>
    public IDictionary<string, MessageAttributeValue>? MessageAttributes { get; set; }

    // JSON Serialization
    /// <summary>
    ///     Gets or sets the property naming policy for JSON serialization.
    ///     Default is JsonPropertyNamingPolicy.CamelCase.
    /// </summary>
    public JsonPropertyNamingPolicy PropertyNamingPolicy { get; set; } = JsonPropertyNamingPolicy.CamelCase;

    /// <summary>
    ///     Gets or sets a value indicating whether property name matching is case-insensitive.
    ///     Default is true.
    /// </summary>
    public bool PropertyNameCaseInsensitive { get; set; } = true;

    // Error Handling
    /// <summary>
    ///     Gets or sets the maximum number of retry attempts for transient errors.
    ///     Default is 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    ///     Gets or sets the base delay for retry backoff, in milliseconds.
    ///     Default is 1000.
    /// </summary>
    public int RetryBaseDelayMs { get; set; } = 1000;

    /// <summary>
    ///     Gets or sets a value indicating whether to continue processing on message errors.
    ///     Default is true.
    /// </summary>
    public bool ContinueOnError { get; set; } = true;

    /// <summary>
    ///     Gets or sets an optional handler invoked when a message mapping throws.
    ///     Return true to skip the message and continue; return false to fail the pipeline.
    /// </summary>
    public Func<Exception, SqsMessage<object>, bool>? MessageErrorHandler { get; set; }

    // NEW: Acknowledgment Configuration
    /// <summary>
    ///     Gets or sets the acknowledgment strategy for messages.
    ///     Default is AutoOnSinkSuccess.
    /// </summary>
    public AcknowledgmentStrategy AcknowledgmentStrategy { get; set; } = AcknowledgmentStrategy.AutoOnSinkSuccess;

    /// <summary>
    ///     Gets or sets the delay for delayed acknowledgment, in milliseconds.
    ///     Only applicable when AcknowledgmentStrategy is Delayed.
    ///     Default is 5000ms.
    /// </summary>
    public int AcknowledgmentDelayMs { get; set; } = 5000;

    /// <summary>
    ///     Gets or sets the batch acknowledgment options.
    ///     When null, default batch options are used.
    /// </summary>
    public BatchAcknowledgmentOptions? BatchAcknowledgment { get; set; }

    // NEW: Connection Pooling Configuration
    /// <summary>
    ///     Gets or sets the maximum number of SQS client connections to pool.
    ///     Default is 10.
    /// </summary>
    public int MaxConnectionPoolSize { get; set; } = 10;

    // NEW: Parallel Processing Configuration
    /// <summary>
    ///     Gets or sets the maximum degree of parallelism for message processing.
    ///     Default is 1 (sequential processing).
    /// </summary>
    public int MaxDegreeOfParallelism { get; set; } = 1;

    /// <summary>
    ///     Gets or sets a value indicating whether to enable parallel message processing.
    ///     Default is false.
    /// </summary>
    public bool EnableParallelProcessing { get; set; }

    /// <summary>
    ///     Validates the configuration and throws if invalid.
    /// </summary>
    public virtual void Validate()
    {
        ValidateSource();
        ValidateSink();
    }

    /// <summary>
    ///     Validates configuration for source node usage.
    /// </summary>
    public virtual void ValidateSource()
    {
        if (string.IsNullOrWhiteSpace(SourceQueueUrl))
            throw new InvalidOperationException("SourceQueueUrl must be specified.");

        ValidateCommon();
    }

    /// <summary>
    ///     Validates configuration for sink node usage.
    /// </summary>
    public virtual void ValidateSink()
    {
        if (string.IsNullOrWhiteSpace(SinkQueueUrl))
            throw new InvalidOperationException("SinkQueueUrl must be specified.");

        if (AcknowledgmentStrategy is AcknowledgmentStrategy.AutoOnSinkSuccess or AcknowledgmentStrategy.Delayed &&
            string.IsNullOrWhiteSpace(SourceQueueUrl))
            throw new InvalidOperationException("SourceQueueUrl must be specified when using automatic acknowledgment.");

        ValidateCommon();
    }

    private void ValidateCommon()
    {
        if (MaxNumberOfMessages is < 1 or > 10)
            throw new InvalidOperationException("MaxNumberOfMessages must be between 1 and 10.");

        if (WaitTimeSeconds is < 0 or > 20)
            throw new InvalidOperationException("WaitTimeSeconds must be between 0 and 20.");

        if (VisibilityTimeout is < 0 or > 43200)
            throw new InvalidOperationException("VisibilityTimeout must be between 0 and 43200 (12 hours).");

        if (BatchSize is < 1 or > 10)
            throw new InvalidOperationException("BatchSize must be between 1 and 10.");

        if (DelaySeconds is < 0 or > 900)
            throw new InvalidOperationException("DelaySeconds must be between 0 and 900 (15 minutes).");

        // Validate AWS credentials (at least one method must be provided)
        if (string.IsNullOrWhiteSpace(AccessKeyId) &&
            string.IsNullOrWhiteSpace(SecretAccessKey) &&
            string.IsNullOrWhiteSpace(ProfileName))
        {
            // Allow default credential chain (environment variables, EC2 role, etc.)
        }

        // Validate acknowledgment configuration
        if (AcknowledgmentStrategy == AcknowledgmentStrategy.Delayed && AcknowledgmentDelayMs < 0)
            throw new InvalidOperationException("AcknowledgmentDelayMs must be non-negative when using Delayed strategy.");

        // Validate batch acknowledgment options if provided
        BatchAcknowledgment?.Validate();

        // Validate connection pooling
        if (MaxConnectionPoolSize < 1)
            throw new InvalidOperationException("MaxConnectionPoolSize must be at least 1.");

        // Validate parallel processing
        if (MaxDegreeOfParallelism < 1)
            throw new InvalidOperationException("MaxDegreeOfParallelism must be at least 1.");

        if (EnableParallelProcessing && MaxDegreeOfParallelism < 2)
        {
            throw new InvalidOperationException(
                "MaxDegreeOfParallelism must be at least 2 when EnableParallelProcessing is true.");
        }
    }
}
