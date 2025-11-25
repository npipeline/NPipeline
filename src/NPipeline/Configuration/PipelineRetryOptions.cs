using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Configuration;

/// <summary>
///     Configurable retry / resilience parameters controlling per-item retries and node restarts.
/// </summary>
public sealed record PipelineRetryOptions(
    int MaxItemRetries,
    int MaxNodeRestartAttempts,
    int MaxSequentialNodeAttempts,
    int? MaxMaterializedItems = null, // Null => unbounded (no cap)
    RetryDelayStrategyConfiguration? DelayStrategyConfiguration = null)
{
    /// <summary>
    ///     Default options: item retries = 0 (no retry), node restarts = 3, sequential node attempts = 5.
    /// </summary>
    public static PipelineRetryOptions Default { get; } = new(0, 3, 5);

    /// <summary>
    ///     Creates a new instance with updated options, preserving unspecified values.
    /// </summary>
    public PipelineRetryOptions With(
        int? maxItemRetries = null,
        int? maxNodeRestartAttempts = null,
        int? maxSequentialNodeAttempts = null,
        int? maxMaterializedItems = null,
        RetryDelayStrategyConfiguration? delayStrategyConfiguration = null)
    {
        return new PipelineRetryOptions(
            maxItemRetries ?? MaxItemRetries,
            maxNodeRestartAttempts ?? MaxNodeRestartAttempts,
            maxSequentialNodeAttempts ?? MaxSequentialNodeAttempts,
            maxMaterializedItems ?? MaxMaterializedItems,
            delayStrategyConfiguration ?? DelayStrategyConfiguration);
    }
}
