using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Configuration;

/// <summary>
///     Configurable retry / resilience parameters controlling per-item retries and node restarts.
/// </summary>
/// <param name="MaxItemRetries">
///     Maximum number of times an individual item will be retried before being sent to dead letter queue.
///     Default is 0 (no retries).
/// </param>
/// <param name="MaxMaterializedItems">
///     Optional cap on the number of items to materialize for retry scenarios.
///     Null means unbounded (no cap). Default is null.
/// </param>
/// <param name="DelayStrategyConfiguration">
///     Configuration for retry delay strategy (exponential backoff, fixed delay, etc.).
///     Default is null (no delay between retries).
/// </param>
/// <param name="MaxNodeRestartAttempts">
///     Maximum number of times a node will be restarted after failure.
///     Default is 3.
/// </param>
/// <param name="MaxSequentialNodeAttempts">
///     Maximum number of sequential node execution attempts before giving up.
///     Default is 5.
/// </param>
public sealed record PipelineRetryOptions(
    int MaxItemRetries = 0,
    int? MaxMaterializedItems = null,
    RetryDelayStrategyConfiguration? DelayStrategyConfiguration = null,
    int MaxNodeRestartAttempts = 3,
    int MaxSequentialNodeAttempts = 5)
{
    /// <summary>
    ///     Default options: item retries = 0 (no retry), node restarts = 3, sequential node attempts = 5.
    /// </summary>
    public static PipelineRetryOptions Default { get; } = new();

    /// <summary>
    ///     Creates a new instance with updated options, preserving unspecified values.
    /// </summary>
    public PipelineRetryOptions With(
        int? maxItemRetries = null,
        int? maxMaterializedItems = null,
        RetryDelayStrategyConfiguration? delayStrategyConfiguration = null,
        int? maxNodeRestartAttempts = null,
        int? maxSequentialNodeAttempts = null)
    {
        return new PipelineRetryOptions(
            maxItemRetries ?? MaxItemRetries,
            maxMaterializedItems ?? MaxMaterializedItems,
            delayStrategyConfiguration ?? DelayStrategyConfiguration,
            maxNodeRestartAttempts ?? MaxNodeRestartAttempts,
            maxSequentialNodeAttempts ?? MaxSequentialNodeAttempts);
    }
}
