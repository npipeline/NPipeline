using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Configuration;

/// <summary>
///     Configurable retry / resilience parameters controlling per-item retries and node restarts.
/// </summary>
/// <remarks>
///     Derive one set of options from another with the record <c>with</c> expression, which sets exactly the
///     properties named and copies the rest — including setting a nullable back to <see langword="null" />:
///     <code>
///     var opts = PipelineRetryOptions.Default with { MaxItemRetries = 3, MaxMaterializedItems = null };
///     </code>
/// </remarks>
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
    ///     Creates profile-aware retry options.
    ///     In <see cref="PipelineOptimizationProfile.Default" /> mode, applies sensible defaults:
    ///     3 item retries, 10,000-item materialization cap, and exponential backoff with full jitter.
    ///     In <see cref="PipelineOptimizationProfile.HighThroughput" /> mode, returns the baseline strict defaults
    ///     (no retries, no materialization cap, no delay strategy).
    /// </summary>
    /// <param name="profile">The optimization profile to build retry options for.</param>
    /// <returns>A <see cref="PipelineRetryOptions" /> instance configured for the given profile.</returns>
    public static PipelineRetryOptions ForProfile(PipelineOptimizationProfile profile)
    {
        return profile switch
        {
            PipelineOptimizationProfile.Default => Default with
            {
                DelayStrategyConfiguration = RetryDelayConfigurationExtensions.DefaultExponentialBackoffWithJitter,
                MaxItemRetries = 3,
                MaxMaterializedItems = 10_000,
            },
            PipelineOptimizationProfile.HighThroughput => Default,
            _ => Default
        };
    }
}
