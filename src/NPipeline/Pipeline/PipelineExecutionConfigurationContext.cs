using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Resilience;

namespace NPipeline.Pipeline;

/// <summary>
///     Execution configuration and runtime resilience state for a pipeline run.
/// </summary>
public sealed class PipelineExecutionConfigurationContext
{
    internal PipelineExecutionConfigurationContext(
        PipelineRetryOptions retryOptions,
        PipelineOptimizationProfile optimizationProfile)
    {
        RetryOptions = retryOptions;
        GlobalRetryOptions = retryOptions;
        ResiliencePolicy = DefaultResiliencePolicy.Instance;
        OptimizationProfile = optimizationProfile;
    }

    /// <summary>
    ///     Initial execution / retry configuration for this pipeline run.
    /// </summary>
    public PipelineRetryOptions RetryOptions { get; }

    /// <summary>
    ///     Effective global retry options for the current pipeline run.
    /// </summary>
    public PipelineRetryOptions GlobalRetryOptions { get; internal set; }

    /// <summary>
    ///     Per-node retry option overrides indexed by node id.
    /// </summary>
    public Dictionary<string, PipelineRetryOptions> NodeRetryOverrides { get; } = new();

    /// <summary>
    ///     Unified resilience policy used by runtime execution.
    /// </summary>
    public IResiliencePolicy ResiliencePolicy { get; internal set; }

    /// <summary>
    ///     Circuit-breaker options for the current run.
    /// </summary>
    public PipelineCircuitBreakerOptions? CircuitBreakerOptions { get; internal set; }

    /// <summary>
    ///     Circuit-breaker memory management options for the current run.
    /// </summary>
    public CircuitBreakerMemoryManagementOptions? CircuitBreakerMemoryOptions { get; internal set; }

    /// <summary>
    ///     The optimization profile governing runtime behavior for this pipeline run.
    ///     This is the runtime source of truth for the active profile - node authors and runtime
    ///     code should read it from here rather than from <see cref="PipelineContextConfiguration" />.
    ///     The profile's effects (retry defaults, dictionary types) are already baked into their
    ///     respective configurations at build time.
    /// </summary>
    public PipelineOptimizationProfile OptimizationProfile { get; }

    /// <summary>
    ///     Indicates the current run uses parallel execution behavior.
    /// </summary>
    public bool IsParallelExecution { get; internal set; }

    /// <summary>
    ///     The last retry-exhausted exception observed in the pipeline.
    /// </summary>
    public RetryExhaustedException? LastRetryExhaustedException { get; internal set; }

    internal ICircuitBreakerManager? CircuitBreakerManager { get; set; }
}
