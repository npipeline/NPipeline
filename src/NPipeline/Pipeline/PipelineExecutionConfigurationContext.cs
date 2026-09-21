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

    private RetryExhaustedException? _lastRetryExhaustedException;

    /// <summary>
    ///     The most recent retry-exhausted exception reported by a node, awaiting consumption by error handling.
    /// </summary>
    /// <remarks>
    ///     This is a hand-off slot, not a run-scoped record. It is written when a node exhausts its retries and taken
    ///     by the first error handler that reports a failure, so that the downstream failure caused by the truncated
    ///     stream carries the real root cause. Leaving it set would attribute the earlier node's message and inner
    ///     exception to every later failure in the run. Use <see cref="TakeLastRetryExhaustedException" /> to consume
    ///     it; reading this property does not clear it.
    /// </remarks>
    public RetryExhaustedException? LastRetryExhaustedException
    {
        get => Volatile.Read(ref _lastRetryExhaustedException);
        internal set => Volatile.Write(ref _lastRetryExhaustedException, value);
    }

    /// <summary>
    ///     Atomically takes the pending retry-exhausted exception, clearing the slot.
    /// </summary>
    /// <returns>The pending exception, or <see langword="null" /> if none is pending.</returns>
    /// <remarks>
    ///     Atomic so that two nodes failing concurrently cannot both report the same root cause.
    /// </remarks>
    internal RetryExhaustedException? TakeLastRetryExhaustedException()
    {
        return Interlocked.Exchange(ref _lastRetryExhaustedException, null);
    }

    internal ICircuitBreakerManager? CircuitBreakerManager { get; set; }
}
