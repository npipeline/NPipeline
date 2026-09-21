using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;
using NPipeline.ErrorHandling;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.RetryDelay;
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
        _effectiveRetryOptions = retryOptions;
        ResiliencePolicy = DefaultResiliencePolicy.Instance;
        OptimizationProfile = optimizationProfile;
    }

    /// <summary>
    ///     Initial execution / retry configuration for this pipeline run.
    /// </summary>
    public PipelineRetryOptions RetryOptions { get; }

    /// <summary>
    ///     Retry options as amended at runtime, which is what retry delays are computed from.
    /// </summary>
    /// <remarks>
    ///     Identical to <see cref="RetryOptions" /> until something overrides the delay strategy through one of the
    ///     <c>Use*Delay</c> extensions on <see cref="PipelineContext" />.
    /// </remarks>
    public PipelineRetryOptions EffectiveRetryOptions => Volatile.Read(ref _effectiveRetryOptions);

    /// <summary>
    ///     Replaces the delay strategy configuration for this run and discards the cached strategy built from the
    ///     previous one.
    /// </summary>
    internal void OverrideRetryDelayConfiguration(RetryDelayStrategyConfiguration configuration)
    {
        lock (_retryDelayGate)
        {
            Volatile.Write(ref _effectiveRetryOptions, _effectiveRetryOptions with { DelayStrategyConfiguration = configuration });
            _retryDelayStrategy = null;
        }
    }

    /// <summary>
    ///     Returns the retry delay strategy for this run, building it once through <paramref name="factory" />.
    /// </summary>
    /// <remarks>
    ///     The strategy is per-run state, so the cache and its gate live on the run's context rather than in a static
    ///     field shared by every pipeline in the process.
    /// </remarks>
    internal IRetryDelayStrategy GetOrCreateRetryDelayStrategy(Func<PipelineRetryOptions, IRetryDelayStrategy> factory)
    {
        var cached = Volatile.Read(ref _retryDelayStrategy);

        if (cached is not null)
            return cached;

        lock (_retryDelayGate)
        {
            _retryDelayStrategy ??= factory(_effectiveRetryOptions);
            return _retryDelayStrategy;
        }
    }

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

    private readonly object _retryDelayGate = new();
    private PipelineRetryOptions _effectiveRetryOptions;
    private IRetryDelayStrategy? _retryDelayStrategy;

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
