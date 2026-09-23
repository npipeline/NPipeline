using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Reliability;

namespace NPipeline.Pipeline;

/// <summary>
///     Execution configuration and runtime resilience state for a pipeline run.
/// </summary>
public sealed class PipelineExecutionConfigurationContext
{
    private readonly Dictionary<string, PipelineResilienceOptions> _nodeResilience = new();

    internal PipelineExecutionConfigurationContext(PipelineOptimizationProfile optimizationProfile)
    {
        ResiliencePolicy = DefaultResiliencePolicy.Instance;
        OptimizationProfile = optimizationProfile;
    }

    /// <summary>
    ///     The pipeline's resilience options for the current run. Nodes without their own options use these.
    /// </summary>
    /// <remarks>
    ///     <see cref="PipelineResilienceOptions.None" /> until a run starts, when the pipeline's built options replace it.
    /// </remarks>
    public PipelineResilienceOptions Resilience { get; internal set; } = PipelineResilienceOptions.None;

    /// <summary>
    ///     The resilience options that apply to <paramref name="nodeId" />: the node's own, or else the pipeline's.
    /// </summary>
    /// <param name="nodeId">The node id.</param>
    public PipelineResilienceOptions GetResilienceOptions(string nodeId)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        return _nodeResilience.TryGetValue(nodeId, out var options) ? options : Resilience;
    }

    /// <summary>
    ///     Sets the node's own resilience options for the current run.
    /// </summary>
    internal void SetNodeResilienceOptions(string nodeId, PipelineResilienceOptions options)
    {
        _nodeResilience[nodeId] = options;
    }

    /// <summary>
    ///     Returns the resilience state to what a new context starts with.
    /// </summary>
    internal void ResetResilienceOptions()
    {
        Resilience = PipelineResilienceOptions.None;
        _nodeResilience.Clear();
    }

    /// <summary>
    ///     Unified resilience policy used by runtime execution.
    /// </summary>
    public IResiliencePolicy ResiliencePolicy { get; internal set; }

    /// <summary>
    ///     The optimization profile governing runtime behavior for this pipeline run.
    ///     This is the runtime source of truth for the active profile - node authors and runtime
    ///     code should read it from here rather than from <see cref="PipelineContextConfiguration" />.
    ///     The profile's effects (resilience defaults, dictionary types) are already baked into their
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

    /// <summary>
    ///     The circuit breakers for this run's nodes. A run started by <see cref="PipelineFactory" /> uses the
    ///     factory's registry for its definition, so breaker state carries over between runs; a strategy executed
    ///     outside a run uses this context's own.
    /// </summary>
    internal CircuitBreakerRegistry CircuitBreakers { get; set; } = new();
}
