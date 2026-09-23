using System.Collections.Concurrent;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Reliability;

namespace NPipeline.Pipeline;

/// <summary>
///     Execution configuration and runtime resilience state for a pipeline run.
/// </summary>
public sealed class PipelineExecutionConfigurationContext
{
    private readonly Dictionary<string, PipelineResilienceOptions> _nodeResilience = new();

    // Terminal nodes below a fan-out execute concurrently, so this is written from several threads.
    private readonly ConcurrentDictionary<string, InputFlow> _inputFlow = new(StringComparer.Ordinal);

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
        _inputFlow.Clear();
    }

    /// <summary>
    ///     Starts tracking whether <paramref name="nodeId" /> receives input, for node retry to consult.
    /// </summary>
    internal InputFlow TrackInputFlow(string nodeId)
    {
        return _inputFlow.GetOrAdd(nodeId, static _ => new InputFlow());
    }

    /// <summary>
    ///     The input tracking for <paramref name="nodeId" />, if node retry asked for it.
    /// </summary>
    internal InputFlow? GetInputFlow(string nodeId)
    {
        return _inputFlow.GetValueOrDefault(nodeId);
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

    /// <summary>
    ///     The circuit breakers for this run's nodes. A run started by <see cref="PipelineFactory" /> uses the
    ///     factory's registry for its definition, so breaker state carries over between runs; a strategy executed
    ///     outside a run uses this context's own.
    /// </summary>
    internal CircuitBreakerRegistry CircuitBreakers { get; set; } = new();
}
