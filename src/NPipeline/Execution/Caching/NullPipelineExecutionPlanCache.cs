using NPipeline.Execution.Plans;
using NPipeline.Graph;

namespace NPipeline.Execution.Caching;

/// <summary>
///     Null object pattern implementation of <see cref="IPipelineExecutionPlanCache" /> that performs no caching.
///     This implementation is used when caching is explicitly disabled.
/// </summary>
/// <remarks>
///     This cache always reports cache misses and discards any plans provided to it.
///     It provides zero overhead when caching is not needed or desired.
/// </remarks>
public sealed class NullPipelineExecutionPlanCache : IPipelineExecutionPlanCache
{
    /// <summary>
    ///     Singleton instance of the null cache.
    /// </summary>
    public static readonly NullPipelineExecutionPlanCache Instance = new();

    private NullPipelineExecutionPlanCache()
    {
    }

    /// <inheritdoc />
    public bool TryGetCachedPlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        out Dictionary<string, NodeExecutionPlan>? cachedPlans)
    {
        cachedPlans = null;
        return false;
    }

    /// <inheritdoc />
    public void CachePlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        Dictionary<string, NodeExecutionPlan> plans)
    {
        // No-op: discards the plans
    }

    /// <inheritdoc />
    public void Clear()
    {
        // No-op: nothing to clear
    }

    /// <inheritdoc />
    public int Count => 0;
}
