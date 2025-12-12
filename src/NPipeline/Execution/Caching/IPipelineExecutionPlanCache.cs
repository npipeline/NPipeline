using NPipeline.Execution.Plans;
using NPipeline.Graph;

namespace NPipeline.Execution.Caching;

/// <summary>
///     Defines a cache for storing and retrieving pre-compiled pipeline execution plans.
///     Implementations can provide different storage mechanisms (in-memory, distributed, etc.).
/// </summary>
/// <remarks>
///     <para>
///         Pipeline execution plans contain compiled expression trees for each node in a pipeline.
///         Caching these plans eliminates expensive reflection and expression compilation on
///         subsequent pipeline runs with the same structure.
///     </para>
///     <para>
///         Cache keys are based on the pipeline definition type and graph structure to ensure
///         that plans are reused only when the pipeline structure is identical.
///     </para>
/// </remarks>
public interface IPipelineExecutionPlanCache
{
    /// <summary>
    ///     Gets the number of cached pipeline execution plans.
    /// </summary>
    int Count { get; }

    /// <summary>
    ///     Attempts to retrieve a cached execution plan for the specified pipeline definition and graph.
    /// </summary>
    /// <param name="pipelineDefinitionType">The type of the pipeline definition.</param>
    /// <param name="graph">The pipeline graph structure.</param>
    /// <param name="cachedPlans">The cached execution plans if found, otherwise null.</param>
    /// <returns>True if cached plans were found, false otherwise.</returns>
    bool TryGetCachedPlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        out Dictionary<string, NodeExecutionPlan>? cachedPlans);

    /// <summary>
    ///     Stores execution plans in the cache for the specified pipeline definition and graph.
    /// </summary>
    /// <param name="pipelineDefinitionType">The type of the pipeline definition.</param>
    /// <param name="graph">The pipeline graph structure.</param>
    /// <param name="plans">The execution plans to cache.</param>
    void CachePlans(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        Dictionary<string, NodeExecutionPlan> plans);

    /// <summary>
    ///     Clears all cached execution plans.
    /// </summary>
    void Clear();
}
