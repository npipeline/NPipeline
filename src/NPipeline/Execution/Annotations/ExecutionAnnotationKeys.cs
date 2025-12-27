namespace NPipeline.Execution.Annotations;

/// <summary>
///     Centralized keys and helpers used for execution annotations (builder graph annotations)
///     and pipeline context properties to avoid magic strings across the core.
/// </summary>
public static class ExecutionAnnotationKeys
{
    // ============================
    // Annotation prefixes
    // ============================

    /// <summary>
    ///     Prefix applied to any global annotation keys stored in the graph annotations bag (e.g., "global::merge.capacity").
    /// </summary>
    public const string GlobalAnnotationPrefix = "global::";

    // ============================
    // Raw global keys (used with PipelineBuilder.SetGlobalAnnotation)
    // ============================

    /// <summary>
    ///     Raw key used with PipelineBuilder.SetGlobalAnnotation to configure the default join interleave capacity.
    ///     The fully-qualified key in the annotation bag is <see cref="GlobalMergeCapacity" />.
    /// </summary>
    public const string GlobalMergeCapacityKey = "merge.capacity";

    /// <summary>
    ///     Raw key used with PipelineBuilder.SetGlobalAnnotation to configure the default per-subscriber branching capacity.
    ///     The fully-qualified key in the annotation bag is <see cref="GlobalBranchingCapacity" />.
    /// </summary>
    public const string GlobalBranchingCapacityKey = "branch.capacity";

    // ============================
    // Fully-qualified global annotations (as they appear in the annotation bag)
    // ============================

    /// <summary>
    ///     Fully-qualified annotation key (in the annotations bag) for global per-subscriber branching capacity.
    ///     Backed by <see cref="GlobalBranchingCapacityKey" /> with <see cref="GlobalAnnotationPrefix" /> applied.
    /// </summary>
    public const string GlobalBranchingCapacity = GlobalAnnotationPrefix + GlobalBranchingCapacityKey;

    /// <summary>
    ///     Fully-qualified annotation key (in the annotations bag) for global join/merge interleave capacity.
    ///     Backed by <see cref="GlobalMergeCapacityKey" /> with <see cref="GlobalAnnotationPrefix" /> applied.
    /// </summary>
    public const string GlobalMergeCapacity = GlobalAnnotationPrefix + GlobalMergeCapacityKey;

    /// <summary>
    ///     Fully-qualified annotation key (in the annotations bag) for a globally-specified execution observer
    ///     (value: IExecutionObserver).
    /// </summary>
    public const string GlobalExecutionObserver = GlobalAnnotationPrefix + "ExecutionObserver";

    // ============================
    // Context property keys (after PipelineRunner copies global annotations into pipeline context)
    // ============================

    /// <summary>
    ///     Prefix used for properties added to <c>PipelineContext.Properties</c> when global annotations are copied over.
    ///     For example, "NPipeline.Global.ExecutionObserver".
    /// </summary>
    public const string GlobalPropertyPrefix = "NPipeline.Global.";

    /// <summary>
    ///     The <c>PipelineContext.Properties</c> key under which a resolved <c>IExecutionObserver</c> is stored.
    /// </summary>
    public const string ExecutionObserverProperty = GlobalPropertyPrefix + "ExecutionObserver";

    // ============================
    // Context-level metrics storage (branching)
    // ============================

    /// <summary>
    ///     Prefix used to store and retrieve per-node branch metrics objects in <c>PipelineContext.Items</c>,
    ///     keyed as "branch.metrics::{nodeId}".
    /// </summary>
    public const string BranchMetricsPrefix = "branch.metrics::";

    // ============================
    // Node-scoped annotations
    // ============================

    /// <summary>
    ///     Node-scoped annotation key for branching options (value: BranchingOptions),
    ///     stored as "branch::{nodeId}".
    /// </summary>
    public static string BranchOptionsForNode(string nodeId)
    {
        return $"branch::{nodeId}";
    }

    /// <summary>
    ///     Node-scoped annotation key for join/merge interleave capacity (value: int),
    ///     stored as "merge.capacity::{nodeId}".
    /// </summary>
    public static string MergeCapacityForNode(string nodeId)
    {
        return $"merge.capacity::{nodeId}";
    }

    /// <summary>
    ///     Builds the branching metrics key used with <c>PipelineContext.Items</c> for a given <paramref name="nodeId" />.
    /// </summary>
    public static string BranchMetricsForNode(string nodeId)
    {
        return BranchMetricsPrefix + nodeId;
    }
}
