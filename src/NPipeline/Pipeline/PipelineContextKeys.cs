namespace NPipeline.Pipeline;

/// <summary>
///     Well-known keys for PipelineContext.Items dictionary.
///     Use these constants for user/extension data and runtime annotation keys that remain string-indexed.
/// </summary>
/// <remarks>
///     <para>
///         Framework-managed execution services (for example retry options, circuit breaker manager, and lineage sinks)
///         are exposed as strongly-typed <see cref="PipelineContext" /> members.
///         <see cref="PipelineContext.Items" /> remains available for user-defined and extension-defined values.
///     </para>
///     <para>
///         <strong>Reserved Key Prefixes:</strong>
///         NPipeline reserves the following key prefixes to prevent conflicts with user-defined keys in context.Items:
///     </para>
///     <list type="table">
///         <listheader>
///             <term>Prefix</term>
///             <description>Purpose</description>
///         </listheader>
///         <item>
///             <term>
///                 <c>parallel.*</c>
///             </term>
///             <description>Parallel execution mode flags and metrics for parallel execution strategies</description>
///         </item>
///         <item>
///             <term>
///                 <c>testing.*</c>
///             </term>
///             <description>Test data and test-specific configuration</description>
///         </item>
///         <item>
///             <term>
///                 <c>NPipeline.*</c>
///             </term>
///             <description>NPipeline-internal data (testing source data)</description>
///         </item>
///         <item>
///             <term>
///                 <c>branch.metrics::</c>
///             </term>
///             <description>Branch-specific execution metrics (format: "branch.metrics::{nodeId}")</description>
///         </item>
///         <item>
///             <term>
///                 <c>diag.resilience.</c>
///             </term>
///             <description>Per-node resilience diagnostics (format: "diag.resilience.{nodeId}.*")</description>
///         </item>
///     </list>
///     <para>
///         <strong>User-Defined Keys:</strong>
///         Users are free to add their own keys to context.Items, but should avoid using any of the reserved
///         prefixes listed above to prevent conflicts with NPipeline internal keys.
///     </para>
///     <example>
///         <code>
///         // Safe: Using strongly-typed PipelineContext properties
///         var context = PipelineContext.Default;
///         var retryOptions = context.GlobalRetryOptions;
/// 
///         // Safe: User-defined key with clear naming
///         context.Items["MyApp.CustomSetting"] = "value";
/// 
///         // Unsafe: Using reserved prefix - may conflict
///         context.Items["parallel.customFlag"] = true; // Avoid! Uses reserved prefix
///         </code>
///     </example>
/// </remarks>
public static class PipelineContextKeys
{
    /// <summary>Dead-letter sink decorator hook key (value: Func&lt;IDeadLetterSink?, IDeadLetterSink?&gt;).</summary>
    /// <remarks>Allows extensions to decorate the resolved dead-letter sink at pipeline execution time.</remarks>
    public const string DeadLetterSinkDecorator = "NPipeline.DeadLetterSinkDecorator";

    /// <summary>Lineage sink decorator hook key (value: Func&lt;ILineageSink?, ILineageSink?&gt;).</summary>
    /// <remarks>Allows extensions to decorate the resolved lineage sink at pipeline execution time.</remarks>
    public const string LineageSinkDecorator = "NPipeline.LineageSinkDecorator";

    /// <summary>
    /// Runtime lineage options override key.
    /// Supports either a <c>LineageOptions</c> value or a <c>Func&lt;LineageOptions?, LineageOptions?&gt;</c>
    /// to derive options from the graph's configured baseline.
    /// </summary>
    public const string LineageOptionsOverride = "NPipeline.LineageOptionsOverride";

    /// <summary>
    /// Runtime item-level lineage enablement override key (value: <c>bool</c>).
    /// When present, this value overrides the graph's configured <c>ItemLevelLineageEnabled</c> flag for the run.
    /// </summary>
    public const string ItemLevelLineageEnabledOverride = "NPipeline.ItemLevelLineageEnabledOverride";

    /// <summary>Parent context for testing - format: "testing.parentContext" (value: PipelineContext)</summary>
    /// <remarks>Used by testing extensions to reference the parent pipeline context in nested scenarios.</remarks>
    public const string TestingParentContext = "testing.parentContext";

    // ===== PARALLEL EXECUTION METRICS =====
    /// <summary>Parallel execution metrics - format: "parallel.metrics::{nodeId}" (value: ParallelExecutionMetrics)</summary>
    /// <remarks>Stores comprehensive metrics about parallel execution for a specific node.</remarks>
    public static string ParallelMetrics(string nodeId)
    {
        return $"parallel.metrics::{nodeId}";
    }

    /// <summary>Parallel output capacity metrics - format: "parallel.metrics.output.capacity::{nodeId}" (value: int)</summary>
    /// <remarks>Tracks the output buffer capacity for parallel nodes.</remarks>
    public static string ParallelMetricsOutputCapacity(string nodeId)
    {
        return $"parallel.metrics.output.capacity::{nodeId}";
    }

    /// <summary>Parallel input high water mark - format: "parallel.metrics.input.highwater::{nodeId}" (value: int)</summary>
    /// <remarks>Tracks the peak input queue depth observed during parallel execution.</remarks>
    public static string ParallelMetricsInputHighWater(string nodeId)
    {
        return $"parallel.metrics.input.highwater::{nodeId}";
    }

    /// <summary>Parallel output high water mark - format: "parallel.metrics.output.highwater::{nodeId}" (value: int)</summary>
    /// <remarks>Tracks the peak output queue depth observed during parallel execution.</remarks>
    public static string ParallelMetricsOutputHighWater(string nodeId)
    {
        return $"parallel.metrics.output.highwater::{nodeId}";
    }

    /// <summary>Parallel retry events count - format: "parallel.metrics.retry.events::{nodeId}" (value: int)</summary>
    /// <remarks>Counts how many times retry was triggered during parallel execution.</remarks>
    public static string ParallelMetricsRetryEvents(string nodeId)
    {
        return $"parallel.metrics.retry.events::{nodeId}";
    }

    /// <summary>Items with retry - format: "parallel.metrics.retry.items::{nodeId}" (value: int)</summary>
    /// <remarks>Counts distinct items that required at least one retry.</remarks>
    public static string ParallelMetricsRetryItems(string nodeId)
    {
        return $"parallel.metrics.retry.items::{nodeId}";
    }

    /// <summary>Max item retry attempts - format: "parallel.metrics.retry.maxItemAttempts::{nodeId}" (value: int)</summary>
    /// <remarks>Tracks the maximum number of attempts for a single item during parallel execution.</remarks>
    public static string ParallelMetricsMaxItemRetryAttempts(string nodeId)
    {
        return $"parallel.metrics.retry.maxItemAttempts::{nodeId}";
    }

    /// <summary>Dropped newest items - format: "parallel.metrics.dropped.newest::{nodeId}" (value: int)</summary>
    /// <remarks>Count of items dropped when using drop-newest queue policy.</remarks>
    public static string ParallelMetricsDroppedNewest(string nodeId)
    {
        return $"parallel.metrics.dropped.newest::{nodeId}";
    }

    /// <summary>Dropped oldest items - format: "parallel.metrics.dropped.oldest::{nodeId}" (value: int)</summary>
    /// <remarks>Count of items dropped when using drop-oldest queue policy.</remarks>
    public static string ParallelMetricsDroppedOldest(string nodeId)
    {
        return $"parallel.metrics.dropped.oldest::{nodeId}";
    }

    /// <summary>Enqueued items - format: "parallel.metrics.enqueued::{nodeId}" (value: int)</summary>
    /// <remarks>Total count of items enqueued for processing in parallel execution.</remarks>
    public static string ParallelMetricsEnqueued(string nodeId)
    {
        return $"parallel.metrics.enqueued::{nodeId}";
    }

    /// <summary>Processed items - format: "parallel.metrics.processed::{nodeId}" (value: int)</summary>
    /// <remarks>Total count of items successfully processed in parallel execution.</remarks>
    public static string ParallelMetricsProcessed(string nodeId)
    {
        return $"parallel.metrics.processed::{nodeId}";
    }

    // ===== DIAGNOSTICS & RESILIENCE =====
    /// <summary>Diagnostics - resilience failures - format: "diag.resilience.{nodeId}.failures" (value: int)</summary>
    /// <remarks>Tracks total failures observed during resilient execution of a node.</remarks>
    public static string DiagnosticsResilienceFailures(string nodeId)
    {
        return $"diag.resilience.{nodeId}.failures";
    }

    /// <summary>Diagnostics - resilience consecutive failures - format: "diag.resilience.{nodeId}.consecutiveFailures" (value: int)</summary>
    /// <remarks>Tracks consecutive failures observed during resilient execution of a node.</remarks>
    public static string DiagnosticsResilienceConsecutiveFailures(string nodeId)
    {
        return $"diag.resilience.{nodeId}.consecutiveFailures";
    }

    /// <summary>Diagnostics - resilience throwing on failure - format: "diag.resilience.{nodeId}.throwingOnFailure" (value: bool)</summary>
    /// <remarks>Indicates whether the resilient strategy is throwing an exception due to repeated failures.</remarks>
    public static string DiagnosticsResilienceThrowingOnFailure(string nodeId)
    {
        return $"diag.resilience.{nodeId}.throwingOnFailure";
    }

    // ===== TESTING EXTENSIONS =====
    /// <summary>Source test data - format: "NPipeline.Testing.SourceData::{nodeId}" (value: List&lt;T&gt;)</summary>
    /// <remarks>Used by testing extensions to inject source data for InMemorySourceNode instances by node ID.</remarks>
    public static string TestingSourceData(string nodeId)
    {
        return $"NPipeline.Testing.SourceData::{nodeId}";
    }

    /// <summary>Source test data by type - format: "NPipeline.Testing.SourceData::{typeName}" (value: List&lt;T&gt;)</summary>
    /// <remarks>Used by testing extensions to inject source data for InMemorySourceNode instances by type name.</remarks>
    public static string TestingSourceDataByType(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return $"NPipeline.Testing.SourceData::{type.FullName}";
    }

    // ===== BRANCHING =====
    /// <summary>Branch execution metrics - format: "branch.metrics::{nodeId}" (value: BranchMetrics)</summary>
    /// <remarks>Stores branch-specific execution metrics for a node performing branching operations.</remarks>
    public static string BranchMetricsForNode(string nodeId)
    {
        return $"branch.metrics::{nodeId}";
    }
}
