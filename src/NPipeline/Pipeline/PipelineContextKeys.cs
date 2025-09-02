namespace NPipeline.Pipeline;

/// <summary>
///     Well-known keys for PipelineContext.Items dictionary.
///     Use these constants to avoid typos and enable IntelliSense.
/// </summary>
/// <remarks>
///     This class provides strongly-typed constants for all context keys used throughout NPipeline.
///     By using these constants instead of string literals, you get compile-time safety and better IDE support.
/// </remarks>
public static class PipelineContextKeys
{
    // ===== EXECUTION MODE =====
    /// <summary>Indicates parallel execution mode (value: bool)</summary>
    /// <remarks>Set to true when using parallel execution strategy to signal downstream components about execution model.</remarks>
    public const string ParallelExecution = "parallel.execution";

    // ===== CONFIGURATION =====
    /// <summary>Pre-configured node instances (value: Dictionary&lt;string, INode&gt;)</summary>
    /// <remarks>Used to inject pre-built node instances into the pipeline, typically for testing or advanced scenarios.</remarks>
    public const string PreconfiguredNodes = "nodes.preconfigured";

    /// <summary>Indicates DI container owns node lifecycle (value: bool)</summary>
    /// <remarks>When true, the pipeline will not dispose node instances as the DI container owns their lifetime.</remarks>
    public const string DiOwnedNodes = "nodes.owned.by.di";

    // ===== RETRY & RESILIENCE =====
    /// <summary>Global retry options (value: PipelineRetryOptions)</summary>
    /// <remarks>Surface-level retry configuration that applies to all nodes unless overridden per-node.</remarks>
    public const string GlobalRetryOptions = "resilience.retry.global.options";

    /// <summary>Circuit breaker options (value: PipelineCircuitBreakerOptions)</summary>
    /// <remarks>Circuit breaker configuration for resilient execution patterns.</remarks>
    public const string CircuitBreakerOptions = "resilience.circuit.breaker.options";

    /// <summary>Circuit breaker memory management options (value: CircuitBreakerMemoryManagementOptions)</summary>
    /// <remarks>Advanced cleanup tuning for circuit breaker manager lifecycle.</remarks>
    public const string CircuitBreakerMemoryOptions = "resilience.circuit.breaker.memory.options";

    /// <summary>Circuit breaker manager (value: ICircuitBreakerManager)</summary>
    /// <remarks>Circuit breaker manager for creating and managing circuit breaker instances.</remarks>
    public const string CircuitBreakerManager = "resilience.circuit.breaker.manager";

    // ===== STATISTICS & METRICS =====
    /// <summary>Pipeline start time UTC (value: DateTime)</summary>
    /// <remarks>Records when the pipeline execution started for metrics and diagnostics.</remarks>
    public const string PipelineStartTimeUtc = "stats.pipeline.start.time.utc";

    /// <summary>Total processed items counter (value: StatsCounter)</summary>
    /// <remarks>Accumulates statistics about items processed throughout the pipeline execution.</remarks>
    public const string TotalProcessedItems = "stats.processed.items.total";

    /// <summary>Last retry exhausted exception (value: RetryExhaustedException)</summary>
    /// <remarks>Stores the last exception when retry attempts have been exhausted for debugging and error handling.</remarks>
    public const string LastRetryExhaustedException = "diagnostics.retry.exhausted.exception";

    /// <summary>Parent context for testing - format: "testing.parentContext" (value: PipelineContext)</summary>
    /// <remarks>Used by testing extensions to reference the parent pipeline context in nested scenarios.</remarks>
    public const string TestingParentContext = "testing.parentContext";

    // ===== LINEAGE =====
    /// <summary>Item-level lineage sink (value: ILineageSink)</summary>
    /// <remarks>Stores the lineage sink instance for tracking item-level lineage during execution.</remarks>
    public const string LineageSink = "lineage.item.sink";

    /// <summary>Pipeline-level lineage sink (value: IPipelineLineageSink)</summary>
    /// <remarks>Stores the pipeline-level lineage sink instance for tracking overall pipeline lineage.</remarks>
    public const string PipelineLineageSink = "lineage.pipeline.sink";

    // ===== PER-NODE OPTIONS & METRICS =====
    /// <summary>Per-node retry options - format: "retry::{nodeId}" (value: PipelineRetryOptions)</summary>
    /// <remarks>Allows configuring retry behavior on a per-node basis, overriding global settings.</remarks>
    public static string NodeRetryOptions(string nodeId)
    {
        return $"retry::{nodeId}";
    }

    /// <summary>Per-node execution options - format: "execopt::{nodeId}" (value: varies by strategy)</summary>
    /// <remarks>Stores execution strategy-specific options for individual nodes (e.g., ParallelOptions).</remarks>
    public static string NodeExecutionOptions(string nodeId)
    {
        return $"execopt::{nodeId}";
    }

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
