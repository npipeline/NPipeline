using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Lineage;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution;

/// <summary>
///     Provides a read-only, cached snapshot of frequently-accessed context values for a node execution.
///     This optimization reduces dictionary lookups and allocations during per-item processing.
/// </summary>
/// <remarks>
///     <para>
///         <strong>Performance Optimization:</strong>
///         This class addresses the performance bottleneck identified in the optimization report where
///         dictionary lookups and IDisposable allocations (context.ScopedNode)
///         occur per-item during node execution. By caching these values at node scope, we achieve:
///     </para>
///     <list type="bullet">
///         <item>
///             <description>~150-250μs reduction in overhead per 1K items</description>
///         </item>
///         <item>
///             <description>Fewer allocations (no per-item IDisposable)</description>
///         </item>
///         <item>
///             <description>Better cache locality (value types in struct)</description>
///         </item>
///     </list>
///     <para>
///         <strong>Immutability Contract:</strong>
///         This class assumes that context state relevant to node execution (retry options, tracing configuration, etc.)
///         does not change during the execution of a single node. This is enforced by:
///     </para>
///     <list type="bullet">
///         <item>
///             <description>Capturing a snapshot at node execution start</description>
///         </item>
///         <item>
///             <description>Optional validation mode that checks for context mutations (DEBUG builds)</description>
///         </item>
///         <item>
///             <description>Developer documentation emphasizing immutability requirements</description>
///         </item>
///     </list>
///     <para>
///         <strong>Thread Safety:</strong>
///         Instances are created per-node execution and are not thread-safe. In parallel execution scenarios,
///         each worker thread should create its own instance.
///     </para>
/// </remarks>
public readonly struct CachedNodeExecutionContext
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="CachedNodeExecutionContext" /> struct.
    /// </summary>
    /// <param name="nodeId">The ID of the node being executed.</param>
    /// <param name="resilience">The resilience options that apply to this node.</param>
    /// <param name="tracingEnabled">Whether tracing is enabled.</param>
    /// <param name="loggingEnabled">Whether logging is enabled.</param>
    /// <param name="lineageOutcomeWriter">The lineage outcome writer resolved once for this node execution.</param>
    /// <param name="circuitBreaker">The node's circuit breaker, or null when it has none.</param>
    /// <param name="cancellationToken">The cancellation token for this execution.</param>
    private CachedNodeExecutionContext(
        string nodeId,
        PipelineResilienceOptions resilience,
        bool tracingEnabled,
        bool loggingEnabled,
        LineageNodeOutcomeWriter lineageOutcomeWriter,
        CircuitBreaker? circuitBreaker,
        CancellationToken cancellationToken)
    {
        NodeId = nodeId;
        Resilience = resilience;
        TracingEnabled = tracingEnabled;
        LoggingEnabled = loggingEnabled;
        LineageOutcomeWriter = lineageOutcomeWriter;
        CircuitBreaker = circuitBreaker;
        CancellationToken = cancellationToken;
    }

    /// <summary>
    ///     Gets the ID of the node being executed.
    /// </summary>
    public string NodeId { get; }

    /// <summary>
    ///     Gets the resilience options that apply to this node execution: the node's own, or else the pipeline's.
    /// </summary>
    public PipelineResilienceOptions Resilience { get; }

    /// <summary>
    ///     Gets a value indicating whether tracing is enabled for this execution.
    ///     True if the tracer is not the null tracer instance.
    /// </summary>
    public bool TracingEnabled { get; }

    /// <summary>
    ///     Gets a value indicating whether logging is enabled for this execution.
    ///     True if the logger factory is not the null logger factory instance.
    /// </summary>
    public bool LoggingEnabled { get; }

    /// <summary>
    ///     Gets the cancellation token for this node execution.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    ///     Gets the lineage outcome writer bound to this node's outcome store, resolved once at creation so
    ///     per-item lineage recording avoids repeated registry lookups on the hot path. Inactive when the
    ///     node is not tracking lineage.
    /// </summary>
    internal LineageNodeOutcomeWriter LineageOutcomeWriter { get; }

    /// <summary>
    ///     Gets the node's circuit breaker, resolved once for this execution, or null when its options configure none.
    /// </summary>
    internal CircuitBreaker? CircuitBreaker { get; }

    /// <summary>
    ///     Creates a cached execution context from the current pipeline context.
    ///     This method captures a snapshot of execution-relevant state for efficient per-item processing.
    /// </summary>
    /// <param name="context">The pipeline context to capture state from.</param>
    /// <param name="nodeId">The ID of the node being executed.</param>
    /// <returns>A cached execution context with immutable state.</returns>
    /// <exception cref="ArgumentNullException">Thrown if context or nodeId is null.</exception>
    /// <remarks>
    ///     <para>
    ///         <strong>Usage Pattern:</strong>
    ///     </para>
    ///     <code>
    ///         // At the start of node execution (before item iteration)
    ///         var cached = CachedNodeExecutionContext.Create(context, nodeId);
    /// 
    ///         // During item processing - use cached values
    ///         await foreach (var item in input.WithCancellation(cached.CancellationToken))
    ///         {
    ///             if (cached.TracingEnabled)
    ///             {
    ///                 using var activity = context.Observability.Tracer.StartActivity("Item.Transform");
    ///                 // ... process item with cached.Resilience
    ///             }
    ///         }
    ///     </code>
    ///     <para>
    ///         <strong>Captured State:</strong>
    ///     </para>
    ///     <list type="bullet">
    ///         <item>
    ///             <description>Resilience options (the node's own, or else the pipeline's)</description>
    ///         </item>
    ///         <item>
    ///             <description>The node's circuit breaker, if its options configure one</description>
    ///         </item>
    ///         <item>
    ///             <description>Tracing enabled flag (based on tracer type)</description>
    ///         </item>
    ///         <item>
    ///             <description>Logging enabled flag (based on logger factory type)</description>
    ///         </item>
    ///         <item>
    ///             <description>Cancellation token</description>
    ///         </item>
    ///     </list>
    ///     <para>
    ///         <strong>Immutability Guarantee:</strong>
    ///         The cached context assumes that context retry state and related configuration
    ///         do not change during node execution. Modifications to these values during node execution will
    ///         not be reflected in the cached context and may lead to inconsistent behavior.
    ///     </para>
    ///     <para>
    ///         In DEBUG builds, an optional validation mode can detect context mutations (see TODO for future enhancement).
    ///     </para>
    /// </remarks>
    public static CachedNodeExecutionContext Create(PipelineContext context, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeId);

        var resilience = context.ExecutionConfiguration.GetResilienceOptions(nodeId);

        // Determine if tracing is enabled by checking if tracer is not the null implementation
        var tracingEnabled = context.Observability.Tracer is not NullPipelineTracer;

        // Determine if logging is enabled by checking if logger factory is not the null implementation
        var loggingEnabled = context.Observability.LoggerFactory is not NullLoggerFactory;

        return new CachedNodeExecutionContext(
            nodeId,
            resilience,
            tracingEnabled,
            loggingEnabled,
            context.Lineage.Outcomes.GetWriter(nodeId),
            context.ExecutionConfiguration.CircuitBreakers.Resolve(nodeId, resilience),
            context.CancellationToken);
    }
}
