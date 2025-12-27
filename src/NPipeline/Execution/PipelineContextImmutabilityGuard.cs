using System.Diagnostics;
using System.Runtime.CompilerServices;
using NPipeline.Configuration;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Provides runtime validation that pipeline context state remains immutable during node execution.
///     This guard helps detect violations of the immutability contract assumed by <see cref="CachedNodeExecutionContext" />.
/// </summary>
/// <remarks>
///     <para>
///         <strong>Purpose:</strong>
///         When using <see cref="CachedNodeExecutionContext" />, the framework assumes that context state
///         (retry options, tracing configuration, etc.) does not change during node execution. This guard
///         validates that assumption by capturing a snapshot of context state and detecting mutations.
///     </para>
///     <para>
///         <strong>Performance:</strong>
///         This validation is only active in DEBUG builds using [Conditional("DEBUG")]. In RELEASE builds,
///         all validation code is completely removed by the compiler, ensuring zero runtime overhead.
///     </para>
///     <para>
///         <strong>Usage Pattern:</strong>
///     </para>
///     <code>
///         var cached = CachedNodeExecutionContext.Create(context, nodeId);
///         var guard = PipelineContextImmutabilityGuard.Create(context, cached);
/// 
///         // ... process items ...
/// 
///         guard.Validate(context); // Throws in DEBUG if context was mutated
///     </code>
///     <para>
///         <strong>What is Validated:</strong>
///     </para>
///     <list type="bullet">
///         <item>
///             <description>Retry options have not changed in context.Items</description>
///         </item>
///         <item>
///             <description>Tracer instance has not been replaced</description>
///         </item>
///         <item>
///             <description>Logger factory instance has not been replaced</description>
///         </item>
///         <item>
///             <description>CancellationToken has not changed</description>
///         </item>
///     </list>
/// </remarks>
internal readonly struct PipelineContextImmutabilityGuard
{
    private readonly string _nodeId;
    private readonly int _retryOptionsHash;
    private readonly int _tracerHash;
    private readonly int _loggerFactoryHash;
    private readonly int _cancellationTokenHash;

    private PipelineContextImmutabilityGuard(
        string nodeId,
        int retryOptionsHash,
        int tracerHash,
        int loggerFactoryHash,
        int cancellationTokenHash)
    {
        _nodeId = nodeId;
        _retryOptionsHash = retryOptionsHash;
        _tracerHash = tracerHash;
        _loggerFactoryHash = loggerFactoryHash;
        _cancellationTokenHash = cancellationTokenHash;
    }

    /// <summary>
    ///     Creates an immutability guard that captures the current state of the context.
    ///     In RELEASE builds, this returns a default struct with no overhead.
    /// </summary>
    /// <param name="context">The pipeline context to monitor.</param>
    /// <param name="cached">The cached execution context created from the same pipeline context.</param>
    /// <returns>A guard that can validate context immutability (or default in RELEASE builds).</returns>
    public static PipelineContextImmutabilityGuard Create(PipelineContext context, CachedNodeExecutionContext cached)
    {
#if DEBUG
        return new PipelineContextImmutabilityGuard(
            cached.NodeId,
            RuntimeHelpers.GetHashCode(cached.RetryOptions),
            RuntimeHelpers.GetHashCode(context.Tracer),
            RuntimeHelpers.GetHashCode(context.LoggerFactory),
            context.CancellationToken.GetHashCode());
#else
            return default;
#endif
    }

    /// <summary>
    ///     Validates that the context state has not changed since the guard was created.
    ///     This method is only active in DEBUG builds and will throw if mutations are detected.
    /// </summary>
    /// <param name="context">The pipeline context to validate.</param>
    /// <exception cref="InvalidOperationException">
    ///     Thrown in DEBUG builds if context state has been mutated during node execution.
    /// </exception>
    [Conditional("DEBUG")]
    public void Validate(PipelineContext context)
    {
        // Check if retry options changed in context.Items
        var currentRetryOptionsHash = GetCurrentRetryOptionsHash(context, _nodeId);

        if (currentRetryOptionsHash != _retryOptionsHash)
        {
            throw new InvalidOperationException(
                $"Context immutability violation detected for node '{_nodeId}': " +
                "Retry options were modified during node execution. " +
                "When using CachedNodeExecutionContext, context state must remain immutable during node execution. " +
                "Consider creating retry options before node execution begins.");
        }

        // Check if tracer was replaced
        if (RuntimeHelpers.GetHashCode(context.Tracer) != _tracerHash)
        {
            throw new InvalidOperationException(
                $"Context immutability violation detected for node '{_nodeId}': " +
                "Tracer was replaced during node execution. " +
                "When using CachedNodeExecutionContext, the tracer instance must remain immutable.");
        }

        // Check if logger factory was replaced
        if (RuntimeHelpers.GetHashCode(context.LoggerFactory) != _loggerFactoryHash)
        {
            throw new InvalidOperationException(
                $"Context immutability violation detected for node '{_nodeId}': " +
                "Logger factory was replaced during node execution. " +
                "When using CachedNodeExecutionContext, the logger factory instance must remain immutable.");
        }

        // Check if cancellation token changed
        if (context.CancellationToken.GetHashCode() != _cancellationTokenHash)
        {
            throw new InvalidOperationException(
                $"Context immutability violation detected for node '{_nodeId}': " +
                "Cancellation token was modified during node execution. " +
                "When using CachedNodeExecutionContext, the cancellation token must remain immutable.");
        }
    }

    private static int GetCurrentRetryOptionsHash(PipelineContext context, string nodeId)
    {
        // Replicate the same retry options resolution logic used in CachedNodeExecutionContext.Create
        if (context.Items.TryGetValue(PipelineContextKeys.NodeRetryOptions(nodeId), out var specific) &&
            specific is PipelineRetryOptions nodeRetryOptions)
            return RuntimeHelpers.GetHashCode(nodeRetryOptions);

        if (context.Items.TryGetValue(PipelineContextKeys.GlobalRetryOptions, out var global))
        {
            if (global is PipelineRetryOptions globalRetryOptions)
                return RuntimeHelpers.GetHashCode(globalRetryOptions);
        }

        return RuntimeHelpers.GetHashCode(context.RetryOptions);
    }
}
