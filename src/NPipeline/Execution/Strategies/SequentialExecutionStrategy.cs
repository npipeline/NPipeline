using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Services;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     Sequential single-threaded execution strategy.
///     Ensures PipelineContext.CurrentNodeId is set to a transform node id while processing each item
///     and restored afterward so downstream nodes (e.g. sinks) don't overwrite attribution for the transform stage.
/// </summary>
public sealed class SequentialExecutionStrategy : IExecutionStrategy
{
    private readonly IPerItemRetryExecutor _perItemRetryExecutor;

    /// <summary>
    ///     Shared singleton instance for the stateless sequential strategy.
    /// </summary>
    public static SequentialExecutionStrategy Instance { get; } = new();

    /// <summary>
    ///     Initializes a new instance of <see cref="SequentialExecutionStrategy" />.
    /// </summary>
    public SequentialExecutionStrategy() : this(PerItemRetryExecutor.Instance)
    {
    }

    internal SequentialExecutionStrategy(IPerItemRetryExecutor perItemRetryExecutor)
    {
        _perItemRetryExecutor = perItemRetryExecutor ?? throw new ArgumentNullException(nameof(perItemRetryExecutor));
    }

    /// <inheritdoc />
    public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var nodeId = context.CurrentNodeId; // capture id for this node once
        var valueTaskTransform = node as IValueTaskTransform<TIn, TOut>;

        // Create cached execution context once per node (optimization: reduces per-item dictionary lookups)
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Create immutability guard (DEBUG-only validation, zero overhead in RELEASE)
        var immutabilityGuard = PipelineContextImmutabilityGuard.Create(context, cached);

        // Use Task.FromResult for already-completed synchronous result
        return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(Iterate(cancellationToken)));

        async IAsyncEnumerable<TOut> Iterate([EnumeratorCancellation] CancellationToken ct)
        {
            var tracer = context.Tracer;
            var nodeId = cached.NodeId;
            var lineageTrackingEnabled = LineageNodeOutcomeRegistry.IsTracking(context.PipelineId, nodeId);
            long fallbackInputIndex = -1;

            // Get observability scope if available
            IAutoObservabilityScope? observabilityScope = null;

            if (context.NodeObservabilityScopes.TryGetValue(nodeId, out var scope))
                observabilityScope = scope;

            try
            {
                await foreach (var item in input.WithCancellation(ct).ConfigureAwait(false))
                {
                    // Track item processed
                    observabilityScope?.IncrementProcessed();

                    fallbackInputIndex++;

                    var hasLineageIndex = LineageExecutionItemContext.TryGetCurrentInputIndex(out var lineageInputIndex);

                    if (!hasLineageIndex && lineageTrackingEnabled)
                    {
                        hasLineageIndex = true;
                        lineageInputIndex = fallbackInputIndex;
                    }

                    // Use cached values to avoid per-item dictionary lookups and allocations
                    using var itemActivity = cached.TracingEnabled
                        ? tracer.StartActivity("Item.Transform")
                        : null;

                    using var _ = context.ScopedNode(cached.NodeId);
                    var executionResult = await _perItemRetryExecutor.ExecuteWithRetryAsync(
                            item,
                            node,
                            valueTaskTransform,
                            context,
                            nodeId,
                            cached.RetryOptions.MaxItemRetries,
                            hasLineageIndex,
                            lineageInputIndex,
                            itemActivity,
                            ct)
                        .ConfigureAwait(false);

                    if (!executionResult.Produced)
                        continue;

                    // Track item emitted
                    observabilityScope?.IncrementEmitted();
                    yield return executionResult.Output!;
                }
            }
            finally
            {
                // Dispose AutoObservabilityScope after all items are processed, even on failure or cancellation
                observabilityScope?.Dispose();
            }

            // Validate context immutability after processing all items (DEBUG-only, zero overhead in RELEASE)
            immutabilityGuard.Validate(context);
        }
    }
}
