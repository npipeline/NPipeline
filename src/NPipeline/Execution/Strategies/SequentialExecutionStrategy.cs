using System.Runtime.CompilerServices;
using System.Diagnostics;
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
///     Sequential single-threaded execution strategy: one item at a time, in order.
/// </summary>
public sealed class SequentialExecutionStrategy : IExecutionStrategy
{
    /// <summary>
    ///     The strategy used when a node's graph definition configures none. The type holds no per-run state, so one
    ///     instance serves every node and every run.
    /// </summary>
    internal static readonly SequentialExecutionStrategy Instance = new();

    private readonly IPerItemRetryExecutor _perItemRetryExecutor;

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
        string nodeId,
        CancellationToken cancellationToken)
    {

        // Create cached execution context once per node (optimization: reduces per-item dictionary lookups)
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Create immutability guard (DEBUG-only validation, zero overhead in RELEASE)
        var immutabilityGuard = PipelineContextImmutabilityGuard.Create(context, cached);

        // Use Task.FromResult for already-completed synchronous result
        return Task.FromResult<IDataStream<TOut>>(new DataStream<TOut>(Iterate(cancellationToken)));

        async IAsyncEnumerable<TOut> Iterate([EnumeratorCancellation] CancellationToken ct)
        {
            var tracer = context.Observability.Tracer;
            var nodeId = cached.NodeId;
            var lineageTrackingEnabled = LineageNodeOutcomeRegistry.IsTracking(context.RunIdentity.PipelineId, nodeId);
            long fallbackInputIndex = -1;
            using var observabilityScope = context.NodeEnvironment.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);
            var timedInput = NPipeline.Execution.NodeTimingDataStreamWrapper.WrapInputWait(input, observabilityScope);

            #pragma warning disable CA2007

            // CA2007 false positive: the enumerator comes from a ConfigureAwait(false) sequence, so its

            // MoveNextAsync and DisposeAsync already return configured awaitables - the analyzer only

            // recognises ConfigureAwait applied directly to the await using expression.

            await using var inputEnumerator = timedInput.WithCancellation(ct).ConfigureAwait(false).GetAsyncEnumerator();

            #pragma warning restore CA2007

            while (true)
            {
                TIn item;

                try
                {
                    if (!await inputEnumerator.MoveNextAsync())
                        break;

                    item = inputEnumerator.Current;
                }
                catch (Exception ex)
                {
                    observabilityScope.RecordFailure(ex);
                    throw;
                }

                // Track item processed
                observabilityScope.IncrementProcessed();

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

                var produced = false;
                TOut? output = default;

                try
                {
                    var workStart = Stopwatch.GetTimestamp();
                    var executionResult = await _perItemRetryExecutor.ExecuteWithRetryAsync(
                            item,
                            node,
                            context,
                            nodeId,
                            cached.Resilience,
                            hasLineageIndex,
                            lineageInputIndex,
                            cached.LineageOutcomeWriter,
                            itemActivity,
                            ct,
                            circuitBreaker: cached.CircuitBreaker)
                        .ConfigureAwait(false);
                    observabilityScope.AddWork(Stopwatch.GetElapsedTime(workStart));
                    produced = executionResult.Produced;
                    output = executionResult.Output;
                }
                catch (Exception ex)
                {
                    observabilityScope.RecordFailure(ex);
                    throw;
                }

                if (!produced)
                    continue;

                // Track item emitted
                observabilityScope.IncrementEmitted();
                yield return output!;
            }

            // Validate context immutability after processing all items (DEBUG-only, zero overhead in RELEASE)
            immutabilityGuard.Validate(context);
        }
    }
}
