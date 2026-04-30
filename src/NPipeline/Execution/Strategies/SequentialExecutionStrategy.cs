using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Lineage;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Sampling;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     Sequential single-threaded execution strategy.
///     Ensures PipelineContext.CurrentNodeId is set to a transform node id while processing each item
///     and restored afterward so downstream nodes (e.g. sinks) don't overwrite attribution for the transform stage.
/// </summary>
public sealed class SequentialExecutionStrategy : IExecutionStrategy
{
    /// <summary>
    ///     Shared singleton instance for the stateless sequential strategy.
    /// </summary>
    public static SequentialExecutionStrategy Instance { get; } = new();

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
                    var attempt = 0;
                    var effectiveRetries = cached.RetryOptions;

                    var produced = false;
                    TOut? output = default;
                    Exception? originalException;
                    var shouldSkipToNextItem = false;
                    LineageOutcomeReason? terminalOutcome = null;

                    while (!shouldSkipToNextItem)
                    {
                        try
                        {
                            var work = valueTaskTransform?.ExecuteValueTaskAsync(item, context, ct)
                                       ?? new ValueTask<TOut>(node.TransformAsync(item, context, ct));

                            output = await work.ConfigureAwait(false);
                            produced = true;
                            break; // success
                        }
                        catch (Exception ex)
                        {
                            originalException = ex;
                            itemActivity?.RecordException(ex);

                            if (node.ErrorHandler is not INodeErrorHandler<ITransformNode<TIn, TOut>, TIn> typedHandler)
                            {
                                // No handler or wrong type, rethrow original exception
                                PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, ex, attempt);
                                RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, LineageOutcomeReason.Error, attempt);
                                throw;
                            }

                            var attribution = FailureAttributionResolver.Resolve(ex, context, nodeId, attempt);
                            var failureContext = new NodeFailureContext(ex, context, attribution, attempt);
                            var decision = await typedHandler.HandleAsync(node, item, failureContext, ct);

                            // Handle by error decision using pattern matching
                            var (producedValue, shouldGotoAfterItem, decisionOutcome) = decision switch
                            {
                                NodeErrorDecision.Skip => HandleSkip(),
                                NodeErrorDecision.DeadLetter => await HandleRedirect(context, nodeId, item, ex, attribution, ct),
                                NodeErrorDecision.Retry => HandleRetry(ref attempt, effectiveRetries.MaxItemRetries, itemActivity),
                                NodeErrorDecision.Fail => throw RecordAndReturn(originalException!, LineageOutcomeReason.Error, attempt),
                                _ => throw new InvalidOperationException($"Error handling failed for node {nodeId}", ex),
                            };

                            if (shouldGotoAfterItem)
                            {
                                produced = producedValue;
                                shouldSkipToNextItem = true;
                                terminalOutcome = decisionOutcome;
                            }

                            // Local functions for handling each decision type
                            static (bool Produced, bool ShouldGotoAfterItem, LineageOutcomeReason? Outcome) HandleSkip()
                            {
                                return (false, true, LineageOutcomeReason.FilteredOut);
                            }

                            static async Task<(bool Produced, bool ShouldGotoAfterItem, LineageOutcomeReason? Outcome)> HandleRedirect(
                                PipelineContext ctx, string id, TIn itm, Exception exception, NodeFailureAttribution attribution, CancellationToken token)
                            {
                                if (ctx.DeadLetterSink is not null)
                                {
                                    var envelope = new DeadLetterEnvelope(itm!, exception, attribution);
                                    await ctx.DeadLetterSink.HandleAsync(envelope, ctx, token).ConfigureAwait(false);
                                }

                                return (false, true, LineageOutcomeReason.DeadLettered);
                            }

                            (bool Produced, bool ShouldGotoAfterItem, LineageOutcomeReason? Outcome) HandleRetry(
                                ref int att, int maxRetries, IPipelineActivity? activity)
                            {
                                att++;

                                if (att > maxRetries)
                                {
                                    throw RecordAndReturn(
                                        new InvalidOperationException($"An item failed to process after {att} attempts.", originalException!),
                                        LineageOutcomeReason.Error,
                                        maxRetries);
                                }

                                activity?.SetTag("retry.attempt", att.ToString());
                                return (false, false, null);
                            }

                            Exception RecordAndReturn(Exception exception, LineageOutcomeReason outcomeReason, int retryCount)
                            {
                                PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, exception, retryCount);
                                RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, outcomeReason, retryCount);
                                return exception;
                            }
                        }
                    }

                    if (produced)
                    {
                        // Track item emitted
                        observabilityScope?.IncrementEmitted();
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, LineageOutcomeReason.Emitted, attempt);
                        yield return output!;
                    }
                    else if (terminalOutcome is not null)
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, terminalOutcome.Value, attempt);
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

        static void RecordLineageOutcome(bool hasLineageIndex, long lineageInputIndex, PipelineContext context, string nodeId,
            LineageOutcomeReason outcomeReason, int retryCount)
        {
            if (!hasLineageIndex)
                return;

            var normalizedRetryCount = Math.Max(0, retryCount);
            LineageNodeOutcomeRegistry.Record(context.PipelineId, nodeId, lineageInputIndex, outcomeReason, normalizedRetryCount);
        }
    }
}
