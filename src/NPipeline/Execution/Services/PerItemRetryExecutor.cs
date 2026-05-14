using NPipeline.Execution.Lineage;
using NPipeline.Execution.Annotations;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Resilience;
using NPipeline.Sampling;

namespace NPipeline.Execution.Services;

internal sealed class PerItemRetryExecutor : IPerItemRetryExecutor
{
    public static PerItemRetryExecutor Instance { get; } = new();

    public async Task<ItemExecutionResult<TOut>> ExecuteWithRetryAsync<TIn, TOut>(
        TIn item,
        ITransformNode<TIn, TOut> node,
        IValueTaskTransform<TIn, TOut>? valueTaskTransform,
        PipelineContext context,
        string nodeId,
        int maxItemRetries,
        bool hasLineageIndex,
        long lineageInputIndex,
        IPipelineActivity? itemActivity,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeId);

        if (maxItemRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(maxItemRetries), "maxItemRetries must be greater than or equal to zero.");

        var attempt = 0;

        while (true)
        {
            try
            {
                var work = valueTaskTransform?.ExecuteValueTaskAsync(item, context, cancellationToken)
                           ?? new ValueTask<TOut>(node.TransformAsync(item, context, cancellationToken));

                var output = await work.ConfigureAwait(false);
                RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, LineageOutcomeReason.Emitted, attempt);

                return ItemExecutionResult<TOut>.Emitted(output, attempt);
            }
            catch (Exception ex)
            {
                itemActivity?.RecordException(ex);

                var policy = ResolveResiliencePolicy(context, nodeId);

                var decision = await policy
                    .DecideItemFailureAsync(node, item, ex, context, nodeId, attempt, cancellationToken)
                    .ConfigureAwait(false);

                switch (decision)
                {
                    case ResilienceDecision.Skip:
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, LineageOutcomeReason.FilteredOut, attempt);
                        return ItemExecutionResult<TOut>.Skipped(attempt);

                    case ResilienceDecision.DeadLetter:
                        await TryDispatchDeadLetterAsync(item, ex, context, nodeId, attempt, cancellationToken).ConfigureAwait(false);
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, LineageOutcomeReason.DeadLettered, attempt);
                        return ItemExecutionResult<TOut>.DeadLettered(attempt);

                    case ResilienceDecision.Retry:
                        attempt++;

                        if (attempt > maxItemRetries)
                        {
                            var exhausted = new InvalidOperationException(
                                $"An item failed to process after {attempt} attempts.",
                                ex);

                            PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, exhausted, maxItemRetries);
                            RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, LineageOutcomeReason.Error, maxItemRetries);
                            throw exhausted;
                        }

                        itemActivity?.SetTag("retry.attempt", attempt.ToString());
                        continue;

                    case ResilienceDecision.Fail:
                        PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, ex, attempt);
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, context, nodeId, LineageOutcomeReason.Error, attempt);
                        throw;

                    default:
                        throw new InvalidOperationException($"Error handling failed for node {nodeId} with decision {decision}.", ex);
                }
            }
        }
    }

    private static IResiliencePolicy ResolveResiliencePolicy(PipelineContext context, string nodeId)
    {
        var key = ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId);

        if (context.NodeExecutionScopeRegistry.TryGetRuntimeAnnotation(key, out var annotation) && annotation is IResiliencePolicy nodePolicy)
            return nodePolicy;

        return context.ResiliencePolicy;
    }

    private static void RecordLineageOutcome(
        bool hasLineageIndex,
        long lineageInputIndex,
        PipelineContext context,
        string nodeId,
        LineageOutcomeReason outcomeReason,
        int retryCount)
    {
        if (!hasLineageIndex)
            return;

        var normalizedRetryCount = Math.Max(0, retryCount);
        LineageNodeOutcomeRegistry.Record(context.PipelineId, nodeId, lineageInputIndex, outcomeReason, normalizedRetryCount);
    }

    private static async Task TryDispatchDeadLetterAsync<TIn>(
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        if (context.DeadLetterSink is null)
            return;

        var attribution = FailureAttributionResolver.Resolve(exception, context, nodeId, retryAttempt);
        var envelope = new DeadLetterEnvelope(failedItem!, exception, attribution);
        await context.DeadLetterSink.HandleAsync(envelope, context, cancellationToken).ConfigureAwait(false);
    }
}
