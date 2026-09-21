using NPipeline.Execution.Lineage;
using NPipeline.Execution.Annotations;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
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
        PipelineContext context,
        string nodeId,
        int maxItemRetries,
        bool hasLineageIndex,
        long lineageInputIndex,
        LineageNodeOutcomeWriter lineageOutcomeWriter,
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
                var output = await node.TransformAsync(item, context, cancellationToken).ConfigureAwait(false);
                RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Emitted, attempt);

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
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.FilteredOut, attempt);
                        return ItemExecutionResult<TOut>.Skipped(attempt);

                    case ResilienceDecision.DeadLetter:
                        await TryDispatchDeadLetterAsync(item, ex, context, nodeId, attempt, cancellationToken).ConfigureAwait(false);
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.DeadLettered, attempt);
                        return ItemExecutionResult<TOut>.DeadLettered(attempt);

                    case ResilienceDecision.Retry:
                        attempt++;

                        if (attempt > maxItemRetries)
                        {
                            var exhausted = new InvalidOperationException(
                                $"An item failed to process after {attempt} attempts.",
                                ex);

                            PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, exhausted, maxItemRetries);
                            RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error, maxItemRetries);
                            throw exhausted;
                        }

                        itemActivity?.SetTag("retry.attempt", attempt.ToString());

                        // Back off before retrying. Without this the configured delay strategy - exponential
                        // backoff, jitter, the composite - is inert for item-level retries and the pipeline spins
                        // against an already-struggling dependency as fast as the CPU allows.
                        await ApplyRetryDelayAsync(policy, context, nodeId, attempt, cancellationToken).ConfigureAwait(false);
                        continue;

                    case ResilienceDecision.Fail:
                        PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, ex, attempt);
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error, attempt);
                        throw;

                    default:
                        throw new InvalidOperationException($"Error handling failed for node {nodeId} with decision {decision}.", ex);
                }
            }
        }
    }

    private static async Task ApplyRetryDelayAsync(
        IResiliencePolicy policy,
        PipelineContext context,
        string nodeId,
        int attempt,
        CancellationToken cancellationToken)
    {
        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(PerItemRetryExecutor));
        TimeSpan delay;

        try
        {
            delay = await policy.GetRetryDelayAsync(context, attempt, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            // A broken delay strategy must not break the retry itself. Cancellation of the pipeline's own token is
            // excluded by the filter so it propagates rather than being swallowed here.
            PerItemRetryExecutorLogMessages.RetryDelayFailed(logger, ex, nodeId);
            return;
        }

        if (delay <= TimeSpan.Zero)
            return;

        PerItemRetryExecutorLogMessages.ApplyingRetryDelay(logger, delay.TotalMilliseconds, nodeId, attempt);

        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
    }

    private static IResiliencePolicy ResolveResiliencePolicy(PipelineContext context, string nodeId)
    {
        var key = ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId);

        if (context.NodeEnvironment.NodeExecutionScopeRegistry.TryGetRuntimeAnnotation(key, out var annotation) && annotation is IResiliencePolicy nodePolicy)
            return nodePolicy;

        return context.ExecutionConfiguration.ResiliencePolicy;
    }

    private static void RecordLineageOutcome(
        bool hasLineageIndex,
        long lineageInputIndex,
        in LineageNodeOutcomeWriter lineageOutcomeWriter,
        PipelineContext context,
        string nodeId,
        LineageOutcomeReason outcomeReason,
        int retryCount)
    {
        if (!hasLineageIndex)
            return;

        // Fast path: write through the node-scoped writer resolved once for this execution,
        // skipping the per-item (pipeline, node) registry lookup. Retry-count normalization
        // (Math.Max(0, ...)) is applied inside the registry for both paths.
        if (lineageOutcomeWriter.IsActive)
        {
            lineageOutcomeWriter.Record(lineageInputIndex, outcomeReason, retryCount);
            return;
        }

        LineageNodeOutcomeRegistry.Record(context.RunIdentity.PipelineId, nodeId, lineageInputIndex, outcomeReason, retryCount);
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
