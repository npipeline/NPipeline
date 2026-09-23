using System.Globalization;
using NPipeline.Execution.Lineage;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Reliability;
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
        PipelineResilienceOptions options,
        bool hasLineageIndex,
        long lineageInputIndex,
        LineageNodeOutcomeWriter lineageOutcomeWriter,
        IPipelineActivity? itemActivity,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(options);

        // attempt is the 1-based number of the attempt being made; retries so far is attempt - 1.
        var attempt = 1;

        while (true)
        {
            try
            {
                var output = await node.TransformAsync(item, context, cancellationToken).ConfigureAwait(false);
                RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Emitted, attempt - 1);

                return ItemExecutionResult<TOut>.Emitted(output, attempt - 1);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Cancelling the pipeline is not an item failure: it must not be retried, skipped, or dead-lettered.
                throw;
            }
            catch (Exception ex)
            {
                itemActivity?.RecordException(ex);

                var policy = ResilienceRuntime.ResolvePolicy(context, nodeId);

                var failure = new ItemFailure<TIn>
                {
                    Item = item,
                    Node = node,
                    NodeId = nodeId,
                    Exception = ex,
                    Attempt = attempt,
                    MaxRetries = options.ItemRetry.MaxRetries,
                    IsTransient = options.ItemRetry.Classifier.IsTransient(ex, cancellationToken),
                    Context = context,
                };

                var decision = await policy.DecideItemFailureAsync(failure, cancellationToken).ConfigureAwait(false);
                var retries = attempt - 1;

                switch (decision)
                {
                    case ResilienceDecision.Skip:
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.FilteredOut, retries);
                        return ItemExecutionResult<TOut>.Skipped(retries);

                    case ResilienceDecision.DeadLetter:
                        if (context.DeadLetterSink is null)
                        {
                            // Dropping the item here would lose it silently while lineage claimed it was dead-lettered.
                            var noSink = new DeadLetterSinkNotConfiguredException(nodeId, ex);
                            PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, noSink, retries);
                            RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error, retries);
                            throw noSink;
                        }

                        await DispatchDeadLetterAsync(context.DeadLetterSink, item, ex, context, nodeId, retries, cancellationToken).ConfigureAwait(false);
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.DeadLettered, retries);
                        return ItemExecutionResult<TOut>.DeadLettered(retries);

                    case ResilienceDecision.Retry:
                        if (attempt > ResilienceRuntime.MaxPolicyRepeats)
                        {
                            var runaway = ResilienceRuntime.RepeatCeilingExceeded(policy, nodeId, decision, ex);
                            PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, runaway, retries);
                            RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error, retries);
                            throw runaway;
                        }

                        itemActivity?.SetTag("retry.attempt", attempt.ToString(CultureInfo.InvariantCulture));

                        context.Observability.ExecutionObserver.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, attempt, ex,
                            context.RunIdentity.PipelineId, context.RunIdentity.PipelineName));

                        await WaitBeforeRetryAsync(options, context, nodeId, attempt, cancellationToken).ConfigureAwait(false);
                        attempt++;
                        continue;

                    case ResilienceDecision.Fail:
                        if (retries > 0)
                        {
                            var exhausted = new RetryExhaustedException(nodeId, attempt, ex);
                            PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, exhausted, retries);
                            RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error, retries);
                            throw exhausted;
                        }

                        PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, ex, retries);
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error, retries);
                        throw;

                    default:
                        throw new InvalidOperationException($"Error handling failed for node {nodeId} with decision {decision}.", ex);
                }
            }
        }
    }

    private static async Task WaitBeforeRetryAsync(
        PipelineResilienceOptions options,
        PipelineContext context,
        string nodeId,
        int retry,
        CancellationToken cancellationToken)
    {
        var delay = options.ItemRetry.Backoff.DelayFor(retry);

        if (delay <= TimeSpan.Zero)
            return;

        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(PerItemRetryExecutor));
        PerItemRetryExecutorLogMessages.ApplyingRetryDelay(logger, delay.TotalMilliseconds, nodeId, retry);

        await Task.Delay(delay, options.Time, cancellationToken).ConfigureAwait(false);
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

    private static async Task DispatchDeadLetterAsync<TIn>(
        IDeadLetterSink deadLetterSink,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        var attribution = FailureAttributionResolver.Resolve(exception, context, nodeId, retryAttempt);
        var envelope = new DeadLetterEnvelope(failedItem!, exception, attribution);
        await deadLetterSink.HandleAsync(envelope, context, cancellationToken).ConfigureAwait(false);
    }
}
