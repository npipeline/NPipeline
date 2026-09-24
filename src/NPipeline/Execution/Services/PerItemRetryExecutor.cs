using System.Globalization;
using Microsoft.Extensions.Logging;
using NPipeline.ErrorHandling;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Lineage;
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
        CancellationToken cancellationToken,
        Action<int>? onRetry = null,
        CircuitBreaker? circuitBreaker = null)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(options);

        // attempt is the 1-based number of the attempt being made; retries so far is attempt - 1.
        var attempt = 1;

        while (true)
        {
            // The breaker admits each attempt (L1). A refused attempt is reported to the policy like a failed one,
            // with IsBreakerOpen set.
            BreakerPermit permit = default;
            var admitted = false;

            try
            {
                if (circuitBreaker is not null)
                {
                    permit = await CircuitBreakerGate.AcquireAsync(circuitBreaker, context, cancellationToken).ConfigureAwait(false);
                    admitted = true;
                }

                var output = await node.TransformAsync(item, context, cancellationToken).ConfigureAwait(false);

                if (admitted)
                {
                    admitted = false;
                    CircuitBreakerGate.RecordSuccess(circuitBreaker!, permit, context);
                }

                RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Emitted, attempt - 1);

                return ItemExecutionResult<TOut>.Emitted(output, attempt - 1);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Cancelling the pipeline is not an item failure: it must not be retried, skipped, or dead-lettered, and
                // says nothing about the dependency's health.
                if (admitted)
                    circuitBreaker!.Release(permit);

                throw;
            }
            catch (Exception ex)
            {
                itemActivity?.RecordException(ex);
                PerItemRetryExecutorLogMessages.AttemptFailed(CreateLogger(context), ex, nodeId, attempt);

                var refusedByBreaker = circuitBreaker is not null && !admitted && ex is CircuitBreakerOpenException;
                var isTransient = !refusedByBreaker && options.ItemRetry.Classifier.IsTransient(ex, cancellationToken);

                // Only a transient failure counts against the breaker: a permanent one says nothing about the dependency.
                if (admitted)
                    CircuitBreakerGate.RecordFailure(circuitBreaker!, permit, isTransient, context);

                var policy = ResilienceRuntime.ResolvePolicy(context, nodeId);

                var failure = new ItemFailure<TIn>
                {
                    Item = item,
                    Node = node,
                    NodeId = nodeId,
                    Exception = ex,
                    Attempt = attempt,
                    MaxRetries = options.ItemRetry.MaxRetries,
                    IsTransient = isTransient,
                    IsBreakerOpen = refusedByBreaker,
                    Context = context,
                };

                var decision = await policy.DecideItemFailureAsync(failure, cancellationToken).ConfigureAwait(false);
                var retries = attempt - 1;

                switch (decision)
                {
                    case ResilienceDecision.Skip:
                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.FilteredOut,
                            retries);

                        ReportNoOutput(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, LineageOutcomeReason.FilteredOut);
                        return ItemExecutionResult<TOut>.Skipped(retries);

                    case ResilienceDecision.DeadLetter:
                        if (context.DeadLetterSink is null)
                        {
                            // Dropping the item here would lose it silently while lineage claimed it was dead-lettered.
                            var noSink = new DeadLetterSinkNotConfiguredException(nodeId, ex);
                            RecordErrorSample(context, nodeId, item, noSink, retries, hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter);

                            RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error,
                                retries);

                            throw noSink;
                        }

                        await DispatchDeadLetterAsync(context.DeadLetterSink, item, ex, context, nodeId, retries, cancellationToken).ConfigureAwait(false);

                        RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.DeadLettered,
                            retries);

                        ReportNoOutput(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, LineageOutcomeReason.DeadLettered);
                        return ItemExecutionResult<TOut>.DeadLettered(retries);

                    case ResilienceDecision.Retry:
                        if (attempt > ResilienceRuntime.MaxPolicyRepeats)
                        {
                            var runaway = ResilienceRuntime.RepeatCeilingExceeded(policy, nodeId, decision, ex);
                            RecordErrorSample(context, nodeId, item, runaway, retries, hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter);

                            RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error,
                                retries);

                            throw runaway;
                        }

                        itemActivity?.SetTag("retry.attempt", attempt.ToString(CultureInfo.InvariantCulture));
                        onRetry?.Invoke(attempt);
                        ResilienceRuntime.ReportRetry(context, nodeId, RetryKind.ItemRetry, attempt, ex);

                        await WaitBeforeRetryAsync(options, context, nodeId, attempt, cancellationToken).ConfigureAwait(false);
                        attempt++;
                        continue;

                    case ResilienceDecision.Fail:
                        if (retries > 0)
                        {
                            var exhausted = new RetryExhaustedException(nodeId, attempt, ex);
                            RecordErrorSample(context, nodeId, item, exhausted, retries, hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter);

                            RecordLineageOutcome(hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter, context, nodeId, LineageOutcomeReason.Error,
                                retries);

                            ResilienceRuntime.ReportRetryExhausted(context, nodeId, RetryKind.ItemRetry, attempt, ex);
                            throw exhausted;
                        }

                        RecordErrorSample(context, nodeId, item, ex, retries, hasLineageIndex, lineageInputIndex, in lineageOutcomeWriter);
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

        PerItemRetryExecutorLogMessages.ApplyingRetryDelay(CreateLogger(context), delay.TotalMilliseconds, nodeId, retry);

        await Task.Delay(delay, options.Time, cancellationToken).ConfigureAwait(false);
    }

    private static ILogger CreateLogger(PipelineContext context) => context.Observability.LoggerFactory.CreateLogger(nameof(PerItemRetryExecutor));

    /// <summary>
    ///     Records the failure as an error sample, correlated to the item through the lineage registered under its input
    ///     index. Without lineage there is nothing to correlate it to, so nothing is recorded.
    /// </summary>
    private static void RecordErrorSample<TIn>(
        PipelineContext context,
        string nodeId,
        TIn item,
        Exception exception,
        int retryCount,
        bool hasLineageIndex,
        long lineageInputIndex,
        in LineageNodeOutcomeWriter lineageOutcomeWriter)
    {
        if (hasLineageIndex && lineageOutcomeWriter.TryGetInput(lineageInputIndex, out var lineage))
            PipelineSampleErrorReporter.TryRecordError(context, nodeId, item, exception, retryCount, lineage.CorrelationId, lineage.AncestryInputIndices);
    }

    /// <summary>
    ///     Reports that the item ends here without an output, so the lineage mapper records it as ended instead of
    ///     waiting for an output. An emitted item is reported by the strategy, where it yields the output.
    /// </summary>
    private static void ReportNoOutput(bool hasLineageIndex, long lineageInputIndex, in LineageNodeOutcomeWriter lineageOutcomeWriter,
        LineageOutcomeReason outcome)
    {
        if (hasLineageIndex)
            lineageOutcomeWriter.ReportDone(lineageInputIndex, outcome);
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
