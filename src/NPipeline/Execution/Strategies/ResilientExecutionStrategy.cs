using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     Node restart (L2): runs a transform node through a resumable strategy, and when the node's stream fails,
///     restarts it from its checkpoint for as long as the resilience policy answers
///     <see cref="ResilienceDecision.RestartNode" />.
/// </summary>
/// <remarks>
///     <para>
///         The builder wraps a transform in this strategy when its <see cref="NodeRestartOptions.MaxRestarts" /> is
///         above zero. The inner strategy is the node's configured one, or when it has none, the node's default
///         (<see cref="IExecutionStrategyProvider" />) or <see cref="SequentialExecutionStrategy" />. It must implement
///         <see cref="IResumableExecutionStrategy" />.
///     </para>
///     <para>
///         A restart resumes at the checkpoint: the first input item whose outcome has not been delivered. Items from
///         the checkpoint to the read head are held for replay, bounded by <see cref="NodeRestartOptions.MaxReplayWindow" />,
///         which pauses reading when reached. With an in-order inner strategy each output is delivered exactly once;
///         see <see cref="IResumableExecutionStrategy" />. Work inside <c>TransformAsync</c> for items in flight at the
///         failure is done again.
///     </para>
///     <para>
///         A failure of the input itself is not restarted: the input cannot be read again, so the failure propagates.
///         Cancelling the pipeline's token throws <see cref="OperationCanceledException" /> and never uses a restart.
///     </para>
/// </remarks>
internal sealed class ResilientExecutionStrategy(IExecutionStrategy? innerStrategy) : IExecutionStrategy
{
    /// <summary>
    ///     The strategy the node runs through between restarts, or <see langword="null" /> to use the node's default.
    /// </summary>
    public IExecutionStrategy? InnerStrategy { get; } = innerStrategy;

    /// <inheritdoc />
    public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
        string nodeId, CancellationToken cancellationToken)
    {
        var inner = InnerStrategy
                    ?? (node as IExecutionStrategyProvider)?.DefaultExecutionStrategy
                    ?? SequentialExecutionStrategy.Instance;

        if (inner is not IResumableExecutionStrategy resumable)
            throw new InvalidOperationException(ErrorMessages.NodeRestartRequiresResumableStrategy(nodeId, inner.GetType().Name));

        // Captured now: the stream is enumerated by a downstream consumer, long after this call returned.
        var options = context.ExecutionConfiguration.GetResilienceOptions(nodeId);
        var policy = ResilienceRuntime.ResolvePolicy(context, nodeId);

        IDataStream<TOut> stream = new DataStream<TOut>(RunAsync(input, resumable, node, context, nodeId, options, policy, cancellationToken));
        context.RegisterForDisposal(stream);
        return Task.FromResult(stream);
    }

    /// <summary>
    ///     Enumerates the node's stream, restarting it from its checkpoint for as long as the policy asks.
    /// </summary>
    private static async IAsyncEnumerable<TOut> RunAsync<TIn, TOut>(IDataStream<TIn> source, IResumableExecutionStrategy inner,
        ITransformNode<TIn, TOut> node, PipelineContext context, string nodeId, PipelineResilienceOptions options, IResiliencePolicy policy,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Spans the whole run, so the restart activity ends after the node's last item rather than when ExecuteAsync
        // returns. The null tracer returns a no-op activity, so this needs no null check.
        using var resilientActivity = context.Observability.Tracer.StartActivity("Node.Resilience");
        resilientActivity.SetTag("resilience.enabled", true);

        // Held across every attempt, so a failed attempt's disposal does not unregister the scope the restart needs.
        using var nodeScope = context.NodeEnvironment.NodeExecutionScopeRegistry.BeginNodeScope(nodeId);

        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(ResilientExecutionStrategy));
        var restartOptions = options.NodeRestart;

        // Created here rather than in ExecuteAsync, so that each enumeration of the returned stream reads the input afresh.
        var input = new ResumableInput<TIn>(source, restartOptions.MaxReplayWindow, cancellationToken);

        await using (input.ConfigureAwait(false))
        {
            // restarts == restarts counted against MaxRestarts; the run in progress is restarts + 1.
            var restarts = 0;
            var totalRestarts = 0;
            long delivered = 0;
            long deliveredSinceRestart = 0;

            while (true)
            {
                // Cancellation must surface as an OperationCanceledException. Ending the stream instead would report a
                // cancelled run as a success with a silently truncated result set.
                cancellationToken.ThrowIfCancellationRequested();

                var (runInput, checkpoint) = input.Open(out var offset);
                var stream = await inner.ExecuteFromAsync(runInput, offset, checkpoint, node, context, nodeId, cancellationToken).ConfigureAwait(false);

#pragma warning disable CA2007

                // CA2007 false positive: the enumerator comes from a ConfigureAwait(false) sequence, so its
                // MoveNextAsync and DisposeAsync already return configured awaitables - the analyzer only
                // recognises ConfigureAwait applied directly to the await using expression.
                await using var enumerator = stream.WithCancellation(cancellationToken).ConfigureAwait(false).GetAsyncEnumerator();
#pragma warning restore CA2007

                var restart = false;

                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    TOut current;

                    try
                    {
                        if (!await enumerator.MoveNextAsync())
                            yield break;

                        current = enumerator.Current;
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        // Cancellation of the pipeline's own token is not a node failure: it must not use a restart or be
                        // rewritten into a RetryExhaustedException.
                        throw;
                    }
                    catch (Exception ex)
                    {
                        resilientActivity.RecordException(ex);

                        // The input itself failed. It cannot be read again, so restarting the node cannot help.
                        if (input.InputFault is { } inputFault)
                            ExceptionDispatchInfo.Throw(inputFault);

                        var decision = await policy.DecideRestartAsync(new StreamFailure
                        {
                            NodeId = nodeId,
                            Exception = ex,
                            Attempt = restarts + 1,
                            MaxRestarts = restartOptions.MaxRestarts,
                            Checkpoint = checkpoint.Watermark,
                            Delivered = delivered,
                            Context = context,
                        }, cancellationToken).ConfigureAwait(false);

                        ResilientExecutionStrategyLogMessages.ErrorHandlerDecision(logger, decision.ToString(), nodeId, restarts, checkpoint.Watermark);

                        if (decision == ResilienceDecision.ContinueWithoutNode)
                            yield break;

                        if (decision != ResilienceDecision.RestartNode)
                        {
                            RecordDiagnostics(context, nodeId, totalRestarts);

                            if (totalRestarts == 0)
                                throw;

                            ResilientExecutionStrategyLogMessages.RetryExhausted(logger, nodeId, totalRestarts + 1);
                            ResilienceRuntime.ReportRetryExhausted(context, nodeId, RetryKind.NodeRestart, totalRestarts + 1, ex);
                            throw new RetryExhaustedException(nodeId, totalRestarts + 1, ex);
                        }

                        if (totalRestarts >= ResilienceRuntime.MaxPolicyRepeats)
                            throw ResilienceRuntime.RepeatCeilingExceeded(policy, nodeId, decision, ex);

                        restarts++;
                        totalRestarts++;
                        deliveredSinceRestart = 0;

                        var delay = restartOptions.Backoff.DelayFor(restarts);

                        if (delay > TimeSpan.Zero)
                        {
                            ResilientExecutionStrategyLogMessages.ApplyingRetryDelay(logger, delay.TotalMilliseconds, nodeId, restarts);
                            await Task.Delay(delay, options.Time, cancellationToken).ConfigureAwait(false);
                        }

                        ResilienceRuntime.ReportRetry(context, nodeId, RetryKind.NodeRestart, restarts, ex);

                        // The attempt failed, but the node is being restarted; a successful replay must not be
                        // reported as failed just because the first attempt threw.
                        nodeScope.ClearFailure();

                        restart = true;
                        break;
                    }

                    delivered++;

                    // A long-running stream with rare faults gets its restarts back once it has run cleanly for a while.
                    if (restarts > 0 && restartOptions.ResetAfterItems is { } resetAfter && ++deliveredSinceRestart >= resetAfter)
                    {
                        restarts = 0;
                        deliveredSinceRestart = 0;
                    }

                    yield return current;
                }

                if (!restart)
                    yield break;
            }
        }
    }

    private static void RecordDiagnostics(PipelineContext context, string nodeId, int restarts)
    {
        var registry = context.NodeEnvironment.NodeExecutionScopeRegistry;
        registry.SetRuntimeAnnotation(PipelineContextKeys.DiagnosticsResilienceFailures(nodeId), restarts);
        registry.SetRuntimeAnnotation(PipelineContextKeys.DiagnosticsResilienceThrowingOnFailure(nodeId), true);
    }
}
