using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     An execution strategy that wraps another strategy to provide resilience against stream failures.
///     <para>
///         If the underlying stream fails, this strategy consults the configured <see cref="IResiliencePolicy" />
///         to decide whether to restart the node, continue without it, or fail the pipeline.
///     </para>
///     <para>
///         Performance considerations:
///         - Materializes streaming inputs only when necessary for resilience
///         - Uses CappedReplayableDataStream for efficient restart support
///         - Implements circuit breaker pattern to prevent cascading failures
///         - Restarts as often as the resilience policy asks, waiting the node's NodeRestart.Backoff between runs
///     </para>
/// </summary>
/// <remarks>
///     <para>
///         This strategy is particularly useful for:
///         - External service calls that may experience transient failures
///         - Database operations that might encounter connection issues
///         - Network-dependent processing that requires fault tolerance
///         - Long-running pipelines where reliability is critical
///     </para>
///     <para>
///         The resilience pattern implemented here includes:
///         - Automatic restart on failure (when configured)
///         - Circuit breaker to prevent repeated failures
///         - Restart limits and a replay cap from the node's <see cref="NodeRestartOptions" />
///         - Integration with pipeline-wide error handling
///     </para>
///     <para>
///         Circuit breaker semantics:
///         - The circuit breaker tracks consecutive failures (not total failures)
///         - A successful item production resets the consecutive failure counter
///         - The breaker trips only when consecutive failures exceed the threshold
///         - This prevents premature breaker trips due to intermittent failures
///         - The restart count is passed to the policy as <see cref="StreamFailure.Attempt" />
///     </para>
///     <para>
///         Delivery guarantee on restart — <b>at-least-once, with duplicates</b>:
///         a restart calls the stream factory again and re-yields from the beginning of the input, but items already
///         emitted downstream before the failure are not retracted. A node that fails after emitting items therefore
///         delivers those items twice. Sinks fed by a resilient node must be idempotent, or must tolerate duplicates
///         some other way (for example by deduplicating on a key).
///     </para>
///     <para>
///         Cancellation: cancelling the pipeline's token throws <see cref="OperationCanceledException" /> out of the
///         stream rather than ending it. A cancelled run never completes normally with a partial result set, and
///         cancellation neither consumes a restart attempt nor counts as a circuit-breaker failure.
///     </para>
///     <para>
///         Pattern matching enhancements:
///         - Uses C# switch expressions for efficient error decision handling
///         - Implements pattern-based circuit breaker logic
///         - Leverages tuple patterns for retry state management
///     </para>
/// </remarks>
/// <example>
///     <code>
///     // Create a resilient strategy wrapping another strategy
///     var innerStrategy = new SequentialExecutionStrategy();
///     var resilientStrategy = new ResilientExecutionStrategy(innerStrategy);
/// 
///     // Apply to a transform node
///     var node = new TransformNode&lt;TInput, TOutput&gt;(transformFunction)
///     {
///         ExecutionStrategy = resilientStrategy
///     };
///     </code>
/// </example>
public sealed class ResilientExecutionStrategy(IExecutionStrategy innerStrategy) : IExecutionStrategy
{
    /// <inheritdoc />
    /// <summary>
    ///     Executes a node with resilience capabilities, including automatic restart on failure.
    ///     <para>
    ///         This method wraps the inner strategy's execution with resilience features such as
    ///         materialization for restart support, circuit breaker functionality, and retry logic.
    ///     </para>
    /// </summary>
    /// <typeparam name="TIn">The input type of the node.</typeparam>
    /// <typeparam name="TOut">The output type of the node.</typeparam>
    /// <param name="input">The input data pipe.</param>
    /// <param name="node">The transform node to execute.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="nodeId">The id of the node being executed, passed explicitly rather than read from the shared context.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A data pipe containing the node's output with resilience capabilities.</returns>
    /// <exception cref="CircuitBreakerOpenException">Thrown when the circuit breaker is open and blocking execution.</exception>
    /// <exception cref="RetryExhaustedException">Thrown when all retry attempts are exhausted.</exception>
    /// <exception cref="OperationCanceledException">Thrown when the operation is canceled.</exception>
    /// <remarks>
    ///     <para>
    ///         The resilience process follows these steps:
    ///         1. Check if error handling is available; if not, delegate directly to inner strategy
    ///         2. Materialize streaming inputs if necessary to support restarts
    ///         3. Apply materialization caps to prevent memory issues
    ///         4. Create a resilient stream that handles failures according to the error handler's decisions
    ///         5. Implement circuit breaker logic to prevent cascading failures
    ///     </para>
    ///     <para>
    ///         Materialization is a performance trade-off that enables resilience:
    ///         - Pros: Allows restart from the beginning on failure
    ///         - Cons: Increased memory usage and potential latency
    ///         - Mitigation: Configurable caps limit memory usage
    ///     </para>
    /// </remarks>
    public async Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
        string nodeId, CancellationToken cancellationToken)
    {
        // Create a resilience activity to track and tag resilience-related telemetry
        using var resilientActivity = context.Observability.Tracer.StartActivity("Node.Resilience");
        resilientActivity.SetTag("resilience.enabled", true);

        // Captured now: the stream is enumerated by a downstream consumer, long after this call returned.
        var options = context.ExecutionConfiguration.GetResilienceOptions(nodeId);
        var policy = ResilienceRuntime.ResolvePolicy(context, nodeId);

        // With no restarts configured and no policy that could ask for one, there is nothing to replay, so the input
        // is not buffered.
        if (options.NodeRestart.MaxRestarts == 0 && policy is DefaultResiliencePolicy)
            return await innerStrategy.ExecuteAsync(input, node, context, nodeId, cancellationToken).ConfigureAwait(false);

        // A forward-only input can be read only once, so it is buffered to let a restart replay it.
        if (input is IForwardOnlyDataStream)
        {
            var cap = options.NodeRestart.MaxReplayWindow;

#pragma warning disable CA2000 // Ownership transferred to PipelineContext via RegisterForDisposal
            var replay = new CappedReplayableDataStream<TIn>(input, cap, input.StreamName + ":capped");
#pragma warning restore CA2000
            context.RegisterForDisposal(replay);

            // Eagerly pre-buffer the entire stream so the cap is enforced even if no failure occurs.
            var count = 0;

            await foreach (var _ in replay.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                count++;

                if (count > cap)
                    break; // enforcement done by pipe; break early once exceeded triggers exception.
            }

            input = replay;
        }

        // The streamFactory is a function that can be called to regenerate the source stream.
        // This is necessary for RestartNode decision.
        Task<IDataStream<TOut>> StreamFactory()
        {
            return innerStrategy.ExecuteAsync(input, node, context, nodeId, cancellationToken);
        }

        var resilientStream = CreateResilientStream<TIn, TOut>(StreamFactory, context, nodeId, options, policy, cancellationToken);
        var pipe = new DataStream<TOut>(resilientStream);
        context.RegisterForDisposal(pipe);
        return pipe;
    }

    /// <summary>
    ///     Enumerates the node's stream, restarting it for as long as the resilience policy answers
    ///     <see cref="ResilienceDecision.RestartNode" />.
    /// </summary>
    private static async IAsyncEnumerable<TOut> CreateResilientStream<TIn, TOut>(Func<Task<IDataStream<TOut>>> streamFactory, PipelineContext context,
        string nodeId, PipelineResilienceOptions options, IResiliencePolicy policy, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(ResilientExecutionStrategy));

        // restarts == number of restarts made so far; the run in progress is restarts + 1.
        var restarts = 0;

        // consecutiveFailures == number of consecutive failures without a successful item production
        var consecutiveFailures = 0;

        // Get or create a resilience activity for recording exceptions
        var resilientActivity = context.Observability.Tracer.CurrentActivity;

        var circuitBreaker = CircuitBreakerResolver.Resolve(context, nodeId, options);

        if (circuitBreaker is not null)
            ResilientExecutionStrategyLogMessages.CircuitBreakerResolved(logger, nodeId, circuitBreaker.GetSnapshot().State);

        while (true)
        {
            // Cancellation must surface as an OperationCanceledException. Exiting the loop instead would complete the
            // iterator normally, and a cancelled run would report success with a silently truncated result set.
            cancellationToken.ThrowIfCancellationRequested();

            if (circuitBreaker is not null && !circuitBreaker.CanExecute())
            {
                RecordDiagnostics(context, nodeId, restarts, consecutiveFailures);
                throw CreateCircuitBreakerOpenException(nodeId, circuitBreaker, "Execution blocked before attempt due to open circuit breaker.");
            }

            var sourceStream = await streamFactory().ConfigureAwait(false);
#pragma warning disable CA2007
            // CA2007 false positive: the enumerator comes from a ConfigureAwait(false) sequence, so its
            // MoveNextAsync and DisposeAsync already return configured awaitables - the analyzer only
            // recognises ConfigureAwait applied directly to the await using expression.
            await using var enumerator = sourceStream.WithCancellation(cancellationToken).ConfigureAwait(false).GetAsyncEnumerator();
#pragma warning restore CA2007
            var restartRequested = false;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                TOut current;

                try
                {
                    if (!await enumerator.MoveNextAsync())
                        yield break; // completed successfully

                    current = enumerator.Current;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Cancellation of the pipeline's own token is not a node failure: it must not consume a restart
                    // attempt, trip the circuit breaker, or be rewritten into a RetryExhaustedException.
                    throw;
                }
                catch (Exception ex)
                {
                    resilientActivity?.RecordException(ex);
                    consecutiveFailures++;

                    if (circuitBreaker is not null)
                    {
                        var breakerResult = circuitBreaker.RecordFailure();

                        if (!breakerResult.Allowed)
                        {
                            RecordDiagnostics(context, nodeId, restarts, consecutiveFailures);
                            throw CreateCircuitBreakerOpenException(nodeId, circuitBreaker, breakerResult.Message);
                        }
                    }

                    var decision = await policy.DecideRestartAsync(new StreamFailure
                    {
                        NodeId = nodeId,
                        Exception = ex,
                        Attempt = restarts + 1,
                        MaxRestarts = options.NodeRestart.MaxRestarts,
                        Context = context,
                    }, cancellationToken).ConfigureAwait(false);

                    ResilientExecutionStrategyLogMessages.ErrorHandlerDecision(logger, decision.ToString(), nodeId, restarts, consecutiveFailures);

                    if (decision == ResilienceDecision.ContinueWithoutNode)
                        yield break;

                    if (decision != ResilienceDecision.RestartNode)
                    {
                        RecordDiagnostics(context, nodeId, restarts, consecutiveFailures);

                        if (restarts == 0)
                            throw;

                        var exhausted = new RetryExhaustedException(nodeId, restarts + 1, ex);
                        ResilientExecutionStrategyLogMessages.RetryExhausted(logger, nodeId, restarts + 1);
                        ResilienceRuntime.ReportRetryExhausted(context, nodeId, RetryKind.NodeRestart, restarts + 1, ex);
                        throw exhausted;
                    }

                    if (restarts >= ResilienceRuntime.MaxPolicyRepeats)
                        throw ResilienceRuntime.RepeatCeilingExceeded(policy, nodeId, decision, ex);

                    restarts++;

                    var delay = options.NodeRestart.Backoff.DelayFor(restarts);

                    if (delay > TimeSpan.Zero)
                    {
                        ResilientExecutionStrategyLogMessages.ApplyingRetryDelay(logger, delay.TotalMilliseconds, nodeId, restarts);
                        await Task.Delay(delay, options.Time, cancellationToken).ConfigureAwait(false);
                    }

                    ResilienceRuntime.ReportRetry(context, nodeId, RetryKind.NodeRestart, restarts, ex);

                    restartRequested = true;
                    break;
                }

                // Successful item production - reset consecutive failure counter for circuit breaker
                consecutiveFailures = 0;

                circuitBreaker?.RecordSuccess();

                yield return current;
            }

            if (!restartRequested)
                break;
        }
    }

    private static void RecordDiagnostics(PipelineContext context, string nodeId, int restarts, int consecutiveFailures)
    {
        var registry = context.NodeEnvironment.NodeExecutionScopeRegistry;
        registry.SetRuntimeAnnotation(PipelineContextKeys.DiagnosticsResilienceFailures(nodeId), restarts);
        registry.SetRuntimeAnnotation(PipelineContextKeys.DiagnosticsResilienceConsecutiveFailures(nodeId), consecutiveFailures);
        registry.SetRuntimeAnnotation(PipelineContextKeys.DiagnosticsResilienceThrowingOnFailure(nodeId), true);
    }

    private static NodeExecutionException CreateCircuitBreakerOpenException(string nodeId, IResilienceCircuitBreaker circuitBreaker, string? reason)
    {
        var detail = reason;

        if (string.IsNullOrWhiteSpace(detail))
            detail = "Circuit breaker is open and blocking execution.";

        var snapshot = circuitBreaker.GetSnapshot();
        var telemetrySuffix = snapshot.TotalOperations <= 0
            ? $"(state: {snapshot.State}, threshold: {snapshot.FailureThreshold})"
            :
            $"(state: {snapshot.State}, failures: {snapshot.FailureCount}, total: {snapshot.TotalOperations}, threshold: {snapshot.FailureThreshold})";

        var innerMessage = $"{detail} {telemetrySuffix}";
        var inner = new CircuitBreakerOpenException(innerMessage.Trim());

        return new NodeExecutionException(nodeId, "Circuit breaker is open and blocking execution", inner);
    }
}
