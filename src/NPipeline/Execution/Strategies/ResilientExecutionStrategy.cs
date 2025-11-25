using System.Runtime.CompilerServices;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     An execution strategy that wraps another strategy to provide resilience against stream failures.
///     <para>
///         If the underlying stream fails, this strategy will consult the <see cref="IPipelineErrorHandler" />
///         to decide whether to restart the node, continue without it, or fail the pipeline.
///     </para>
///     <para>
///         Performance considerations:
///         - Materializes streaming inputs only when necessary for resilience
///         - Uses CappedReplayableDataPipe for efficient restart support
///         - Implements circuit breaker pattern to prevent cascading failures
///         - Provides configurable retry limits with exponential backoff
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
///         - Configurable retry limits and materialization caps
///         - Integration with pipeline-wide error handling
///     </para>
///     <para>
///         Circuit breaker semantics:
///         - The circuit breaker tracks consecutive failures (not total failures)
///         - A successful item production resets the consecutive failure counter
///         - The breaker trips only when consecutive failures exceed the threshold
///         - This prevents premature breaker trips due to intermittent failures
///         - Total failures are still tracked separately for retry limits
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
///     var node = new TransformNode&lt;Input, Output&gt;(transformFunction)
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
    public async Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(IDataPipe<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Create a resilience activity to track and tag resilience-related telemetry
        using var resilientActivity = context.Tracer.StartActivity("Node.Resilience");
        resilientActivity.SetTag("resilience.enabled", true);

        if (context.PipelineErrorHandler is null)
        {
            // If there is no graph-level error handler, there is no need for this resilience layer.
            // We can just delegate directly to the inner strategy.
            return await innerStrategy.ExecuteAsync(input, node, context, cancellationToken).ConfigureAwait(false);
        }

        EnsureCircuitBreakerManagerIsAvailable(context);

        // If the input is a streaming pipe, we must materialize it to support restarts.
        // This is a performance trade-off for resiliency.
        if (input is IStreamingDataPipe)
        {
            // Determine effective cap (prefer node-specific override captured in context.Items earlier by the runner)
            // Pattern matching for retry options resolution with precedence:
            // 1. Node-specific override (highest priority)
            // 2. Graph-level retry options
            // 3. Context-level retry options (lowest priority)
            int? cap = null;

            if (context.Items.TryGetValue($"retry::{context.CurrentNodeId}", out var ro) && ro is PipelineRetryOptions pro &&
                pro.MaxMaterializedItems is not null)
                cap = pro.MaxMaterializedItems;
            else if (context.Items.TryGetValue(PipelineContextKeys.GlobalRetryOptions, out var gro) && gro is PipelineRetryOptions gpr &&
                     gpr.MaxMaterializedItems is not null)
                cap = gpr.MaxMaterializedItems;
            else if (context.RetryOptions.MaxMaterializedItems is not null)
                cap = context.RetryOptions.MaxMaterializedItems;

            // Always wrap in replayable pipe so restarts re-enumerate buffered items. Cap enforced when specified.
#pragma warning disable CA2000 // Ownership transferred to PipelineContext via RegisterForDisposal
            var replay = new CappedReplayableDataPipe<TIn>(input, cap, cap is not null
                ? input.StreamName + ":capped"
                : input.StreamName + ":replay");
#pragma warning restore CA2000
            context.RegisterForDisposal(replay);

            // Eagerly pre-buffer entire stream when a cap is set to enforce limit even if no failure occurs.
            // This ensures memory limits are respected even in successful execution scenarios.
            if (cap is not null)
            {
                var count = 0;

                await foreach (var _ in replay.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    count++;

                    if (count > cap)
                        break; // enforcement done by pipe; break early once exceeded triggers exception.
                }

                // Reset enumerator by creating a new replay pipe over the same underlying source buffer.
                // Since CappedReplayableDataPipe stores buffered items internally, we can reuse it directly (subsequent enumeration will replay buffer).
            }

            input = replay;
        }

        // The streamFactory is a function that can be called to regenerate the source stream.
        // This is necessary for the RestartNode decision.
        Task<IDataPipe<TOut>> StreamFactory()
        {
            return innerStrategy.ExecuteAsync(input, node, context, cancellationToken);
        }

        // Capture retry options & nodeId at creation time so later enumeration (during sink execution) still uses correct values.
        var creationNodeId = context.CurrentNodeId;

        // Determine effective retry options precedence using pattern matching:
        // 1. Node-specific override stored in context.Items (retry::<nodeId>)
        // 2. Graph-level configured retry options surfaced in context.Items[PipelineContextKeys.GlobalRetryOptions]
        // 3. Context-level RetryOptions (may be default if not explicitly set on PipelineContext construction)
        var effectiveAtCreation = context.RetryOptions;

        if (context.Items.TryGetValue($"retry::{creationNodeId}", out var specific) && specific is PipelineRetryOptions prc)
            effectiveAtCreation = prc;
        else if (context.Items.TryGetValue(PipelineContextKeys.GlobalRetryOptions, out var globalRetry) && globalRetry is PipelineRetryOptions grc)
            effectiveAtCreation = grc;

        var resilientStream = CreateResilientStream<TIn, TOut>(StreamFactory, context, creationNodeId, effectiveAtCreation, cancellationToken);
        var pipe = new StreamingDataPipe<TOut>(resilientStream);
        context.RegisterForDisposal(pipe);
        return pipe;
    }

    /// <summary>
    ///     Creates a resilient async enumerable stream that handles failures according to the configured error handling strategy.
    ///     <para>
    ///         This method implements the core resilience logic using pattern matching for efficient decision making
    ///         and circuit breaker functionality to prevent cascading failures.
    ///     </para>
    /// </summary>
    /// <typeparam name="TIn">The input type of the node.</typeparam>
    /// <typeparam name="TOut">The output type of the node.</typeparam>
    /// <param name="streamFactory">Factory function to create new streams on restart.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="creationNodeId">The node ID captured at creation time.</param>
    /// <param name="capturedRetryOptions">The retry options captured at creation time.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A resilient async enumerable stream.</returns>
    /// <remarks>
    ///     <para>
    ///         The resilience logic uses pattern matching for:
    ///         - Error decision handling (RestartNode, ContinueWithoutNode, FailPipeline)
    ///         - Circuit breaker state management
    ///         - Retry limit enforcement
    ///     </para>
    ///     <para>
    ///         Performance optimizations:
    ///         - Minimal allocations in the hot path
    ///         - Efficient state tracking with value types
    ///         - Early exit conditions to avoid unnecessary processing
    ///     </para>
    /// </remarks>
    private static async IAsyncEnumerable<TOut> CreateResilientStream<TIn, TOut>(Func<Task<IDataPipe<TOut>>> streamFactory, PipelineContext context,
        string creationNodeId, PipelineRetryOptions capturedRetryOptions, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ResilientExecutionStrategy));

        // Use captured nodeId & retry options; don't rely on context.CurrentNodeId which may change during sink enumeration.
        var nodeId = creationNodeId;
        var effectiveRetries = capturedRetryOptions;

        // failures == number of restart-triggering failures observed so far (each leading to a restart decision)
        var failures = 0;

        // consecutiveFailures == number of consecutive failures without a successful item production
        var consecutiveFailures = 0;
        Exception? lastFailure = null;
        ICircuitBreaker? circuitBreaker = null;

        // Get or create a resilience activity for recording exceptions
        var resilientActivity = context.Tracer.CurrentActivity;

        // Resolve circuit breaker instance for this node if enabled
        if (context.Items.TryGetValue(PipelineContextKeys.CircuitBreakerOptions, out var cbo) && cbo is PipelineCircuitBreakerOptions cbr && cbr.Enabled)
        {
            if (context.Items.TryGetValue(PipelineContextKeys.CircuitBreakerManager, out var managerObj) && managerObj is ICircuitBreakerManager manager)
            {
                circuitBreaker = manager.GetCircuitBreaker(nodeId, cbr);
                logger.Log(LogLevel.Debug, "Circuit breaker resolved for node {NodeId}. State: {State}", nodeId, circuitBreaker.State);
            }
            else
                logger.Log(LogLevel.Warning,
                    "Circuit breaker options enabled but manager unavailable for node {NodeId}. Resilience will continue without breaker integration.", nodeId);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            if (circuitBreaker is not null && !circuitBreaker.CanExecute())
            {
                context.Items[PipelineContextKeys.DiagnosticsResilienceFailures(nodeId)] = failures;
                context.Items[PipelineContextKeys.DiagnosticsResilienceConsecutiveFailures(nodeId)] = consecutiveFailures;
                context.Items[PipelineContextKeys.DiagnosticsResilienceThrowingOnFailure(nodeId)] = true;
                throw CreateCircuitBreakerOpenException(nodeId, circuitBreaker, "Execution blocked before attempt due to open circuit breaker.");
            }

            // Gate semantics using pattern matching:
            // MaxNodeRestartAttempts defines the maximum number of restart attempts AFTER the initial attempt.
            // failures counts how many restart decisions have already occurred.
            // If failures >= MaxNodeRestartAttempts we should surface the last failure (or a descriptive exception) and stop.
            logger.Log(LogLevel.Debug, "Checking retry limit for node {NodeId}. Failures: {Failures}, MaxAttempts: {MaxAttempts}", nodeId, failures,
                effectiveRetries.MaxNodeRestartAttempts);

            if (failures >= effectiveRetries.MaxNodeRestartAttempts)
            {
                logger.Log(LogLevel.Warning, "Retry limit exceeded at start of loop for node {NodeId}. Throwing RetryExhaustedException.", nodeId);

                throw new RetryExhaustedException(nodeId, effectiveRetries.MaxNodeRestartAttempts,
                    lastFailure ?? new InvalidOperationException($"Node '{nodeId}' exceeded maximum restart attempts without a specific failure."));
            }

            var sourceStream = await streamFactory().ConfigureAwait(false);
            await using var enumerator = sourceStream.GetAsyncEnumerator(cancellationToken);
            var restartRequested = false;

            while (!cancellationToken.IsCancellationRequested)
            {
                TOut current;

                try
                {
                    using (context.ScopedNode(nodeId))
                    {
                        if (!await enumerator.MoveNextAsync().ConfigureAwait(false))
                            yield break; // completed successfully

                        current = enumerator.Current;
                    }
                }
                catch (Exception ex)
                {
                    lastFailure = ex;

                    // Record exception on resilience activity for observability
                    resilientActivity?.RecordException(ex);

                    // Increment consecutive failure counter for circuit breaker
                    consecutiveFailures++;

                    if (circuitBreaker is not null)
                    {
                        var breakerResult = circuitBreaker.RecordFailure();

                        if (!breakerResult.Allowed)
                        {
                            context.Items[PipelineContextKeys.DiagnosticsResilienceFailures(nodeId)] = failures;
                            context.Items[PipelineContextKeys.DiagnosticsResilienceConsecutiveFailures(nodeId)] = consecutiveFailures;
                            context.Items[PipelineContextKeys.DiagnosticsResilienceThrowingOnFailure(nodeId)] = true;
                            throw CreateCircuitBreakerOpenException(nodeId, circuitBreaker, breakerResult.Message);
                        }
                    }

                    // Pattern matching for failure limit check before attempting retry
                    if (failures >= effectiveRetries.MaxNodeRestartAttempts)
                    {
                        // Tag exception so upstream error handling does not attempt to restart unrelated nodes (e.g., sink) masking restart limit.
                        try
                        {
                            ex.Data["resilience.final"] = true;
                        }
                        catch
                        {
                            /* ignore */
                        }

                        context.Items[PipelineContextKeys.DiagnosticsResilienceFailures(nodeId)] = failures;
                        context.Items[PipelineContextKeys.DiagnosticsResilienceConsecutiveFailures(nodeId)] = consecutiveFailures;
                        context.Items[PipelineContextKeys.DiagnosticsResilienceThrowingOnFailure(nodeId)] = true;

                        // Log before throwing to capture the state
                        logger.Log(LogLevel.Warning,
                            "Failure limit reached for node {NodeId}. Failures: {Failures}, Consecutive failures: {ConsecutiveFailures}, MaxAttempts: {MaxAttempts}. Throwing RetryExhaustedException.",
                            nodeId, failures, consecutiveFailures, effectiveRetries.MaxNodeRestartAttempts);

                        var retryEx = new RetryExhaustedException(nodeId, effectiveRetries.MaxNodeRestartAttempts, ex);
                        logger.Log(LogLevel.Debug, "Created RetryExhaustedException with message: {ExceptionMessage}", retryEx.Message);
                        throw retryEx;
                    }

                    var decision = await context.PipelineErrorHandler!.HandleNodeFailureAsync(nodeId, ex, context, cancellationToken).ConfigureAwait(false);

                    // Log the decision from the error handler
                    logger.Log(LogLevel.Debug,
                        "ErrorHandler returned decision {Decision} for node {NodeId}. Current failures: {Failures}, Consecutive failures: {ConsecutiveFailures}.",
                        decision, nodeId, failures, consecutiveFailures);

                    // Pattern matching for error decision handling - this is a key enhancement using C# switch expressions
                    var shouldContinue = decision switch
                    {
                        PipelineErrorDecision.RestartNode when failures < effectiveRetries.MaxNodeRestartAttempts => true,
                        PipelineErrorDecision.ContinueWithoutNode => false,
                        PipelineErrorDecision.FailPipeline => false,
                        _ => false,
                    };

                    logger.Log(LogLevel.Debug, "shouldContinue for node {NodeId} is {ShouldContinue}.", nodeId, shouldContinue);

                    if (shouldContinue)
                    {
                        failures++;

                        // Apply retry delay before restarting the node
                        var delayStrategy = context.GetRetryDelayStrategy();
                        try
                        {
                            var delay = await delayStrategy.GetDelayAsync(failures, cancellationToken).ConfigureAwait(false);
                            if (delay > TimeSpan.Zero)
                            {
                                logger.Log(LogLevel.Debug, "Applying retry delay of {Delay}ms for node {NodeId} after {FailureCount} failures", 
                                    delay.TotalMilliseconds, nodeId, failures);
                                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                            }
                        }
                        catch (Exception delayEx)
                        {
                            // Log delay strategy failure but continue with retry
                            logger.Log(LogLevel.Warning, delayEx, "Failed to apply retry delay for node {NodeId}. Continuing with retry without delay.", nodeId);
                        }

                        context.ExecutionObserver.OnRetry(new NodeRetryEvent(nodeId, RetryKind.NodeRestart, failures, ex));
                        restartRequested = true;
                        break;
                    }

                    // Either ContinueWithoutNode or FailPipeline
                    if (decision == PipelineErrorDecision.ContinueWithoutNode)
                        yield break;

                    // FailPipeline or default
                    throw;
                }

                // Successful item production - reset consecutive failure counter for circuit breaker
                consecutiveFailures = 0;

                if (circuitBreaker is not null)
                    _ = circuitBreaker.RecordSuccess();

                yield return current;
            }

            if (restartRequested)
            {
                // Maintain consecutive failure count (breaker counts consecutive restarts). If a restart succeeds and we yield an item, it resets above.
                continue; // outer loop will re-check gate and restart
            }

            // If we got here without restart or completion already yielded all items, exit outer loop.
            break;
        }
    }

    private static void EnsureCircuitBreakerManagerIsAvailable(PipelineContext context)
    {
        if (!context.Items.TryGetValue(PipelineContextKeys.CircuitBreakerOptions, out var cbo) || cbo is not PipelineCircuitBreakerOptions options ||
            !options.Enabled)
            return;

        if (context.Items.TryGetValue(PipelineContextKeys.CircuitBreakerManager, out var existing) && existing is ICircuitBreakerManager)
            return;

        var logger = context.LoggerFactory.CreateLogger(nameof(CircuitBreakerManager));
        CircuitBreakerMemoryManagementOptions? memoryOptions = null;

        if (context.Items.TryGetValue(PipelineContextKeys.CircuitBreakerMemoryOptions, out var memoryOptionObject) &&
            memoryOptionObject is CircuitBreakerMemoryManagementOptions configuredMemoryOptions)
            memoryOptions = configuredMemoryOptions;

        var manager = context.CreateAndRegister(new CircuitBreakerManager(logger, memoryOptions));
        context.Items[PipelineContextKeys.CircuitBreakerManager] = manager;
    }

    private static NodeExecutionException CreateCircuitBreakerOpenException(string nodeId, ICircuitBreaker circuitBreaker, string? reason)
    {
        var detail = reason;

        if (string.IsNullOrWhiteSpace(detail))
            detail = "Circuit breaker is open and blocking execution.";

        WindowStatistics? statistics = null;

        if (circuitBreaker.Options.TrackOperationsInWindow)
            statistics = circuitBreaker.GetStatistics();

        var telemetrySuffix = statistics is null
            ? $"(state: {circuitBreaker.State}, threshold: {circuitBreaker.Options.FailureThreshold})"
            : $"(state: {circuitBreaker.State}, failures: {statistics.FailureCount}, total: {statistics.TotalOperations}, threshold: {circuitBreaker.Options.FailureThreshold})";

        var innerMessage = $"{detail} {telemetrySuffix}";
        var inner = new CircuitBreakerOpenException(innerMessage.Trim());

        return new NodeExecutionException(nodeId, "Circuit breaker is open and blocking execution", inner);
    }
}
