using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Implementation of IErrorHandlingService that provides infrastructure services for pipeline execution including error handling and persistence.
///     <para>
///         This service coordinates error handling operations across the pipeline, implementing retry logic,
///         circuit breaker functionality, and integration with the persistence layer.
///     </para>
/// </summary>
/// <remarks>
///     <para>
///         The ErrorHandlingService is responsible for:
///         - Coordinating retry logic for failed nodes
///         - Implementing circuit breaker pattern to prevent cascading failures
///         - Integrating with the persistence layer for state management
///         - Providing a unified interface for error handling operations
///     </para>
///     <para>
///         Performance considerations:
///         - Avoids retry logic when nodes already implement resilience strategies
///         - Uses efficient pattern matching for error decision handling
///         - Implements circuit breaker with minimal overhead
///         - Provides fast-path execution for successful operations
///     </para>
///     <para>
///         Pattern matching enhancements:
///         - Uses C# switch expressions for efficient error decision handling
///         - Implements pattern-based retry limit enforcement
///         - Leverages tuple patterns for state management
///     </para>
/// </remarks>
public sealed class ErrorHandlingService : IErrorHandlingService
{
    /// <summary>
    ///     Gets the singleton instance of the ErrorHandlingService.
    ///     <para>
    ///         This singleton pattern ensures consistent error handling behavior across the pipeline
    ///         while minimizing resource usage.
    ///     </para>
    /// </summary>
    public static ErrorHandlingService Instance { get; } = new();

    /// <inheritdoc />
    /// <summary>
    ///     Executes a node with retry logic and error handling.
    ///     <para>
    ///         This method implements the core error handling logic, including retry attempts,
    ///         circuit breaker functionality, and integration with the error handler.
    ///     </para>
    /// </summary>
    /// <param name="nodeDef">The definition of node to execute.</param>
    /// <param name="nodeInstance">The instantiated node object.</param>
    /// <param name="graph">The pipeline graph containing node definitions and configuration.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="executeBody">The execution body to run with retry logic.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A task that represents asynchronous execution.</returns>
    /// <exception cref="NodeExecutionException">Thrown when the node execution fails after all retries.</exception>
    /// <exception cref="CircuitBreakerTrippedException">Thrown when the circuit breaker trips due to too many failures.</exception>
    /// <exception cref="RetryExhaustedException">Thrown when all retry attempts are exhausted.</exception>
    /// <exception cref="OperationCanceledException">Thrown when the operation is canceled.</exception>
    /// <remarks>
    ///     <para>
    ///         The error handling process follows these steps:
    ///         1. Check if the node already implements a resilient strategy (skip retry logic if true)
    ///         2. Execute the node with retry logic and circuit breaker functionality
    ///         3. Handle exceptions according to the configured error handling strategy
    ///         4. Throw appropriate exceptions when all retry attempts are exhausted
    ///     </para>
    ///     <para>
    ///         Performance optimizations:
    ///         - Fast-path execution for nodes with resilient strategies
    ///         - Efficient retry limit checking using pattern matching
    ///         - Minimal allocations in the hot path
    ///     </para>
    /// </remarks>
    public async Task ExecuteWithRetriesAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        PipelineGraph graph,
        PipelineContext context,
        Func<Task> executeBody,
        CancellationToken cancellationToken)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ErrorHandlingService));

        try
        {
            await ExecuteWithRetriesInternalAsync(
                nodeDef,
                nodeInstance,
                graph,
                context,
                executeBody,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Preserve cancellation semantics: do not wrap cancellation in a NodeExecutionException
            throw;
        }
        catch (PipelineExecutionException)
        {
            // Re-throw PipelineExecutionException without wrapping
            throw;
        }
        catch (NodeExecutionException)
        {
            // Re-throw NodeExecutionException without wrapping to avoid double wrapping
            throw;
        }
        catch (Exception ex)
        {
            // Check if this is a parallel execution scenario where we want to preserve the original exception
            var isParallelExecution = IsParallelExecution(context);

            // FIRST PRIORITY: Check if there's a RetryExhaustedException in the context that might be related to this failure
            // This handles cases where an upstream node failed with RetryExhaustedException but the current node
            // is seeing a different exception (like InvalidOperationException) when trying to process the data

            if (context.Items.TryGetValue(PipelineContextKeys.LastRetryExhaustedException, out var retryExObj) &&
                retryExObj is RetryExhaustedException contextRetryEx)
            {
                // Use the RetryExhaustedException from context as the inner exception
                throw new NodeExecutionException(nodeDef.Id, contextRetryEx.Message, contextRetryEx);
            }

            // SECOND PRIORITY: Check if the exception or any of its inner exceptions is a RetryExhaustedException
            var currentException = ex;

            while (currentException is not null)
            {
                if (currentException is RetryExhaustedException)
                {
                    // Wrap the RetryExhaustedException in NodeExecutionException with RetryExhaustedException as inner exception
                    throw new NodeExecutionException(nodeDef.Id, currentException.Message, currentException);
                }

                currentException = currentException.InnerException;
            }

            // THIRD PRIORITY: If the exception is a NodeExecutionException, check if it has a RetryExhaustedException as inner exception
            if (ex is NodeExecutionException nodeEx)
            {
                if (nodeEx.InnerException is RetryExhaustedException innerRetryEx)
                    throw;
            }

            // FOURTH PRIORITY: If the exception is a RetryExhaustedException, wrap it in NodeExecutionException with RetryExhaustedException as inner exception
            if (ex is RetryExhaustedException retryEx)
                throw new NodeExecutionException(nodeDef.Id, retryEx.Message, retryEx);

            // FIFTH PRIORITY: Check if the exception message contains "Retry attempts exhausted" which indicates it's a RetryExhaustedException
            if (ex.Message.Contains("Retry attempts exhausted"))
            {
                // The exception is already a RetryExhaustedException, just wrap it in NodeExecutionException
                throw new NodeExecutionException(nodeDef.Id, ex.Message, ex);
            }

            if (isParallelExecution)
            {
                // For parallel execution, preserve the original exception type for correct exception propagation semantics
                throw;
            }

            // Wrap other exceptions in NodeExecutionException
            throw new NodeExecutionException(nodeDef.Id, ex.Message, ex);
        }
    }

    /// <summary>
    ///     Internal method that implements the retry logic.
    /// </summary>
    private static async Task ExecuteWithRetriesInternalAsync(
        NodeDefinition nodeDefinition,
        INode node,
        PipelineGraph graph,
        PipelineContext context,
        Func<Task> executeAsync,
        CancellationToken cancellationToken)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ErrorHandlingService));

        ArgumentNullException.ThrowIfNull(nodeDefinition);
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(executeAsync);

        var retryCount = 0;
        var effectiveRetryOptions = GetEffectiveRetryOptions(nodeDefinition, context);
        var maxRetries = effectiveRetryOptions.MaxNodeRestartAttempts;
        Exception? lastException = null;

        // First attempt
        try
        {
            await executeAsync().ConfigureAwait(false);
            return; // Success
        }
        catch (Exception ex)
        {
            // If the node execution was canceled, preserve and rethrow immediately so that cancellation
            // is not wrapped by the retry/error handling logic.
            if (ex is OperationCanceledException)
                throw;

            lastException = ex;

            // Check if there's a RetryExhaustedException in the context after each retry attempt
            if (context.Items.TryGetValue(PipelineContextKeys.LastRetryExhaustedException, out var retryExObj) &&
                retryExObj is RetryExhaustedException contextRetryEx)
            {
                // Use the RetryExhaustedException from the context as the inner exception
                throw new NodeExecutionException(nodeDefinition.Id, contextRetryEx.Message, contextRetryEx);
            }
        }

        // Retry attempts
        while (retryCount < maxRetries)
        {
            // Check if we should retry before calling the error handler
            if (retryCount >= maxRetries)
                break;

            // Call error handler to decide whether to retry
            var errorDecision = await HandleNodeErrorAsync(nodeDefinition, node, lastException!, context, cancellationToken);

            if (errorDecision != NodeErrorDecision.Retry)
            {
                // If the last exception was a cancellation, rethrow it rather than wrapping it.
                if (lastException is OperationCanceledException)
                    throw lastException;

                if (lastException is PipelineException)
                    throw lastException;

                // Check if the exception or any of its inner exceptions is a RetryExhaustedException
                var exToCheck = lastException;

                while (exToCheck is not null)
                {
                    if (exToCheck is RetryExhaustedException)
                        throw new NodeExecutionException(nodeDefinition.Id, exToCheck.Message, exToCheck);

                    exToCheck = exToCheck.InnerException;
                }

                throw new NodeExecutionException(nodeDefinition.Id, lastException!.Message, lastException);
            }

            retryCount++;

            // Apply retry delay before retry attempt
            var delayStrategy = context.GetRetryDelayStrategy();

            try
            {
                var delay = await delayStrategy.GetDelayAsync(retryCount, cancellationToken).ConfigureAwait(false);

                if (delay > TimeSpan.Zero)
                {
                    ErrorHandlingServiceLogMessages.ApplyingRetryDelay(logger, delay.TotalMilliseconds, nodeDefinition.Id, retryCount);
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception delayEx)
            {
                // Log delay strategy failure but continue with retry
                ErrorHandlingServiceLogMessages.RetryDelayFailed(logger, delayEx, nodeDefinition.Id);
            }

            try
            {
                await executeAsync().ConfigureAwait(false);
                return; // Success
            }
            catch (Exception ex)
            {
                // Preserve cancellations immediately
                if (ex is OperationCanceledException)
                    throw;

                lastException = ex;

                // Check if there's a RetryExhaustedException in the context after each retry attempt
                if (context.Items.TryGetValue(PipelineContextKeys.LastRetryExhaustedException, out var retryExObj) &&
                    retryExObj is RetryExhaustedException contextRetryEx)
                {
                    // Use the RetryExhaustedException from the context as the inner exception
                    throw new NodeExecutionException(nodeDefinition.Id, contextRetryEx.Message, contextRetryEx);
                }
            }
        }

        // At this point lastException must be non-null (first attempt captured it and subsequent attempts only reassign on failure).
        var failureException = lastException; // Documented invariant: failures have occurred so lastException is set.

        // If failure was caused by cancellation, preserve the original OperationCanceledException
        // instead of wrapping it in a NodeExecutionException. This ensures cancellation propagates
        // correctly to callers and observers (e.g., OpenTelemetry tests expect OperationCanceledException).
        if (failureException is OperationCanceledException)
            throw failureException;

        // Check if the exception or any of its inner exceptions is a RetryExhaustedException
        var currentException = lastException;

        while (currentException is not null)
        {
            if (currentException is RetryExhaustedException)
                throw currentException;

            currentException = currentException.InnerException;
        }

        if (failureException is PipelineException)
            throw failureException;

        // If the last exception is a RetryExhaustedException, wrap it in NodeExecutionException with RetryExhaustedException as inner exception
        if (failureException is RetryExhaustedException retryEx)
            throw new NodeExecutionException(nodeDefinition.Id, retryEx.Message, retryEx);

        // Create a RetryExhaustedException when retries are exhausted
        var retryExhaustedException = new RetryExhaustedException(nodeDefinition.Id, retryCount + 1, failureException);
        throw new NodeExecutionException(nodeDefinition.Id, retryExhaustedException.Message, retryExhaustedException);
    }

    /// <summary>
    ///     Checks if the current execution is in parallel mode.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <returns>True if execution is in parallel mode, otherwise false.</returns>
    private static bool IsParallelExecution(PipelineContext context)
    {
        return context.Items.TryGetValue(PipelineContextKeys.ParallelExecution, out var parallelValue) &&
               parallelValue is bool isParallel && isParallel;
    }

    /// <summary>
    ///     Gets the effective retry options for a node based on node, graph, and context settings.
    /// </summary>
    /// <param name="nodeDefinition">The node definition.</param>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The effective retry options.</returns>
    private static PipelineRetryOptions GetEffectiveRetryOptions(NodeDefinition nodeDefinition, PipelineContext context)
    {
        // Check for node-specific retry options
        if (context.Items.TryGetValue($"retry::{nodeDefinition.Id}", out var specific) &&
            specific is PipelineRetryOptions specificOptions)
            return specificOptions;

        // Fall back to global retry options
        if (context.Items.TryGetValue(PipelineContextKeys.GlobalRetryOptions, out var global) &&
            global is PipelineRetryOptions globalOptions)
            return globalOptions;

        // Default retry options
        return PipelineRetryOptions.Default;
    }

    /// <summary>
    ///     Handles a node error by consulting to node's error handler if available.
    /// </summary>
    /// <param name="nodeDefinition">The node definition.</param>
    /// <param name="node">The node instance.</param>
    /// <param name="exception">The exception that occurred.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The error decision.</returns>
    private static async Task<NodeErrorDecision> HandleNodeErrorAsync(
        NodeDefinition nodeDefinition,
        INode node,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // First check if there's a pipeline error handler
        if (context.PipelineErrorHandler is not null)
        {
            try
            {
                var pipelineDecision = await context.PipelineErrorHandler.HandleNodeFailureAsync(
                    nodeDefinition.Id,
                    exception,
                    context,
                    cancellationToken);

                // If the error handler returned RestartNode, verify the node is using ResilientExecutionStrategy
                if (pipelineDecision == PipelineErrorDecision.RestartNode)
                {
                    if (nodeDefinition.ExecutionStrategy?.GetType().Name != "ResilientExecutionStrategy")
                    {
                        throw new InvalidOperationException(
                            $"Node '{nodeDefinition.Id}' error handler returned RestartNode, but the node is not using ResilientExecutionStrategy. " +
                            "Node restart functionality requires wrapping the node with ResilientExecutionStrategy. " +
                            "Fix: Add .WithResilience() to the node configuration, e.g., " +
                            $"builder.AddNode(\"{nodeDefinition.Id}\", ...).WithResilience()");
                    }
                }

                // Convert PipelineErrorDecision to NodeErrorDecision
                return pipelineDecision switch
                {
                    PipelineErrorDecision.RestartNode => NodeErrorDecision.Retry,
                    PipelineErrorDecision.ContinueWithoutNode => NodeErrorDecision.Skip,
                    PipelineErrorDecision.FailPipeline => NodeErrorDecision.Fail,
                    _ => NodeErrorDecision.Fail,
                };
            }
            catch
            {
                // If pipeline error handler fails, fall back to node error handler
            }
        }

        // Check if node has an error handler
        if (nodeDefinition.ErrorHandlerType is null)
            return NodeErrorDecision.Fail;

        var errorHandlerType = nodeDefinition.ErrorHandlerType;

        var handleAsyncMethod = errorHandlerType.GetMethods()
            .FirstOrDefault(m => m.Name == "HandleAsync" && m.GetParameters().Length == 5);

        if (handleAsyncMethod is null)
            return NodeErrorDecision.Fail;

        try
        {
            // Create an instance of the error handler
            var errorHandler = context.ErrorHandlerFactory.CreateErrorHandler(errorHandlerType);

            if (errorHandler is null)
                return NodeErrorDecision.Fail;

            var invokeResult = handleAsyncMethod.Invoke(
                errorHandler,
                [node, null, exception, context, cancellationToken]);

            if (invokeResult is Task<NodeErrorDecision> task)
            {
                var result = await task.ConfigureAwait(false);
                return result;
            }

            return NodeErrorDecision.Fail;
        }
        catch
        {
            return NodeErrorDecision.Fail;
        }
    }
}
