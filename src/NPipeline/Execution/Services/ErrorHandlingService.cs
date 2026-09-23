using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using NPipeline.ErrorHandling;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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

            // Taken, not read: leaving it set would attribute this node's root cause to every later failure too.
            if (context.ExecutionConfiguration.TakeLastRetryExhaustedException() is { } contextRetryEx)
                throw new NodeExecutionException(nodeDef.Id, contextRetryEx.Message, contextRetryEx);

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
    ///     Executes the node, and executes it again for as long as the resilience policy answers
    ///     <see cref="ResilienceDecision.Retry" />.
    /// </summary>
    private static async Task ExecuteWithRetriesInternalAsync(
        NodeDefinition nodeDefinition,
        INode node,
        PipelineGraph graph,
        PipelineContext context,
        Func<Task> executeAsync,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(nodeDefinition);
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(executeAsync);

        var options = context.ExecutionConfiguration.GetResilienceOptions(nodeDefinition.Id);

        // attempt is the 1-based number of the execution being made; retries so far is attempt - 1.
        for (var attempt = 1;; attempt++)
        {
            Exception failure;

            try
            {
                await executeAsync().ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                failure = ex;

                // Check whether a node exhausted its retries and left the root cause for us to report.
                if (context.ExecutionConfiguration.TakeLastRetryExhaustedException() is { } contextRetryEx)
                    throw new NodeExecutionException(nodeDefinition.Id, contextRetryEx.Message, contextRetryEx);
            }

            var policy = ResilienceRuntime.ResolvePolicy(context, nodeDefinition.Id);

            var decision = await policy.DecideNodeFailureAsync(new NodeFailure
            {
                Definition = nodeDefinition,
                Node = node,
                Exception = failure,
                Attempt = attempt,
                MaxRetries = options.NodeRetry.MaxRetries,
                IsTransient = options.NodeRetry.Classifier.IsTransient(failure, cancellationToken),
                Context = context,
            }, cancellationToken).ConfigureAwait(false);

            if (decision != ResilienceDecision.Retry)
            {
                if (attempt > 1 && failure is not OperationCanceledException)
                    ResilienceRuntime.ReportRetryExhausted(context, nodeDefinition.Id, RetryKind.NodeRetry, attempt, failure);

                ThrowFinalFailure(nodeDefinition.Id, failure, attempt);
            }

            if (attempt > ResilienceRuntime.MaxPolicyRepeats)
                throw new NodeExecutionException(nodeDefinition.Id, failure.Message, ResilienceRuntime.RepeatCeilingExceeded(policy, nodeDefinition.Id, decision, failure));

            ResilienceRuntime.ReportRetry(context, nodeDefinition.Id, RetryKind.NodeRetry, attempt, failure);

            var delay = options.NodeRetry.Backoff.DelayFor(attempt);

            if (delay > TimeSpan.Zero)
            {
                var logger = context.Observability.LoggerFactory.CreateLogger(nameof(ErrorHandlingService));
                ErrorHandlingServiceLogMessages.ApplyingRetryDelay(logger, delay.TotalMilliseconds, nodeDefinition.Id, attempt);

                // A cancellation during the delay propagates: the node is not executed again.
                await Task.Delay(delay, options.Time, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    ///     Throws the failure the node ends with once the policy stops retrying it.
    /// </summary>
    /// <param name="nodeId">The node id.</param>
    /// <param name="failure">The last failure.</param>
    /// <param name="attempts">How many times the node was executed.</param>
    [DoesNotReturn]
    private static void ThrowFinalFailure(string nodeId, Exception failure, int attempts)
    {
        if (failure is OperationCanceledException)
            ExceptionDispatchInfo.Throw(failure);

        // Only a failure that was retried is an exhausted one; a first failure surfaces as itself.
        if (attempts == 1 && failure is PipelineException)
            ExceptionDispatchInfo.Throw(failure);

        // A retry-exhausted failure from an inner layer already names its node and attempts.
        for (var current = failure; current is not null; current = current.InnerException)
        {
            if (current is RetryExhaustedException)
                throw new NodeExecutionException(nodeId, current.Message, current);
        }

        if (attempts == 1)
            throw new NodeExecutionException(nodeId, failure.Message, failure);

        var exhausted = new RetryExhaustedException(nodeId, attempts, failure);
        throw new NodeExecutionException(nodeId, exhausted.Message, exhausted);
    }

    /// <summary>
    ///     Checks if the current execution is in parallel mode.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <returns>True if execution is in parallel mode, otherwise false.</returns>
    private static bool IsParallelExecution(PipelineContext context)
    {
        return context.ExecutionConfiguration.IsParallelExecution;
    }
}
