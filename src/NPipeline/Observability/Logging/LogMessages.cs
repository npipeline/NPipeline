using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace NPipeline.Observability.Logging;

/// <summary>
///     Source-generated logging methods for circuit breaker operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class CircuitBreakerLogMessages
{
    [LoggerMessage(1, LogLevel.Warning, "Circuit breaker transitioned from {PreviousState} to Open: {Reason}")]
    public static partial void TransitionedToOpen(ILogger logger, string previousState, string reason);

    [LoggerMessage(2, LogLevel.Information, "Circuit breaker transitioned from {PreviousState} to Half-Open: {Reason}. Success threshold: {Threshold}")]
    public static partial void TransitionedToHalfOpen(ILogger logger, string previousState, string reason, int threshold);

    [LoggerMessage(3, LogLevel.Information, "Circuit breaker transitioned from {PreviousState} to Closed: {Reason}. Metrics reset.")]
    public static partial void TransitionedToClosed(ILogger logger, string previousState, string reason);
}

/// <summary>
///     Source-generated logging methods for circuit breaker manager operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class CircuitBreakerManagerLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Circuit breaker cleanup timer initialized with interval: {Interval}")]
    public static partial void CleanupTimerInitialized(ILogger logger, TimeSpan interval);

    [LoggerMessage(2, LogLevel.Debug, "Removed circuit breaker for node {NodeId}")]
    public static partial void CircuitBreakerRemoved(ILogger logger, string nodeId);

    [LoggerMessage(3, LogLevel.Debug, "Automatic cleanup is disabled, manual cleanup triggered")]
    public static partial void ManualCleanupTriggered(ILogger logger);

    [LoggerMessage(4, LogLevel.Debug, "Manual cleanup triggered")]
    public static partial void ManualCleanupTriggeredWithAutoEnabled(ILogger logger);

    [LoggerMessage(5, LogLevel.Warning,
        "Maximum circuit breaker limit ({MaxCount}) reached while creating circuit breaker for node {NodeId}. Issuing aggressive cleanup.")]
    public static partial void MaxCircuitBreakerLimitReached(ILogger logger, int maxCount, string nodeId);

    [LoggerMessage(6, LogLevel.Error, "Unable to create circuit breaker for node '{NodeId}' because the manager exhausted its maximum capacity of {MaxCount}.")]
    public static partial void CircuitBreakerCreationFailed(ILogger logger, string nodeId, int maxCount);

    [LoggerMessage(7, LogLevel.Debug, "Creating circuit breaker for node {NodeId} with options: {@Options}")]
    public static partial void CreatingCircuitBreaker(ILogger logger, string nodeId, object options);

    [LoggerMessage(8, LogLevel.Error, "Error during automatic circuit breaker cleanup")]
    public static partial void CleanupError(ILogger logger, Exception exception);

    [LoggerMessage(9, LogLevel.Warning, "Cleanup operation already in progress, skipping")]
    public static partial void CleanupSkippedInProgress(ILogger logger);

    [LoggerMessage(10, LogLevel.Debug, "Removed inactive circuit breaker for node {NodeId}")]
    public static partial void InactiveCircuitBreakerRemoved(ILogger logger, string nodeId);

    [LoggerMessage(11, LogLevel.Debug, "Removed stale tracking for circuit breaker node {NodeId}")]
    public static partial void StaleTrackingRemoved(ILogger logger, string nodeId);

    [LoggerMessage(12, LogLevel.Warning, "Aggressive cleanup removed least recently used circuit breaker for node {NodeId} last accessed at {LastAccess}")]
    public static partial void AggressiveCleanupRemoved(ILogger logger, string nodeId, DateTimeOffset lastAccess);

    [LoggerMessage(13, LogLevel.Warning,
        "Aggressive cleanup removed stale tracking for least recently used circuit breaker node {NodeId} last accessed at {LastAccess}")]
    public static partial void AggressiveCleanupStaleTrackingRemoved(ILogger logger, string nodeId, DateTimeOffset lastAccess);

    [LoggerMessage(14, LogLevel.Warning, "Aggressive cleanup requested but no tracked circuit breakers were available for eviction.")]
    public static partial void AggressiveCleanupNoVictims(ILogger logger);

    [LoggerMessage(15, LogLevel.Information, "Cleanup completed: removed {Count} circuit breaker(s)")]
    public static partial void CleanupCompleted(ILogger logger, int count);
}

/// <summary>
///     Source-generated logging methods for observability surface operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class ObservabilitySurfaceLogMessages
{
    [LoggerMessage(1, LogLevel.Information, "Starting pipeline run for {PipelineName}")]
    public static partial void PipelineStarting(ILogger logger, string pipelineName);

    [LoggerMessage(2, LogLevel.Information, "Finished pipeline run for {PipelineName}")]
    public static partial void PipelineFinished(ILogger logger, string pipelineName);

    [LoggerMessage(3, LogLevel.Error, "Failed to emit observability metrics for pipeline {PipelineName}")]
    public static partial void MetricsEmissionFailed(ILogger logger, Exception exception, string pipelineName);

    [LoggerMessage(4, LogLevel.Error, "Pipeline run for {PipelineName} failed")]
    public static partial void PipelineFailed(ILogger logger, Exception exception, string pipelineName);

    [LoggerMessage(5, LogLevel.Error, "Failed to emit observability metrics after pipeline failure for {PipelineName}")]
    public static partial void MetricsEmissionFailedAfterPipelineFailure(ILogger logger, Exception exception, string pipelineName);

    [LoggerMessage(6, LogLevel.Information, "Executing node {NodeId} of type {NodeType}")]
    public static partial void NodeExecuting(ILogger logger, string nodeId, string nodeType);

    [LoggerMessage(7, LogLevel.Debug, "Storing AutoObservabilityScope in context with key: {ContextKey}")]
    public static partial void AutoObservabilityScopeStored(ILogger logger, string contextKey);

    [LoggerMessage(8, LogLevel.Information, "Finished executing node {NodeId}")]
    public static partial void NodeFinished(ILogger logger, string nodeId);

    [LoggerMessage(9, LogLevel.Error, "Node {NodeId} failed")]
    public static partial void NodeFailed(ILogger logger, Exception exception, string nodeId);
}

/// <summary>
///     Source-generated logging methods for resilient execution strategy operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class ResilientExecutionStrategyLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Circuit breaker resolved for node {NodeId}. State: {State}")]
    public static partial void CircuitBreakerResolved(ILogger logger, string nodeId, string state);

    [LoggerMessage(2, LogLevel.Warning,
        "Circuit breaker options enabled but manager unavailable for node {NodeId}. Resilience will continue without breaker integration.")]
    public static partial void CircuitBreakerManagerUnavailable(ILogger logger, string nodeId);

    [LoggerMessage(3, LogLevel.Debug, "Checking retry limit for node {NodeId}. Failures: {Failures}, MaxAttempts: {MaxAttempts}")]
    public static partial void CheckingRetryLimit(ILogger logger, string nodeId, int failures, int maxAttempts);

    [LoggerMessage(4, LogLevel.Warning, "Retry limit exceeded at start of loop for node {NodeId}. Throwing RetryExhaustedException.")]
    public static partial void RetryLimitExceeded(ILogger logger, string nodeId);

    [LoggerMessage(5, LogLevel.Warning,
        "Failure limit reached for node {NodeId}. Failures: {Failures}, Consecutive failures: {ConsecutiveFailures}, MaxAttempts: {MaxAttempts}. Throwing RetryExhaustedException.")]
    public static partial void FailureLimitReached(ILogger logger, string nodeId, int failures, int consecutiveFailures, int maxAttempts);

    [LoggerMessage(6, LogLevel.Debug, "Created RetryExhaustedException with message: {ExceptionMessage}")]
    public static partial void RetryExhaustedExceptionCreated(ILogger logger, string exceptionMessage);

    [LoggerMessage(7, LogLevel.Debug,
        "ErrorHandler returned decision {Decision} for node {NodeId}. Current failures: {Failures}, Consecutive failures: {ConsecutiveFailures}.")]
    public static partial void ErrorHandlerDecision(ILogger logger, string decision, string nodeId, int failures, int consecutiveFailures);

    [LoggerMessage(8, LogLevel.Debug, "shouldContinue for node {NodeId} is {ShouldContinue}.")]
    public static partial void ShouldContinueDecision(ILogger logger, string nodeId, bool shouldContinue);

    [LoggerMessage(9, LogLevel.Debug, "Applying retry delay of {Delay}ms for node {NodeId} after {FailureCount} failures")]
    public static partial void ApplyingRetryDelay(ILogger logger, double delay, string nodeId, int failureCount);

    [LoggerMessage(10, LogLevel.Warning, "Failed to apply retry delay for node {NodeId}. Continuing with retry without delay.")]
    public static partial void RetryDelayFailed(ILogger logger, Exception exception, string nodeId);
}

/// <summary>
///     Source-generated logging methods for parallel execution strategy operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class ParallelExecutionStrategyLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Node {NodeId}, Final MaxRetries: {MaxRetries}")]
    public static partial void FinalMaxRetries(ILogger logger, string nodeId, int maxRetries);

    [LoggerMessage(2, LogLevel.Warning, "Node {NodeId}, Failed to enqueue item {Item} after {MaxAttempts} drop attempts")]
    public static partial void EnqueueFailed(ILogger logger, string nodeId, string? item, int maxAttempts);

    [LoggerMessage(3, LogLevel.Debug, "Node {NodeId}, Found per-node retry options: MaxRetries={MaxRetries}")]
    public static partial void PerNodeRetryOptionsFound(ILogger logger, string nodeId, int maxRetries);

    [LoggerMessage(4, LogLevel.Debug, "Node {NodeId}, Using global retry options: MaxItemRetries={MaxRetries}")]
    public static partial void GlobalRetryOptionsUsed(ILogger logger, string nodeId, int maxRetries);

    [LoggerMessage(5, LogLevel.Debug, "Node {NodeId}, Using context retry options: MaxItemRetries={MaxRetries}")]
    public static partial void ContextRetryOptionsUsed(ILogger logger, string nodeId, int maxRetries);

    [LoggerMessage(6, LogLevel.Debug, "Node {NodeId} failed on attempt {Attempt}.")]
    public static partial void NodeFailure(ILogger logger, Exception exception, string nodeId, int attempt);
}

/// <summary>
///     Source-generated logging methods for pipeline context operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class PipelineContextLogMessages
{
    [LoggerMessage(1, LogLevel.Warning, "Late-registration disposal failed: {Message}")]
    public static partial void LateRegistrationDisposalFailed(ILogger logger, string message);
}

/// <summary>
///     Source-generated logging methods for SQS sink node operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class SqsSinkNodeLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Processing IAcknowledgableMessage, MessageType={MessageType}")]
    public static partial void ProcessingAcknowledgableMessage(ILogger logger, string? messageType);

    [LoggerMessage(2, LogLevel.Debug, "Processing regular message, MessageType={MessageType}")]
    public static partial void ProcessingRegularMessage(ILogger logger, string? messageType);

    [LoggerMessage(3, LogLevel.Debug, "Sending message, ItemType={ItemType}")]
    public static partial void SendingMessage(ILogger logger, string itemType);

    [LoggerMessage(4, LogLevel.Debug, "Message sent successfully")]
    public static partial void MessageSent(ILogger logger);

    [LoggerMessage(5, LogLevel.Warning, "Failed to send message to SQS. Continuing due to ContinueOnError setting.")]
    public static partial void SendMessageFailed(ILogger logger, Exception exception);

    [LoggerMessage(6, LogLevel.Warning, "Failed to serialize message for batch. Skipping.")]
    public static partial void BatchSerializationFailed(ILogger logger, Exception exception);

    [LoggerMessage(7, LogLevel.Warning, "Failed to send message {Id} to SQS: {Message}")]
    public static partial void BatchMessageFailed(ILogger logger, string id, string message);

    [LoggerMessage(8, LogLevel.Warning, "Failed to send message batch to SQS. Continuing due to ContinueOnError setting.")]
    public static partial void SendMessageBatchFailed(ILogger logger, Exception exception);

    [LoggerMessage(9, LogLevel.Warning, "One or more delayed acknowledgment tasks failed during disposal")]
    public static partial void DelayedAcknowledgmentFailed(ILogger logger, Exception exception);

    [LoggerMessage(10, LogLevel.Error, "Error flushing acknowledgment batch on dispose")]
    public static partial void FlushBatchOnDisposeFailed(ILogger logger, Exception exception);

    [LoggerMessage(11, LogLevel.Error, "Error flushing acknowledgment batch")]
    public static partial void FlushBatchFailed(ILogger logger, Exception exception);

    [LoggerMessage(12, LogLevel.Warning, "Failed to delete message {MessageId}: {ErrorMessage}")]
    public static partial void DeleteMessageFailed(ILogger logger, string messageId, string errorMessage);
}

/// <summary>
///     Source-generated logging methods for composite execution observer operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class CompositeExecutionObserverLogMessages
{
    [LoggerMessage(1, LogLevel.Warning, "Execution observer {Observer}.{Method} threw an exception and will be skipped.")]
    public static partial void ObserverFailure(ILogger logger, string observer, string method, Exception exception);
}

/// <summary>
///     Source-generated logging methods for pipeline runner operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class PipelineRunnerLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Storing retry options in context.Items: MaxItemRetries={MaxItemRetries}")]
    public static partial void StoringRetryOptions(ILogger logger, int maxItemRetries);

    [LoggerMessage(2, LogLevel.Debug, "graph.ErrorHandling.RetryOptions is null")]
    public static partial void RetryOptionsNull(ILogger logger);

    [LoggerMessage(3, LogLevel.Debug, "CircuitBreakerManager created and stored in context")]
    public static partial void CircuitBreakerManagerCreated(ILogger logger);

    [LoggerMessage(4, LogLevel.Warning, "Node {NodeId} failed with exception type {ExceptionType}: {ExceptionMessage}")]
    public static partial void NodeFailed(ILogger logger, string nodeId, string exceptionType, string exceptionMessage);

    [LoggerMessage(5, LogLevel.Warning,
        "Node {NodeId} uses ResilientExecutionStrategy but MaxNodeRestartAttempts is {MaxAttempts} (must be > 0). Restart functionality is disabled. Configure: builder.WithRetryOptions(o => o.WithMaxNodeRestartAttempts(3))")]
    public static partial void ResilientStrategyWithoutRestartAttempts(ILogger logger, string nodeId, int maxAttempts);

    [LoggerMessage(6, LogLevel.Warning,
        "Node {NodeId} has MaxMaterializedItems set to null. Restart functionality is disabled for streaming inputs. Configure: builder.WithRetryOptions(o => o.WithMaxMaterializedItems(1000))")]
    public static partial void ResilientStrategyWithoutMaterializedItems(ILogger logger, string nodeId);

    [LoggerMessage(7, LogLevel.Warning, "Preserving original exception {ExceptionType} for parallel execution of node {NodeId}")]
    public static partial void PreservingExceptionForParallelExecution(ILogger logger, string exceptionType, string nodeId);

    [LoggerMessage(8, LogLevel.Warning, "Preserving OperationCanceledException for node {NodeId}")]
    public static partial void PreservingCancellationException(ILogger logger, string nodeId);

    [LoggerMessage(9, LogLevel.Warning, "Wrapping non-PipelineException {ExceptionType} in PipelineExecutionException for node {NodeId}")]
    public static partial void WrappingException(ILogger logger, string exceptionType, string nodeId);
}

/// <summary>
///     Source-generated logging methods for error handling service operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class ErrorHandlingServiceLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Applying retry delay of {Delay}ms for node {NodeId} after {RetryCount} retries")]
    public static partial void ApplyingRetryDelay(ILogger logger, double delay, string nodeId, int retryCount);

    [LoggerMessage(2, LogLevel.Warning, "Failed to apply retry delay for node {NodeId}. Continuing with retry without delay.")]
    public static partial void RetryDelayFailed(ILogger logger, Exception exception, string nodeId);
}

/// <summary>
///     Source-generated logging methods for persistence service operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class PersistenceServiceLogMessages
{
    [LoggerMessage(1, LogLevel.Error, "State snapshot failed for node {NodeId}")]
    public static partial void StateSnapshotFailed(ILogger logger, Exception exception, string nodeId);
}

/// <summary>
///     Source-generated logging methods for default error handler factory operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class DefaultErrorHandlerFactoryLogMessages
{
    [LoggerMessage(1, LogLevel.Warning, "Failed to create error handler of type {HandlerType}")]
    public static partial void ErrorHandlerCreationFailed(ILogger logger, string handlerType);
}

/// <summary>
///     Source-generated logging methods for default lineage factory operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class DefaultLineageFactoryLogMessages
{
    [LoggerMessage(1, LogLevel.Warning, "Failed to create lineage sink of type {SinkType}")]
    public static partial void LineageSinkCreationFailed(ILogger logger, string sinkType);
}

/// <summary>
///     Source-generated logging methods for pipeline context retry delay extensions.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class PipelineContextRetryDelayExtensionsLogMessages
{
    [LoggerMessage(1, LogLevel.Warning,
        "RetryDelayStrategy: Failed to create strategy from configuration. BackoffStrategy={BackoffStrategy}, JitterStrategy={JitterStrategy}")]
    public static partial void RetryDelayStrategyCreationFailed(ILogger logger, Exception exception, string backoffStrategy, string jitterStrategy);
}

/// <summary>
///     Source-generated logging methods for branch node operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class BranchNodeLogMessages
{
    [LoggerMessage(1, LogLevel.Warning, "Exception in branch handler {BranchIndex} for node '{NodeId}'")]
    public static partial void BranchHandlerException(ILogger logger, Exception exception, int branchIndex, string nodeId);

    [LoggerMessage(2, LogLevel.Warning, "Exception in branch handler {BranchIndex} for node '{NodeId}'. {AdditionalMessage}")]
    public static partial void BranchHandlerExceptionWithMessage(ILogger logger, Exception exception, int branchIndex, string nodeId, string additionalMessage);
}
