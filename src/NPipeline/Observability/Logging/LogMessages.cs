using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;

namespace NPipeline.Observability.Logging;

/// <summary>
///     Source-generated logging methods for circuit breaker operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class CircuitBreakerLogMessages
{
    [LoggerMessage(1, LogLevel.Warning, "Circuit breaker for node {NodeId} opened: {Reason}")]
    public static partial void Opened(ILogger logger, string nodeId, string reason);

    [LoggerMessage(2, LogLevel.Information, "Circuit breaker for node {NodeId} half-opened: {Reason}")]
    public static partial void HalfOpened(ILogger logger, string nodeId, string reason);

    [LoggerMessage(3, LogLevel.Information, "Circuit breaker for node {NodeId} closed: {Reason}")]
    public static partial void Closed(ILogger logger, string nodeId, string reason);

    [LoggerMessage(4, LogLevel.Warning, "Execution observer failed handling the circuit breaker transition from {PreviousState} to {State} for node {NodeId}")]
    public static partial void StateChangeListenerFailed(ILogger logger, Exception exception, string nodeId, string previousState, string state);

    [LoggerMessage(5, LogLevel.Debug, "Item on node {NodeId} is waiting up to {MaxPause} for its open circuit breaker")]
    public static partial void Pausing(ILogger logger, string nodeId, TimeSpan maxPause);
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

    [LoggerMessage(7, LogLevel.Debug, "Storing AutoObservabilityScope in context for node: {NodeId}")]
    public static partial void AutoObservabilityScopeStored(ILogger logger, string nodeId);

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
    [LoggerMessage(3, LogLevel.Warning, "Node {NodeId} failed after {Attempts} runs. Throwing RetryExhaustedException.")]
    public static partial void RetryExhausted(ILogger logger, string nodeId, int attempts);

    [LoggerMessage(7, LogLevel.Debug,
        "Resilience policy returned decision {Decision} for node {NodeId}. Restarts so far: {Restarts}, checkpoint: {Checkpoint}.")]
    public static partial void ErrorHandlerDecision(ILogger logger, string decision, string nodeId, int restarts, long checkpoint);

    [LoggerMessage(9, LogLevel.Debug, "Applying restart delay of {Delay}ms for node {NodeId} before restart {Restart}")]
    public static partial void ApplyingRetryDelay(ILogger logger, double delay, string nodeId, int restart);
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
    [LoggerMessage(1, LogLevel.Debug, "Setting pipeline resilience options on PipelineContext: ItemRetry.MaxRetries={MaxItemRetries}")]
    public static partial void StoringResilienceOptions(ILogger logger, int maxItemRetries);

    [LoggerMessage(4, LogLevel.Warning, "Node {NodeId} failed with exception type {ExceptionType}: {ExceptionMessage}")]
    public static partial void NodeFailed(ILogger logger, string nodeId, string exceptionType, string exceptionMessage);

    [LoggerMessage(8, LogLevel.Warning, "Preserving OperationCanceledException for node {NodeId}")]
    public static partial void PreservingCancellationException(ILogger logger, string nodeId);

    [LoggerMessage(9, LogLevel.Warning, "Wrapping non-PipelineException {ExceptionType} in PipelineExecutionException for node {NodeId}")]
    public static partial void WrappingException(ILogger logger, string exceptionType, string nodeId);

    [LoggerMessage(10, LogLevel.Warning, "Cleanup after a failed pipeline run threw {ExceptionType}; the original failure is preserved")]
    public static partial void CleanupFailed(ILogger logger, Exception exception, string exceptionType);

    [LoggerMessage(11, LogLevel.Warning, "Reporting the pipeline failure to the observer threw {ExceptionType}; the original failure is preserved")]
    public static partial void FailureReportingFailed(ILogger logger, Exception exception, string exceptionType);

    [LoggerMessage(12, LogLevel.Warning, "A callback registered on the run's cancellation token threw while the run was being cancelled")]
    public static partial void CancellationCallbackFailed(ILogger logger, Exception exception);
}

/// <summary>
///     Source-generated logging methods for error handling service operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class ErrorHandlingServiceLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Applying retry delay of {Delay}ms for node {NodeId} before retry {Retry}")]
    public static partial void ApplyingRetryDelay(ILogger logger, double delay, string nodeId, int retry);
}

/// <summary>
///     Source-generated logging methods for item-level retry operations.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class PerItemRetryExecutorLogMessages
{
    [LoggerMessage(1, LogLevel.Debug, "Applying retry delay of {Delay}ms for item on node {NodeId} before retry {Retry}")]
    public static partial void ApplyingRetryDelay(ILogger logger, double delay, string nodeId, int retry);

    [LoggerMessage(2, LogLevel.Debug, "Item on node {NodeId} failed on attempt {Attempt}")]
    public static partial void AttemptFailed(ILogger logger, Exception exception, string nodeId, int attempt);
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
///     Source-generated logging methods for runtime pipeline binding.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class RuntimePipelineBinderLogMessages
{
    [LoggerMessage(1, LogLevel.Warning,
        "Item-level lineage sink {SinkType} is configured but item-level lineage is disabled, so it will never be invoked. "
        + "Call builder.EnableItemLevelLineage() in the pipeline definition to enable per-item lineage records.")]
    public static partial void ItemLevelLineageSinkIgnored(ILogger logger, string sinkType);
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

/// <summary>
///     Source-generated logging methods for the counting multicast data streams.
/// </summary>
[ExcludeFromCodeCoverage]
internal static partial class DataStreamLog
{
    [LoggerMessage(1, LogLevel.Warning, "Multicast pump for stream '{StreamName}' did not shut down within {Timeout} after disposal cancelled it")]
    public static partial void MulticastPumpShutdownTimedOut(ILogger logger, string streamName, TimeSpan timeout);
}
