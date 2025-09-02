namespace NPipeline.ErrorHandling;

/// <summary>
///     Base exception type for all pipeline execution errors.
///     See <see href="~/docs/reference/api/exceptions.md#pipelineexception" /> for detailed documentation.
/// </summary>
public abstract class PipelineException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="PipelineException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    protected PipelineException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="PipelineException" /> class with a specified error message
    ///     and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    protected PipelineException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
///     Exception thrown when a node encounters an error during execution.
///     See <see href="~/docs/reference/api/exceptions.md#nodeexecutionexception" /> for detailed documentation.
/// </summary>
public sealed class NodeExecutionException : PipelineException
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="NodeExecutionException" /> class with the node ID and error message.
    /// </summary>
    /// <param name="nodeId">The ID of the node that failed.</param>
    /// <param name="message">The error message describing the failure.</param>
    public NodeExecutionException(string nodeId, string message) : base($"Node '{nodeId}' execution failed: {message}")
    {
        NodeId = nodeId;
        ErrorCode = "NODE_EXECUTION_ERROR";
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="NodeExecutionException" /> class with the node ID, error message,
    ///     and inner exception.
    /// </summary>
    /// <param name="nodeId">The ID of the node that failed.</param>
    /// <param name="message">The error message describing the failure.</param>
    /// <param name="innerException">The exception that caused the node to fail.</param>
    public NodeExecutionException(string nodeId, string message, Exception innerException)
        : base($"Node '{nodeId}' execution failed: {message}", innerException)
    {
        NodeId = nodeId;
        ErrorCode = "NODE_EXECUTION_ERROR";
    }

    /// <summary>
    ///     Gets the ID of the node that failed.
    /// </summary>
    public string NodeId { get; }

    /// <summary>
    ///     Gets the error code associated with this exception.
    /// </summary>
    public string ErrorCode { get; }
}

/// <summary>
///     Exception thrown when a pipeline encounters an error during execution.
///     See <see href="~/docs/reference/api/exceptions.md#pipelineexecutionexception" /> for detailed documentation.
/// </summary>
public sealed class PipelineExecutionException : PipelineException
{
    public PipelineExecutionException() : base("Pipeline execution failed.")
    {
        ErrorCode = "PIPELINE_EXECUTION_ERROR";
    }

    public PipelineExecutionException(string message) : base($"Pipeline execution failed: {message}")
    {
        ErrorCode = "PIPELINE_EXECUTION_ERROR";
    }

    public PipelineExecutionException(string message, Exception innerException) : base($"Pipeline execution failed: {message}", innerException)
    {
        ErrorCode = "PIPELINE_EXECUTION_ERROR";
    }

    /// <summary>
    ///     Gets the error code associated with this exception.
    /// </summary>
    public string ErrorCode { get; }
}

/// <summary>
///     Exception thrown when the circuit breaker trips due to too many consecutive failures.
///     See <see href="~/docs/reference/api/exceptions.md#circuitbreakertrippedexception" /> for detailed documentation.
/// </summary>
public sealed class CircuitBreakerTrippedException : PipelineException
{
    public CircuitBreakerTrippedException() : base("Circuit breaker tripped.")
    {
        ErrorCode = "CIRCUIT_BREAKER_TRIPPED";
    }

    public CircuitBreakerTrippedException(string message) : base(message)
    {
        ErrorCode = "CIRCUIT_BREAKER_TRIPPED";
    }

    public CircuitBreakerTrippedException(string message, Exception innerException) : base(message, innerException)
    {
        ErrorCode = "CIRCUIT_BREAKER_TRIPPED";
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CircuitBreakerTrippedException" /> class with the failure threshold.
    /// </summary>
    /// <param name="failureThreshold">The number of consecutive failures that triggered the circuit breaker.</param>
    public CircuitBreakerTrippedException(int failureThreshold)
        : base($"Circuit breaker tripped after reaching failure threshold of {failureThreshold} consecutive attempts.")
    {
        FailureThreshold = failureThreshold;
        ErrorCode = "CIRCUIT_BREAKER_TRIPPED";
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CircuitBreakerTrippedException" /> class with the failure threshold and node ID.
    /// </summary>
    /// <param name="failureThreshold">The number of consecutive failures that triggered the circuit breaker.</param>
    /// <param name="nodeId">The ID of the node that triggered the circuit breaker.</param>
    public CircuitBreakerTrippedException(int failureThreshold, string nodeId)
        : base($"Circuit breaker tripped for node '{nodeId}' after reaching failure threshold of {failureThreshold} consecutive attempts.")
    {
        FailureThreshold = failureThreshold;
        NodeId = nodeId;
        ErrorCode = "CIRCUIT_BREAKER_TRIPPED";
    }

    /// <summary>
    ///     Gets the failure threshold that triggered the circuit breaker.
    /// </summary>
    public int FailureThreshold { get; }

    /// <summary>
    ///     Gets the ID of the node that triggered the circuit breaker, if applicable.
    /// </summary>
    public string? NodeId { get; }

    /// <summary>
    ///     Gets the error code associated with this exception.
    /// </summary>
    public string ErrorCode { get; }
}

/// <summary>
///     Exception thrown when all retry attempts have been exhausted.
///     See <see href="~/docs/reference/api/exceptions.md#retryexhaustedexception" /> for detailed documentation.
/// </summary>
public sealed class RetryExhaustedException : PipelineException
{
    public RetryExhaustedException() : base("Retry attempts exhausted.")
    {
        ErrorCode = "RETRY_EXHAUSTED";
        NodeId = string.Empty;
    }

    public RetryExhaustedException(string message) : base(message)
    {
        ErrorCode = "RETRY_EXHAUSTED";
        NodeId = string.Empty;
    }

    public RetryExhaustedException(string message, Exception innerException) : base(message, innerException)
    {
        ErrorCode = "RETRY_EXHAUSTED";
        NodeId = string.Empty;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="RetryExhaustedException" /> class with the node ID, attempt count, and last exception.
    /// </summary>
    /// <param name="nodeId">The ID of the node that exhausted its retry attempts.</param>
    /// <param name="attemptCount">The total number of attempts made.</param>
    /// <param name="lastException">The last exception that occurred during the final retry attempt.</param>
    public RetryExhaustedException(string nodeId, int attemptCount, Exception lastException)
        : base($"Retry attempts exhausted for node '{nodeId}' after {attemptCount} attempts.", lastException)
    {
        NodeId = nodeId;
        AttemptCount = attemptCount;
        ErrorCode = "RETRY_EXHAUSTED";
    }

    /// <summary>
    ///     Gets the ID of the node that exhausted its retry attempts.
    /// </summary>
    public string NodeId { get; }

    /// <summary>
    ///     Gets the total number of attempts made.
    /// </summary>
    public int AttemptCount { get; }

    /// <summary>
    ///     Gets the error code associated with this exception.
    /// </summary>
    public string ErrorCode { get; }
}

/// <summary>
///     Exception thrown when the circuit breaker is open and blocks execution.
///     See <see href="~/docs/reference/api/exceptions.md#circuitbreakeropenexception" /> for detailed documentation.
/// </summary>
public sealed class CircuitBreakerOpenException : PipelineException
{
    public CircuitBreakerOpenException() : base("Circuit breaker is open and blocking execution.")
    {
        ErrorCode = "CIRCUIT_BREAKER_OPEN";
    }

    public CircuitBreakerOpenException(string message) : base(message)
    {
        ErrorCode = "CIRCUIT_BREAKER_OPEN";
    }

    public CircuitBreakerOpenException(string message, Exception innerException) : base(message, innerException)
    {
        ErrorCode = "CIRCUIT_BREAKER_OPEN";
    }

    /// <summary>
    ///     Gets the error code associated with this exception.
    /// </summary>
    public string ErrorCode { get; }
}

/// <summary>
///     Exception thrown when a node restart is requested.
///     See <see href="~/docs/reference/api/exceptions.md#noderestartexception" /> for detailed documentation.
/// </summary>
public sealed class NodeRestartException : PipelineException
{
    public NodeRestartException(string message) : base(message)
    {
        ErrorCode = "NODE_RESTART";
    }

    public NodeRestartException(string message, Exception innerException) : base(message, innerException)
    {
        ErrorCode = "NODE_RESTART";
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="NodeRestartException" /> class with a cancellation token.
    /// </summary>
    /// <param name="message">The error message.</param>
    /// <param name="cancellationToken">The cancellation token associated with the restart.</param>
    public NodeRestartException(string message, CancellationToken cancellationToken) : base(message)
    {
        CancellationToken = cancellationToken;
        ErrorCode = "NODE_RESTART";
    }

    /// <summary>
    ///     Gets the cancellation token associated with the node restart.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>
    ///     Gets the error code associated with this exception.
    /// </summary>
    public string ErrorCode { get; }
}

/// <summary>
///     Exception thrown when the maximum number of node restart attempts is exceeded.
///     See <see href="~/docs/reference/api/exceptions.md#maxnoderestartattemptsexceededexception" /> for detailed documentation.
/// </summary>
public sealed class MaxNodeRestartAttemptsExceededException : PipelineException
{
    public MaxNodeRestartAttemptsExceededException(string message) : base(message)
    {
        ErrorCode = "MAX_NODE_RESTART_ATTEMPTS_EXCEEDED";
    }

    public MaxNodeRestartAttemptsExceededException(string message, Exception innerException) : base(message, innerException)
    {
        ErrorCode = "MAX_NODE_RESTART_ATTEMPTS_EXCEEDED";
    }

    /// <summary>
    ///     Gets the error code associated with this exception.
    /// </summary>
    public string ErrorCode { get; }
}
