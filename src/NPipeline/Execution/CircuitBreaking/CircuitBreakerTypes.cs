namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Represents the outcome of an operation tracked by the circuit breaker.
/// </summary>
internal enum OperationOutcome
{
    /// <summary>
    ///     The operation completed successfully.
    /// </summary>
    Success,

    /// <summary>
    ///     The operation failed.
    /// </summary>
    Failure,
}

/// <summary>
///     Represents a single operation record with timestamp and outcome.
/// </summary>
/// <param name="Timestamp">The UTC timestamp when the operation was recorded.</param>
/// <param name="Outcome">The outcome of the operation.</param>
internal record OperationRecord(DateTime Timestamp, OperationOutcome Outcome);

/// <summary>
///     Statistics for operations within the rolling window.
/// </summary>
/// <param name="TotalOperations">Total number of operations in the window.</param>
/// <param name="FailureCount">Number of failed operations in the window.</param>
/// <param name="SuccessCount">Number of successful operations in the window.</param>
/// <param name="FailureRate">Failure rate (0.0 to 1.0) in the window.</param>
internal record WindowStatistics(
    int TotalOperations,
    int FailureCount,
    int SuccessCount,
    double FailureRate);

/// <summary>
///     Result of a circuit breaker operation attempt.
/// </summary>
/// <param name="Allowed">Whether the operation was allowed to proceed.</param>
/// <param name="StateChanged">Whether the circuit breaker state changed as a result of this operation.</param>
/// <param name="NewState">The new state if a state change occurred.</param>
/// <param name="Message">Descriptive message about the operation result.</param>
internal record CircuitBreakerExecutionResult(
    bool Allowed,
    bool StateChanged,
    CircuitBreakerState? NewState,
    string Message);
