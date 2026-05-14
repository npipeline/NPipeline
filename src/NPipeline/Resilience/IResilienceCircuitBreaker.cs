namespace NPipeline.Resilience;

/// <summary>
///     Snapshot of a resilience circuit breaker state.
/// </summary>
/// <param name="State">Current circuit state name.</param>
/// <param name="FailureThreshold">Configured threshold used for tripping.</param>
/// <param name="FailureCount">Current failure count in window (if tracked).</param>
/// <param name="TotalOperations">Current total operations in window (if tracked).</param>
public readonly record struct ResilienceCircuitSnapshot(
    string State,
    int FailureThreshold,
    int FailureCount,
    int TotalOperations);

/// <summary>
///     Result of recording a failed execution against the resilience circuit breaker.
/// </summary>
/// <param name="Allowed">Whether execution is still allowed after the failure record.</param>
/// <param name="Message">Diagnostic message from the underlying breaker.</param>
/// <param name="Snapshot">Circuit snapshot captured after the update.</param>
public readonly record struct ResilienceCircuitResult(
    bool Allowed,
    string Message,
    ResilienceCircuitSnapshot Snapshot);

/// <summary>
///     Unified circuit breaker abstraction exposed by the resilience policy.
/// </summary>
public interface IResilienceCircuitBreaker
{
    /// <summary>
    ///     Determines whether execution is currently allowed.
    /// </summary>
    bool CanExecute();

    /// <summary>
    ///     Records a successful execution.
    /// </summary>
    void RecordSuccess();

    /// <summary>
    ///     Records a failed execution.
    /// </summary>
    /// <returns>Result describing the circuit state after the failure.</returns>
    ResilienceCircuitResult RecordFailure();

    /// <summary>
    ///     Captures a lightweight snapshot of the current breaker state.
    /// </summary>
    ResilienceCircuitSnapshot GetSnapshot();
}
