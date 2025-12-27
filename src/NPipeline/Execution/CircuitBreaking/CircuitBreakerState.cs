namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Represents the state of a circuit breaker.
/// </summary>
internal enum CircuitBreakerState
{
    /// <summary>
    ///     Normal operating state where all operations are allowed.
    /// </summary>
    Closed,

    /// <summary>
    ///     Failure state where operations are blocked.
    /// </summary>
    Open,

    /// <summary>
    ///     Testing state where limited operations are allowed to test recovery.
    /// </summary>
    HalfOpen,
}
