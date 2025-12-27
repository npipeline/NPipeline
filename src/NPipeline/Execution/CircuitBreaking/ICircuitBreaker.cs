using NPipeline.Configuration;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Defines contract for a circuit breaker implementation.
/// </summary>
internal interface ICircuitBreaker
{
    /// <summary>
    ///     Gets current state of circuit breaker.
    /// </summary>
    CircuitBreakerState State { get; }

    /// <summary>
    ///     Gets circuit breaker configuration options.
    /// </summary>
    PipelineCircuitBreakerOptions Options { get; }

    /// <summary>
    ///     Determines whether an operation can be executed based on current state.
    /// </summary>
    /// <returns>True if operation is allowed, false otherwise.</returns>
    bool CanExecute();

    /// <summary>
    ///     Records a successful operation and updates the circuit breaker state accordingly.
    /// </summary>
    /// <returns>The result of the operation recording including any state changes.</returns>
    CircuitBreakerExecutionResult RecordSuccess();

    /// <summary>
    ///     Records a failed operation and updates the circuit breaker state accordingly.
    /// </summary>
    /// <returns>The result of the operation recording including any state changes.</returns>
    CircuitBreakerExecutionResult RecordFailure();

    /// <summary>
    ///     Gets current statistics from the circuit breaker.
    /// </summary>
    /// <returns>The current window statistics.</returns>
    WindowStatistics GetStatistics();
}
