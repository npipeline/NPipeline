using NPipeline.Configuration;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Defines the contract for a circuit breaker manager that handles creation and lifecycle management.
/// </summary>
internal interface ICircuitBreakerManager
{
    /// <summary>
    ///     Gets or creates a circuit breaker for the specified node ID with the given options.
    /// </summary>
    /// <param name="nodeId">The unique identifier for the node.</param>
    /// <param name="options">The circuit breaker configuration options.</param>
    /// <returns>The circuit breaker instance for the node.</returns>
    ICircuitBreaker GetCircuitBreaker(string nodeId, PipelineCircuitBreakerOptions options);

    /// <summary>
    ///     Removes the circuit breaker for the specified node ID.
    /// </summary>
    /// <param name="nodeId">The unique identifier for the node.</param>
    void RemoveCircuitBreaker(string nodeId);

    /// <summary>
    ///     Manually triggers cleanup of inactive circuit breakers.
    /// </summary>
    /// <returns>The number of circuit breakers that were removed.</returns>
    int TriggerCleanup();

    /// <summary>
    ///     Gets the current count of tracked circuit breakers.
    /// </summary>
    /// <returns>The number of tracked circuit breakers.</returns>
    int GetTrackedCircuitBreakerCount();
}
