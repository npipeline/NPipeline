using Microsoft.Extensions.Logging;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution.CircuitBreaking;

/// <summary>
///     Finds the circuit breaker for a node from the node's resilience options and the run's breaker manager.
/// </summary>
internal static class CircuitBreakerResolver
{
    /// <summary>
    ///     The node's breaker, or null when its options configure none.
    /// </summary>
    public static IResilienceCircuitBreaker? Resolve(PipelineContext context, string nodeId, PipelineResilienceOptions options)
    {
        if (options.CircuitBreaker is not { Enabled: true } breakerOptions)
            return null;

        var manager = context.ExecutionConfiguration.CircuitBreakerManager ?? CreateManager(context);
        return new ResilienceCircuitBreakerAdapter(manager.GetCircuitBreaker(nodeId, breakerOptions));
    }

    /// <summary>
    ///     Creates the manager when a strategy runs outside a pipeline run, whose setup would otherwise have created it.
    /// </summary>
    private static ICircuitBreakerManager CreateManager(PipelineContext context)
    {
        var execution = context.ExecutionConfiguration;

        lock (execution)
        {
            if (execution.CircuitBreakerManager is { } existing)
                return existing;

            var logger = context.Observability.LoggerFactory.CreateLogger(nameof(CircuitBreakerManager));
            var manager = context.CreateAndRegister(new CircuitBreakerManager(logger, execution.CircuitBreakerMemoryOptions));
            execution.CircuitBreakerManager = manager;
            return manager;
        }
    }

    private sealed class ResilienceCircuitBreakerAdapter(ICircuitBreaker circuitBreaker) : IResilienceCircuitBreaker
    {
        public bool CanExecute()
        {
            return circuitBreaker.CanExecute();
        }

        public void RecordSuccess()
        {
            _ = circuitBreaker.RecordSuccess();
        }

        public ResilienceCircuitResult RecordFailure()
        {
            var result = circuitBreaker.RecordFailure();
            return new ResilienceCircuitResult(result.Allowed, result.Message, GetSnapshot());
        }

        public ResilienceCircuitSnapshot GetSnapshot()
        {
            var options = circuitBreaker.Options;
            var stats = options.TrackOperationsInWindow
                ? circuitBreaker.GetStatistics()
                : new WindowStatistics(0, 0, 0, 0);

            return new ResilienceCircuitSnapshot(
                circuitBreaker.State.ToString(),
                options.FailureThreshold,
                stats.FailureCount,
                stats.TotalOperations);
        }
    }
}
