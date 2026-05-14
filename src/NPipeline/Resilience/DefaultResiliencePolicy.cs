using Microsoft.Extensions.Logging;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Resilience;

/// <summary>
///     Default resilience policy that coordinates error decisions, retry delay, and circuit breakers.
/// </summary>
public sealed class DefaultResiliencePolicy : IResiliencePolicy
{
    /// <summary>
    ///     Shared singleton instance for stateless default behavior.
    /// </summary>
    public static DefaultResiliencePolicy Instance { get; } = new();

    /// <inheritdoc />
    public ValueTask<TimeSpan> GetRetryDelayAsync(
        PipelineContext context,
        int attemptNumber,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.GetRetryDelayStrategy().GetDelayAsync(attemptNumber, cancellationToken);
    }

    /// <inheritdoc />
    public Task<ResilienceDecision> DecideNodeFailureAsync(
        NodeDefinition nodeDefinition,
        INode node,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(nodeDefinition);
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentNullException.ThrowIfNull(context);

        return Task.FromResult(ResilienceDecision.Fail);
    }

    /// <inheritdoc />
    public Task<ResilienceDecision> DecidePipelineFailureAsync(
        string nodeId,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentNullException.ThrowIfNull(context);

        return Task.FromResult(ResilienceDecision.Fail);
    }

    /// <inheritdoc />
    public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(exception);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeId);

        return Task.FromResult(ResilienceDecision.Fail);
    }

    /// <inheritdoc />
    public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeId);

        if (context.CircuitBreakerOptions is not { Enabled: true } circuitBreakerOptions)
            return null;

        EnsureCircuitBreakerManagerIsAvailable(context);

        if (context.CircuitBreakerManager is not ICircuitBreakerManager manager)
            return null;

        var breaker = manager.GetCircuitBreaker(nodeId, circuitBreakerOptions);
        return new ResilienceCircuitBreakerAdapter(breaker);
    }

    private static void EnsureCircuitBreakerManagerIsAvailable(PipelineContext context)
    {
        if (context.CircuitBreakerOptions is not { Enabled: true })
            return;

        if (context.CircuitBreakerManager is ICircuitBreakerManager)
            return;

        var logger = context.LoggerFactory.CreateLogger(nameof(CircuitBreakerManager));
        var manager = context.CreateAndRegister(new CircuitBreakerManager(logger, context.CircuitBreakerMemoryOptions));
        context.CircuitBreakerManager = manager;
    }

    private sealed class ResilienceCircuitBreakerAdapter(ICircuitBreaker circuitBreaker) : IResilienceCircuitBreaker
    {
        private readonly ICircuitBreaker _circuitBreaker = circuitBreaker;

        public bool CanExecute()
        {
            return _circuitBreaker.CanExecute();
        }

        public void RecordSuccess()
        {
            _ = _circuitBreaker.RecordSuccess();
        }

        public ResilienceCircuitResult RecordFailure()
        {
            var result = _circuitBreaker.RecordFailure();
            return new ResilienceCircuitResult(result.Allowed, result.Message, GetSnapshot());
        }

        public ResilienceCircuitSnapshot GetSnapshot()
        {
            var options = _circuitBreaker.Options;
            var stats = options.TrackOperationsInWindow
                ? _circuitBreaker.GetStatistics()
                : new WindowStatistics(0, 0, 0, 0);

            return new ResilienceCircuitSnapshot(
                _circuitBreaker.State.ToString(),
                options.FailureThreshold,
                stats.FailureCount,
                stats.TotalOperations);
        }
    }
}
