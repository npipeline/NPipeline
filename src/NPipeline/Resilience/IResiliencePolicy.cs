using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Resilience;

/// <summary>
///     Central resilience entry point used by runtime execution.
/// </summary>
public interface IResiliencePolicy
{
    /// <summary>
    ///     Decides how runtime should respond to a node-level failure.
    /// </summary>
    Task<ResilienceDecision> DecideNodeFailureAsync(
        NodeDefinition nodeDefinition,
        INode node,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Decides how runtime should respond to a pipeline-level stream failure.
    /// </summary>
    Task<ResilienceDecision> DecidePipelineFailureAsync(
        string nodeId,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Decides how runtime should respond to an item-level failure.
    /// </summary>
    Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Resolves retry delay for the given attempt.
    /// </summary>
    ValueTask<TimeSpan> GetRetryDelayAsync(
        PipelineContext context,
        int attemptNumber,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Resolves a circuit breaker for the node when circuit breaking is enabled.
    /// </summary>
    IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId);
}
