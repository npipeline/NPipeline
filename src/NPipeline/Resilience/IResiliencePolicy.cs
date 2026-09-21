using NPipeline.Execution;
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
    ///     Resolves the backoff to wait before the given retry attempt.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="retryKind">
    ///     Whether a single item is being retried or the whole node is being restarted. The two have very different
    ///     costs — an item retry is cheap and typically wants a short backoff, a node restart replays the node's
    ///     entire input — so a policy will usually want to answer them differently.
    /// </param>
    /// <param name="attemptNumber">The 1-based number of the attempt about to be made.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    ValueTask<TimeSpan> GetRetryDelayAsync(
        PipelineContext context,
        RetryKind retryKind,
        int attemptNumber,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Resolves a circuit breaker for the node when circuit breaking is enabled.
    /// </summary>
    IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId);
}
