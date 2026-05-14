using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Resilience;

/// <summary>
///     Base class for resilience policies that provides fail-fast defaults for all decision methods.
///     Override only the methods relevant to your scenario.
/// </summary>
/// <remarks>
///     <para>
///         Use this base class instead of implementing <see cref="IResiliencePolicy" /> directly
///         to avoid repeating boilerplate for decisions you don't need to customize.
///     </para>
///     <code>
///     // Minimal policy: only override item-level failures.
///     public sealed class RetryTransientPolicy : ResiliencePolicyBase
///     {
///         public override Task&lt;ResilienceDecision&gt; DecideItemFailureAsync&lt;TIn, TOut&gt;(
///             ITransformNode&lt;TIn, TOut&gt; node, TIn failedItem, Exception exception,
///             PipelineContext context, string nodeId, int retryAttempt, CancellationToken ct)
///         {
///             return Task.FromResult(exception is TimeoutException
///                 ? ResilienceDecision.Retry
///                 : ResilienceDecision.Fail);
///         }
///     }
///     </code>
/// </remarks>
public abstract class ResiliencePolicyBase : IResiliencePolicy
{
    /// <inheritdoc />
    /// <remarks>Default: <see cref="ResilienceDecision.Fail" />.</remarks>
    public virtual Task<ResilienceDecision> DecideNodeFailureAsync(
        NodeDefinition nodeDefinition,
        INode node,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(ResilienceDecision.Fail);
    }

    /// <inheritdoc />
    /// <remarks>Default: <see cref="ResilienceDecision.Fail" />.</remarks>
    public virtual Task<ResilienceDecision> DecidePipelineFailureAsync(
        string nodeId,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(ResilienceDecision.Fail);
    }

    /// <inheritdoc />
    /// <remarks>Default: <see cref="ResilienceDecision.Fail" />.</remarks>
    public virtual Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        return Task.FromResult(ResilienceDecision.Fail);
    }

    /// <inheritdoc />
    /// <remarks>Default: delegates to the pipeline's configured retry delay strategy.</remarks>
    public virtual ValueTask<TimeSpan> GetRetryDelayAsync(
        PipelineContext context,
        int attemptNumber,
        CancellationToken cancellationToken)
    {
        return context.GetRetryDelayStrategy().GetDelayAsync(attemptNumber, cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>Default: delegates to <see cref="DefaultResiliencePolicy" /> circuit breaker resolution.</remarks>
    public virtual IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
    {
        return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
    }
}
