namespace NPipeline.Reliability;

/// <summary>
///     Base class for resilience policies. Every decision defaults to <see cref="DefaultResiliencePolicy" />, which
///     follows the node's <see cref="PipelineResilienceOptions" />, so override only the decisions you care about.
/// </summary>
/// <remarks>
///     <code>
///     // Dead-letter validation failures; leave every other failure to the node's options.
///     public sealed class DeadLetterInvalidItems : ResiliencePolicyBase
///     {
///         public override ValueTask&lt;ResilienceDecision&gt; DecideItemFailureAsync&lt;TIn&gt;(
///             ItemFailure&lt;TIn&gt; failure, CancellationToken cancellationToken)
///         {
///             return failure.Exception is ValidationException
///                 ? ValueTask.FromResult(ResilienceDecision.DeadLetter)
///                 : base.DecideItemFailureAsync(failure, cancellationToken);
///         }
///     }
///     </code>
/// </remarks>
public abstract class ResiliencePolicyBase : IResiliencePolicy
{
    /// <inheritdoc />
    /// <remarks>Default: <see cref="DefaultResiliencePolicy.DecideItemFailureAsync{TIn}" />.</remarks>
    public virtual ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
    {
        return DefaultResiliencePolicy.Instance.DecideItemFailureAsync(failure, cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>Default: <see cref="DefaultResiliencePolicy.DecideRestartAsync" />.</remarks>
    public virtual ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
    {
        return DefaultResiliencePolicy.Instance.DecideRestartAsync(failure, cancellationToken);
    }

    /// <inheritdoc />
    /// <remarks>Default: <see cref="DefaultResiliencePolicy.DecideNodeFailureAsync" />.</remarks>
    public virtual ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
    {
        return DefaultResiliencePolicy.Instance.DecideNodeFailureAsync(failure, cancellationToken);
    }
}
