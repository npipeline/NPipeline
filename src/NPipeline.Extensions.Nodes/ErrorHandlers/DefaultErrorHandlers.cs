using NPipeline.Nodes;
using NPipeline.Reliability;

// ReSharper disable once CheckNamespace
namespace NPipeline.Extensions.Nodes;

/// <summary>
///     Default resilience policy for validation nodes.
///     Translates <see cref="ValidationException" /> to the configured decision.
/// </summary>
public sealed class DefaultValidationErrorHandler<T>(
    ResilienceDecision onValidationFailure = ResilienceDecision.Skip)
    : ResiliencePolicyBase
{
    /// <inheritdoc />
    /// <remarks>
    ///     A <see cref="ValidationException" /> from this node gets the configured decision. Any other failure follows the node's
    ///     resilience options.
    /// </remarks>
    public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
    {
        if (failure.Node is ITransformNode<T, T> && failure.Exception is ValidationException)
            return ValueTask.FromResult(onValidationFailure);

        return base.DecideItemFailureAsync(failure, cancellationToken);
    }
}

/// <summary>
///     Default resilience policy for filtering nodes.
///     Translates <see cref="FilteringException" /> to the configured decision.
/// </summary>
public sealed class DefaultFilteringErrorHandler<T>(
    ResilienceDecision onFilteredOut = ResilienceDecision.Skip)
    : ResiliencePolicyBase
{
    /// <inheritdoc />
    /// <remarks>
    ///     A <see cref="FilteringException" /> from this node gets the configured decision. Any other failure follows the node's
    ///     resilience options.
    /// </remarks>
    public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
    {
        if (failure.Node is ITransformNode<T, T> && failure.Exception is FilteringException)
            return ValueTask.FromResult(onFilteredOut);

        return base.DecideItemFailureAsync(failure, cancellationToken);
    }
}

/// <summary>
///     Default resilience policy for type conversion nodes.
///     Translates <see cref="TypeConversionException" /> to the configured decision.
/// </summary>
public sealed class DefaultTypeConversionErrorHandler<TIn, TOut>(
    ResilienceDecision onConversionFailure = ResilienceDecision.Skip)
    : ResiliencePolicyBase
{
    /// <inheritdoc />
    /// <remarks>
    ///     A <see cref="TypeConversionException" /> from this node gets the configured decision. Any other failure follows the node's
    ///     resilience options.
    /// </remarks>
    public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TItemIn>(ItemFailure<TItemIn> failure, CancellationToken cancellationToken)
    {
        if (failure.Node is ITransformNode<TIn, TOut> && failure.Exception is TypeConversionException)
            return ValueTask.FromResult(onConversionFailure);

        return base.DecideItemFailureAsync(failure, cancellationToken);
    }
}
