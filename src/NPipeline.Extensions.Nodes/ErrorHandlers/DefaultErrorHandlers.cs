using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

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
    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        if (node is not ITransformNode<T, T>)
            return Task.FromResult(ResilienceDecision.Fail);

        var decision = exception is ValidationException
            ? onValidationFailure
            : ResilienceDecision.Fail;

        return Task.FromResult(decision);
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
    public override Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        if (node is not ITransformNode<T, T>)
            return Task.FromResult(ResilienceDecision.Fail);

        var decision = exception is FilteringException
            ? onFilteredOut
            : ResilienceDecision.Fail;

        return Task.FromResult(decision);
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
    public override Task<ResilienceDecision> DecideItemFailureAsync<TItemIn, TItemOut>(
        ITransformNode<TItemIn, TItemOut> node,
        TItemIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        if (node is not ITransformNode<TIn, TOut>)
            return Task.FromResult(ResilienceDecision.Fail);

        var decision = exception is TypeConversionException
            ? onConversionFailure
            : ResilienceDecision.Fail;

        return Task.FromResult(decision);
    }
}
