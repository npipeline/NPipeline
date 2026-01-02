using NPipeline.ErrorHandling;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Core.ErrorHandlers;

/// <summary>
///     Default error handler for validation nodes.
///     Translates <see cref="ValidationException" /> to the configured <see cref="NodeErrorDecision" />.
///     Other exception types result in <see cref="NodeErrorDecision.Fail" />.
/// </summary>
public sealed class DefaultValidationErrorHandler<T> : INodeErrorHandler<ITransformNode<T, T>, T>
{
    private readonly NodeErrorDecision _onValidationFailure;

    /// <summary>
    ///     Initializes a new instance with the specified error decision for validation failures.
    /// </summary>
    /// <param name="onValidationFailure">Decision to make when validation fails. Defaults to Skip.</param>
    public DefaultValidationErrorHandler(NodeErrorDecision onValidationFailure = NodeErrorDecision.Skip)
    {
        _onValidationFailure = onValidationFailure;
    }

    /// <summary>
    ///     Handles validation node errors by converting ValidationException to the configured decision.
    /// </summary>
    public Task<NodeErrorDecision> HandleAsync(
        ITransformNode<T, T> node,
        T failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var decision = error is ValidationException validationEx
            ? _onValidationFailure
            : NodeErrorDecision.Fail;

        return Task.FromResult(decision);
    }
}

/// <summary>
///     Default error handler for filtering nodes.
///     Translates <see cref="FilteringException" /> to the configured <see cref="NodeErrorDecision" />.
///     Other exception types result in <see cref="NodeErrorDecision.Fail" />.
/// </summary>
public sealed class DefaultFilteringErrorHandler<T> : INodeErrorHandler<ITransformNode<T, T>, T>
{
    private readonly NodeErrorDecision _onFilteredOut;

    /// <summary>
    ///     Initializes a new instance with the specified error decision for filtered items.
    /// </summary>
    /// <param name="onFilteredOut">Decision to make when item is filtered out. Defaults to Skip.</param>
    public DefaultFilteringErrorHandler(NodeErrorDecision onFilteredOut = NodeErrorDecision.Skip)
    {
        _onFilteredOut = onFilteredOut;
    }

    /// <summary>
    ///     Handles filtering node errors by converting FilteringException to the configured decision.
    /// </summary>
    public Task<NodeErrorDecision> HandleAsync(
        ITransformNode<T, T> node,
        T failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var decision = error is FilteringException filteringEx
            ? _onFilteredOut
            : NodeErrorDecision.Fail;

        return Task.FromResult(decision);
    }
}

/// <summary>
///     Default error handler for type conversion nodes.
///     Translates <see cref="TypeConversionException" /> to the configured <see cref="NodeErrorDecision" />.
///     Other exception types result in <see cref="NodeErrorDecision.Fail" />.
/// </summary>
public sealed class DefaultTypeConversionErrorHandler<TIn, TOut> : INodeErrorHandler<ITransformNode<TIn, TOut>, TIn>
{
    private readonly NodeErrorDecision _onConversionFailure;

    /// <summary>
    ///     Initializes a new instance with the specified error decision for conversion failures.
    /// </summary>
    /// <param name="onConversionFailure">Decision to make when conversion fails. Defaults to Skip.</param>
    public DefaultTypeConversionErrorHandler(NodeErrorDecision onConversionFailure = NodeErrorDecision.Skip)
    {
        _onConversionFailure = onConversionFailure;
    }

    /// <summary>
    ///     Handles type conversion node errors by converting TypeConversionException to the configured decision.
    /// </summary>
    public Task<NodeErrorDecision> HandleAsync(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var decision = error is TypeConversionException
            ? _onConversionFailure
            : NodeErrorDecision.Fail;

        return Task.FromResult(decision);
    }
}
