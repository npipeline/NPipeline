using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Core;

/// <summary>
///     A conversion node that transforms items from one type to another with comprehensive error handling.
/// </summary>
/// <typeparam name="TIn">The input type.</typeparam>
/// <typeparam name="TOut">The output type.</typeparam>
public sealed class TypeConversionNode<TIn, TOut> : TransformNode<TIn, TOut>
{
    private Func<TIn, TOut>? _converter;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TypeConversionNode{TIn, TOut}" /> class.
    /// </summary>
    public TypeConversionNode()
    {
    }

    /// <summary>
    ///     Sets the conversion function to use.
    /// </summary>
    /// <param name="converter">The conversion function.</param>
    /// <returns>This instance for chaining.</returns>
    public TypeConversionNode<TIn, TOut> WithConverter(Func<TIn, TOut> converter)
    {
        ArgumentNullException.ThrowIfNull(converter);
        _converter = converter;
        return this;
    }

    /// <summary>
    ///     Executes the conversion asynchronously.
    /// </summary>
    public override Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_converter == null)
            throw new InvalidOperationException("No converter has been configured. Use WithConverter() to set a conversion function.");

        try
        {
            var result = _converter(item);
            return Task.FromResult(result);
        }
        catch (TypeConversionException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new TypeConversionException(typeof(TIn), typeof(TOut), item,
                $"An error occurred during conversion: {ex.Message}", ex);
        }
    }

    /// <summary>
    ///     Executes the conversion synchronously (ValueTask override).
    /// </summary>
    protected override ValueTask<TOut> ExecuteValueTaskAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_converter == null)
            throw new InvalidOperationException("No converter has been configured. Use WithConverter() to set a conversion function.");

        try
        {
            var result = _converter(item);
            return new ValueTask<TOut>(result);
        }
        catch (TypeConversionException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new TypeConversionException(typeof(TIn), typeof(TOut), item,
                $"An error occurred during conversion: {ex.Message}", ex);
        }
    }
}
