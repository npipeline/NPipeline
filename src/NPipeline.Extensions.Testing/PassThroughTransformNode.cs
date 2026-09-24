// ReSharper disable ClassNeverInstantiated.Local

using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A testing utility transform node that passes through input data with optional type conversion.
///     This node is primarily used in testing scenarios where you need to simulate data transformation
///     without actual processing logic, or to convert between compatible types in a pipeline.
/// </summary>
/// <typeparam name="TIn">The input type of the transform node.</typeparam>
/// <typeparam name="TOut">The output type of the transform node.</typeparam>
public class PassThroughTransformNode<TIn, TOut> : TransformNode<TIn, TOut>
{
    /// <inheritdoc />
    public override ValueTask<TOut> TransformAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // If input is already the desired type, return directly
        if (item is TOut outItem)
            return ValueTask.FromResult<TOut>(outItem);

        // Handle null input for reference / nullable target types
        if (item == null)

            // If TOut is a reference type or nullable, default(TOut) is acceptable
            return ValueTask.FromResult<TOut>(default!);

        var inputObj = (object)item;

        // Try safe conversion for primitive/value types
        try
        {
            var targetType = typeof(TOut);

            // If target is nullable, get underlying type
            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            var converted = Convert.ChangeType(inputObj, underlyingType);

            return ValueTask.FromResult<TOut>((TOut)converted);
        }
        catch (InvalidCastException)
        {
            throw;
        }
        catch (Exception ex) when (ex is FormatException || ex is InvalidCastException || ex is OverflowException)
        {
            throw new InvalidCastException("Unable to cast item to target type.", ex);
        }
    }
}
