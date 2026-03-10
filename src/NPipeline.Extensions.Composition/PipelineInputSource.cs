using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Composition;

/// <summary>
///     A source node that retrieves input from the parent pipeline context.
/// </summary>
/// <typeparam name="T">The type of input item.</typeparam>
public sealed class PipelineInputSource<T> : ISourceNode<T>
{
    /// <inheritdoc />
    public IDataStream<T> OpenStream(
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Retrieve input item from context
        if (!context.Parameters.TryGetValue(CompositeContextKeys.InputItem, out var item))
        {
            throw new InvalidOperationException(
                "No input item found in pipeline context. " +
                "CompositeTransformNode should have stored the input item.");
        }

        if (item is T typedItem)
        {
            // Return single item as a data pipe
            return new InMemoryDataStream<T>([typedItem], "CompositeInput");
        }

        if (item is null)
        {
            if (IsNullableType())
                return new InMemoryDataStream<T>([default!], "CompositeInput");

            throw new InvalidCastException(
                $"Input item is null. Expected {typeof(T)}.");
        }

        throw new InvalidCastException(
            $"Input item type mismatch. Expected {typeof(T)}, got {item.GetType()}.");
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        // No resources to dispose
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private static bool IsNullableType()
    {
        var type = typeof(T);
        return !type.IsValueType || Nullable.GetUnderlyingType(type) is not null;
    }
}
