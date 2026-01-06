using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Composition;

/// <summary>
///     A sink node that captures output to the parent pipeline context.
/// </summary>
/// <typeparam name="T">The type of output item.</typeparam>
public sealed class PipelineOutputSink<T> : ISinkNode<T>
{
    /// <inheritdoc />
    public async Task ExecuteAsync(
        IDataPipe<T> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Consume the single output item
        T? outputItem = default;
        var itemReceived = false;

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            outputItem = item;
            itemReceived = true;
            break; // Only consume first item
        }

        if (!itemReceived)
        {
            throw new InvalidOperationException(
                "PipelineOutputSink did not receive any output item. "
                + "Ensure the sub-pipeline produces at least one output.");
        }

        // Store output in context for retrieval by parent
        // Note: outputItem may be null for nullable types, which is valid
        context.Parameters[CompositeContextKeys.OutputItem] = (object?)outputItem ?? DBNull.Value;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        // No resources to dispose
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
