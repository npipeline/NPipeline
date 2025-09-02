using NPipeline.Execution.Strategies;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A marker node that indicates a stream of batches should be flattened into a stream of individual items.
/// </summary>
/// <typeparam name="T">The type of item in the batches.</typeparam>
public sealed class UnbatchingNode<T> : TransformNode<IEnumerable<T>, T>
{
    /// <summary>
    ///     This method should not be called directly. The unbatching logic is handled by the <see cref="UnbatchingExecutionStrategy" />.
    /// </summary>
    public override Task<T> ExecuteAsync(IEnumerable<T> item, PipelineContext context, CancellationToken cancellationToken)
    {
        // This node is a marker and doesn't perform a transformation itself.
        // The actual unbatching is handled by the UnbatchingExecutionStrategy.
        throw new NotSupportedException("UnbatchingNode should not be executed directly.");
    }
}
