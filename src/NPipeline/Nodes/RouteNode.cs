using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A pass-through node used to mark conditional routing points in the graph.
/// </summary>
/// <typeparam name="T">The item type.</typeparam>
public sealed class RouteNode<T> : TransformNode<T, T>
{
    /// <inheritdoc />
    public override ValueTask<T> TransformAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        return ValueTask.FromResult<T>(item);
    }
}
