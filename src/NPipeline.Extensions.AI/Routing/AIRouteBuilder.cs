using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Routing;

/// <summary>
///     Fluent builder for configuring route branches on an AI-driven route node.
/// </summary>
/// <typeparam name="T">The item type flowing through the route.</typeparam>
public sealed class AIRouteBuilder<T> : IInputNodeHandle<T>
{
    private readonly PipelineBuilder _builder;
    private readonly TransformNodeHandle<T, T> _enrichHandle;

    /// <summary>
    ///     Initializes a new instance.
    /// </summary>
    /// <param name="builder">The pipeline builder used for registration.</param>
    /// <param name="enrichHandle">The handle of the enrichment node (input side).</param>
    /// <param name="routeHandle">The handle of the underlying route node (output side).</param>
    public AIRouteBuilder(PipelineBuilder builder, TransformNodeHandle<T, T> enrichHandle, TransformNodeHandle<T, T> routeHandle)
    {
        _builder = builder ?? throw new ArgumentNullException(nameof(builder));
        _enrichHandle = enrichHandle ?? throw new ArgumentNullException(nameof(enrichHandle));
        RouteHandle = routeHandle ?? throw new ArgumentNullException(nameof(routeHandle));
    }

    string INodeHandle.Id => _enrichHandle.Id;

    /// <summary>
    ///     The handle of the underlying route node. Use this as the source for manual connections.
    /// </summary>
    public TransformNodeHandle<T, T> RouteHandle { get; }

    /// <summary>
    ///     Routes items matching the predicate to the target node.
    /// </summary>
    /// <param name="predicate">Condition that determines whether an item is routed to this output.</param>
    /// <param name="target">The target node handle.</param>
    public AIRouteBuilder<T> When(Func<T, bool> predicate, IInputNodeHandle<T> target)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(target);

        _builder.ConnectWhen(RouteHandle, target, predicate);
        return this;
    }

    /// <summary>
    ///     Routes unmatched items to the target node.
    /// </summary>
    /// <param name="target">The target node handle.</param>
    public AIRouteBuilder<T> Otherwise(IInputNodeHandle<T> target)
    {
        ArgumentNullException.ThrowIfNull(target);

        _builder.ConnectOtherwise(RouteHandle, target);
        return this;
    }
}
