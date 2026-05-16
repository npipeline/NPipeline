using NPipeline;
using NPipeline.DataFlow.Routing;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Extension methods for creating and configuring conditional route nodes.
/// </summary>
public static class PipelineBuilderRoutingExtensions
{
    /// <summary>
    ///     Adds a route node to the pipeline.
    /// </summary>
    public static TransformNodeHandle<T, T> AddRoute<T>(this PipelineBuilder builder, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var handle = builder.AddTransformWithKind<RouteNode<T>, T, T>(NodeKind.Route, name ?? "Route");
        var node = new RouteNode<T>();
        _ = builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    /// <summary>
    ///     Adds a route node and applies route option configuration.
    /// </summary>
    public static TransformNodeHandle<T, T> AddRoute<T>(this PipelineBuilder builder, Action<RouteOptions<T>> configure, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var handle = builder.AddRoute<T>(name);
        builder.ConfigureRoute(handle, configure);
        return handle;
    }

    /// <summary>
    ///     Applies route option configuration to an existing route node.
    /// </summary>
    public static PipelineBuilder ConfigureRoute<T>(
        this PipelineBuilder builder,
        TransformNodeHandle<T, T> route,
        Action<RouteOptions<T>> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(route);
        ArgumentNullException.ThrowIfNull(configure);

        EnsureRouteNode(builder, route.Id, nameof(ConfigureRoute));
        var options = GetOrCreateRouteOptions<T>(builder, route.Id);
        configure(options);
        return builder;
    }

    /// <summary>
    ///     Connects a route output to a target and binds a condition to that output.
    /// </summary>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="route">The route node handle.</param>
    /// <param name="target">The target input node handle.</param>
    /// <param name="predicate">Condition that determines whether an item is routed to this output.</param>
    /// <param name="sourceOutputName">Optional route output name. Defaults to the target node ID.</param>
    /// <param name="targetInputName">Optional target input name for nodes with named inputs.</param>
    public static PipelineBuilder ConnectWhen<T>(
        this PipelineBuilder builder,
        TransformNodeHandle<T, T> route,
        IInputNodeHandle<T> target,
        Func<T, bool> predicate,
        string? sourceOutputName = null,
        string? targetInputName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(route);
        ArgumentNullException.ThrowIfNull(predicate);
        ArgumentNullException.ThrowIfNull(target);

        EnsureRouteNode(builder, route.Id, nameof(ConnectWhen));

        var outputName = sourceOutputName ?? target.Id;
        var options = GetOrCreateRouteOptions<T>(builder, route.Id);
        options.When(outputName, predicate);

        return builder.Connect(route, target, outputName, targetInputName);
    }

    /// <summary>
    ///     Connects the otherwise route output to a target for unmatched items.
    /// </summary>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="route">The route node handle.</param>
    /// <param name="target">The target input node handle.</param>
    /// <param name="sourceOutputName">Optional otherwise output name. Defaults to <see cref="RouteOutputNames.Otherwise" />.</param>
    /// <param name="targetInputName">Optional target input name for nodes with named inputs.</param>
    public static PipelineBuilder ConnectOtherwise<T>(
        this PipelineBuilder builder,
        TransformNodeHandle<T, T> route,
        IInputNodeHandle<T> target,
        string? sourceOutputName = null,
        string? targetInputName = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(route);
        ArgumentNullException.ThrowIfNull(target);

        EnsureRouteNode(builder, route.Id, nameof(ConnectOtherwise));

        var outputName = sourceOutputName ?? RouteOutputNames.Otherwise;
        var options = GetOrCreateRouteOptions<T>(builder, route.Id);
        options.Otherwise(outputName);

        return builder.Connect(route, target, outputName, targetInputName);
    }

    private static RouteOptions<T> GetOrCreateRouteOptions<T>(PipelineBuilder builder, string nodeId)
    {
        var key = ExecutionAnnotationKeys.RouteOptionsForNode(nodeId);

        if (builder.NodeState.ExecutionAnnotations.TryGetValue(key, out var existing))
        {
            if (existing is RouteOptions<T> typedOptions)
                return typedOptions;

            throw new InvalidOperationException(
                $"Route options for node '{nodeId}' were configured with type '{existing.GetType().Name}', expected '{typeof(RouteOptions<T>).Name}'.");
        }

        var options = new RouteOptions<T>();
        builder.SetNodeExecutionOption(key, options);
        return options;
    }

    private static void EnsureRouteNode(PipelineBuilder builder, string nodeId, string operation)
    {
        if (!builder.NodeState.Nodes.TryGetValue(nodeId, out var nodeDef))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(nodeId, operation));

        if (nodeDef.Kind != NodeKind.Route)
        {
            throw new InvalidOperationException(
                $"Node '{nodeDef.Name}' ({nodeDef.Id}) is '{nodeDef.Kind}', but '{operation}' requires a '{NodeKind.Route}' node.");
        }
    }
}
