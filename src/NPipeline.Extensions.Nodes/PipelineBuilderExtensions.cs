using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes;

/// <summary>
///     Pipeline builder extension methods for registering nodes from NPipeline.Extensions.Nodes.
/// </summary>
public static class PipelineBuilderExtensions
{
    /// <summary>
    ///     Adds a validation node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <typeparam name="TValidationNode">The specific validation node type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddValidationNode<T, TValidationNode>(
        this PipelineBuilder builder,
        string? name = null)
        where TValidationNode : Core.ValidationNode<T>, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(TValidationNode).Name;
        return builder.AddTransform<TValidationNode, T, T>(nodeName);
    }

    /// <summary>
    ///     Adds a filtering node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddFilteringNode<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(Core.FilteringNode<T>).Name;
        return builder.AddTransform<Core.FilteringNode<T>, T, T>(nodeName);
    }
}

