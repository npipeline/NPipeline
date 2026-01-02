using NPipeline.Extensions.Nodes.Core;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes;

/// <summary>
///     Pipeline builder extension methods for registering nodes from NPipeline.Extensions.Nodes.
/// </summary>
public static class PipelineBuilderExtensions
{
    /// <summary>
    ///     Adds a numeric cleansing node to the pipeline for cleaning and normalizing numeric values.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddNumericCleansing<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(NumericCleansingNode<T>).Name;
        return builder.AddTransform<NumericCleansingNode<T>, T, T>(nodeName);
    }

    /// <summary>
    ///     Adds a datetime cleansing node to the pipeline for cleaning and normalizing date/time values.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddDateTimeCleansing<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(DateTimeCleansingNode<T>).Name;
        return builder.AddTransform<DateTimeCleansingNode<T>, T, T>(nodeName);
    }

    /// <summary>
    ///     Adds a numeric validation node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddNumericValidation<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(NumericValidationNode<T>).Name;
        return builder.AddTransform<NumericValidationNode<T>, T, T>(nodeName);
    }

    /// <summary>
    ///     Adds a datetime validation node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddDateTimeValidation<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(DateTimeValidationNode<T>).Name;
        return builder.AddTransform<DateTimeValidationNode<T>, T, T>(nodeName);
    }

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
        where TValidationNode : ValidationNode<T>, new()
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
        var nodeName = name ?? typeof(FilteringNode<T>).Name;
        return builder.AddTransform<FilteringNode<T>, T, T>(nodeName);
    }

    /// <summary>
    ///     Adds a type conversion node to the pipeline.
    /// </summary>
    /// <typeparam name="TIn">The input type.</typeparam>
    /// <typeparam name="TOut">The output type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<TIn, TOut> AddTypeConversion<TIn, TOut>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(TypeConversionNode<TIn, TOut>).Name;
        return builder.AddTransform<TypeConversionNode<TIn, TOut>, TIn, TOut>(nodeName);
    }

    /// <summary>
    ///     Adds an enrichment node to the pipeline for setting property values from lookups, computations, or defaults.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddEnrichment<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(EnrichmentNode<T>).Name;
        return builder.AddTransform<EnrichmentNode<T>, T, T>(nodeName);
    }
}
