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
    ///     Adds a string cleansing node to the pipeline for cleaning and normalizing string values.
    /// </summary>
    public static TransformNodeHandle<T, T> AddStringCleansing<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        return AddConfiguredNode<StringCleansingNode<T>, T, T>(builder, name, null);
    }

    /// <summary>
    ///     Adds a string cleansing node configured with the supplied delegate.
    /// </summary>
    public static TransformNodeHandle<T, T> AddStringCleansing<T>(
        this PipelineBuilder builder,
        Action<StringCleansingNode<T>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return AddConfiguredNode<StringCleansingNode<T>, T, T>(builder, name, configure);
    }

    /// <summary>
    ///     Adds a numeric cleansing node to the pipeline for cleaning and normalizing numeric values.
    /// </summary>
    public static TransformNodeHandle<T, T> AddNumericCleansing<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        return AddConfiguredNode<NumericCleansingNode<T>, T, T>(builder, name, null);
    }

    /// <summary>
    ///     Adds a numeric cleansing node configured with the supplied delegate.
    /// </summary>
    public static TransformNodeHandle<T, T> AddNumericCleansing<T>(
        this PipelineBuilder builder,
        Action<NumericCleansingNode<T>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return AddConfiguredNode<NumericCleansingNode<T>, T, T>(builder, name, configure);
    }

    /// <summary>
    ///     Adds a collection cleansing node to the pipeline for cleaning and normalizing collections.
    /// </summary>
    public static TransformNodeHandle<T, T> AddCollectionCleansing<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        return AddConfiguredNode<CollectionCleansingNode<T>, T, T>(builder, name, null);
    }

    /// <summary>
    ///     Adds a collection cleansing node configured with the supplied delegate.
    /// </summary>
    public static TransformNodeHandle<T, T> AddCollectionCleansing<T>(
        this PipelineBuilder builder,
        Action<CollectionCleansingNode<T>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return AddConfiguredNode<CollectionCleansingNode<T>, T, T>(builder, name, configure);
    }

    /// <summary>
    ///     Adds a datetime cleansing node to the pipeline for cleaning and normalizing date/time values.
    /// </summary>
    public static TransformNodeHandle<T, T> AddDateTimeCleansing<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        return AddConfiguredNode<DateTimeCleansingNode<T>, T, T>(builder, name, null);
    }

    /// <summary>
    ///     Adds a datetime cleansing node configured with the supplied delegate.
    /// </summary>
    public static TransformNodeHandle<T, T> AddDateTimeCleansing<T>(
        this PipelineBuilder builder,
        Action<DateTimeCleansingNode<T>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return AddConfiguredNode<DateTimeCleansingNode<T>, T, T>(builder, name, configure);
    }

    /// <summary>
    ///     Adds a string validation node to the pipeline and applies the default validation error handler.
    /// </summary>
    public static TransformNodeHandle<T, T> AddStringValidation<T>(
        this PipelineBuilder builder,
        Action<StringValidationNode<T>> configure,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var handle = AddConfiguredNode<StringValidationNode<T>, T, T>(builder, name, configure);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultValidationErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a numeric validation node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <param name="applyDefaultErrorHandler">When true, wires up the default validation error handler.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddNumericValidation<T>(
        this PipelineBuilder builder,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        var handle = AddConfiguredNode<NumericValidationNode<T>, T, T>(builder, name, null);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultValidationErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a numeric validation node with explicit configuration.
    /// </summary>
    public static TransformNodeHandle<T, T> AddNumericValidation<T>(
        this PipelineBuilder builder,
        Action<NumericValidationNode<T>> configure,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var handle = AddConfiguredNode<NumericValidationNode<T>, T, T>(builder, name, configure);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultValidationErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a datetime validation node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <param name="applyDefaultErrorHandler">When true, wires up the default validation error handler.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddDateTimeValidation<T>(
        this PipelineBuilder builder,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        var handle = AddConfiguredNode<DateTimeValidationNode<T>, T, T>(builder, name, null);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultValidationErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a datetime validation node with explicit configuration.
    /// </summary>
    public static TransformNodeHandle<T, T> AddDateTimeValidation<T>(
        this PipelineBuilder builder,
        Action<DateTimeValidationNode<T>> configure,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var handle = AddConfiguredNode<DateTimeValidationNode<T>, T, T>(builder, name, configure);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultValidationErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a generic validation node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <typeparam name="TValidationNode">The specific validation node type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <param name="configure">Optional configuration delegate.</param>
    /// <param name="applyDefaultErrorHandler">When true, wires up the default validation error handler.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddValidationNode<T, TValidationNode>(
        this PipelineBuilder builder,
        string? name = null,
        Action<TValidationNode>? configure = null,
        bool applyDefaultErrorHandler = true)
        where TValidationNode : ValidationNode<T>, new()
    {
        var handle = AddConfiguredNode<TValidationNode, T, T>(builder, name, configure);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultValidationErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a collection validation node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <param name="applyDefaultErrorHandler">When true, wires up the default validation error handler.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddCollectionValidation<T>(
        this PipelineBuilder builder,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        var handle = AddConfiguredNode<CollectionValidationNode<T>, T, T>(builder, name, null);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultValidationErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a collection validation node with explicit configuration.
    /// </summary>
    public static TransformNodeHandle<T, T> AddCollectionValidation<T>(
        this PipelineBuilder builder,
        Action<CollectionValidationNode<T>> configure,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var handle = AddConfiguredNode<CollectionValidationNode<T>, T, T>(builder, name, configure);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultValidationErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a filtering node to the pipeline.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <param name="applyDefaultErrorHandler">When true, wires up the default filtering error handler.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddFilteringNode<T>(
        this PipelineBuilder builder,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var nodeName = name ?? typeof(FilteringNode<T>).Name;
        var handle = builder.AddTransform<FilteringNode<T>, T, T>(nodeName);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultFilteringErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a filtering node with explicit configuration.
    /// </summary>
    public static TransformNodeHandle<T, T> AddFilteringNode<T>(
        this PipelineBuilder builder,
        Action<FilteringNode<T>> configure,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var nodeName = name ?? typeof(FilteringNode<T>).Name;
        var handle = builder.AddTransform<FilteringNode<T>, T, T>(nodeName);

        var node = new FilteringNode<T>();
        configure(node);
        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultFilteringErrorHandler<T>));

        return handle;
    }

    /// <summary>
    ///     Adds a type conversion node configured with the provided conversion delegate.
    /// </summary>
    public static TransformNodeHandle<TIn, TOut> AddTypeConversion<TIn, TOut>(
        this PipelineBuilder builder,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        return AddTypeConversionInternal<TIn, TOut>(builder, null, name, applyDefaultErrorHandler);
    }

    /// <summary>
    ///     Adds a type conversion node configured with the provided conversion delegate.
    /// </summary>
    public static TransformNodeHandle<TIn, TOut> AddTypeConversion<TIn, TOut>(
        this PipelineBuilder builder,
        Action<TypeConversionNode<TIn, TOut>> configure,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return AddTypeConversionInternal<TIn, TOut>(builder, configure, name, applyDefaultErrorHandler);
    }

    /// <summary>
    ///     Adds a type conversion node configured with the provided conversion delegate.
    /// </summary>
    public static TransformNodeHandle<TIn, TOut> AddTypeConversion<TIn, TOut>(
        this PipelineBuilder builder,
        Func<TIn, TOut> converter,
        string? name = null,
        bool applyDefaultErrorHandler = true)
    {
        ArgumentNullException.ThrowIfNull(converter);
        return builder.AddTypeConversion<TIn, TOut>(node => node.WithConverter(converter), name, applyDefaultErrorHandler);
    }

    /// <summary>
    ///     Adds an enrichment node to the pipeline for setting property values from lookups, computations, or defaults.
    /// </summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="configure">Configuration delegate for setting property values.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node for chaining.</returns>
    public static TransformNodeHandle<T, T> AddEnrichment<T>(
        this PipelineBuilder builder,
        Action<EnrichmentNode<T>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(configure);
        return AddConfiguredNode<EnrichmentNode<T>, T, T>(builder, name, configure);
    }

    private static TransformNodeHandle<TIn, TOut> AddTypeConversionInternal<TIn, TOut>(
        PipelineBuilder builder,
        Action<TypeConversionNode<TIn, TOut>>? configure,
        string? name,
        bool applyDefaultErrorHandler)
    {
        var handle = AddConfiguredNode<TypeConversionNode<TIn, TOut>, TIn, TOut>(builder, name, configure);

        if (applyDefaultErrorHandler)
            builder.WithErrorHandler(handle, typeof(DefaultTypeConversionErrorHandler<TIn, TOut>));

        return handle;
    }

    private static TransformNodeHandle<TIn, TOut> AddConfiguredNode<TNode, TIn, TOut>(
        PipelineBuilder builder,
        string? name,
        Action<TNode>? configure)
        where TNode : class, ITransformNode<TIn, TOut>, new()
    {
        ArgumentNullException.ThrowIfNull(builder);

        var nodeName = name ?? typeof(TNode).Name;
        var handle = builder.AddTransform<TNode, TIn, TOut>(nodeName);

        if (configure != null)
        {
            var node = new TNode();
            configure(node);
            builder.AddPreconfiguredNodeInstance(handle.Id, node);
        }

        return handle;
    }
}
