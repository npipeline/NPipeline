using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Composition;

/// <summary>
///     Extension methods for adding composite nodes to pipelines.
/// </summary>
public static class CompositionPipelineBuilderExtensions
{
    // Shared runner instance for all composite nodes (thread-safe)
    private static readonly Lazy<IPipelineRunner> SharedRunner = new(() => PipelineRunner.Create());

    /// <summary>
    ///     Adds a composite node that executes the specified sub-pipeline.
    /// </summary>
    /// <typeparam name="TIn">The input type to the composite node.</typeparam>
    /// <typeparam name="TOut">The output type from the composite node.</typeparam>
    /// <typeparam name="TDefinition">The sub-pipeline definition type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name.</param>
    /// <param name="contextConfiguration">Optional context configuration.</param>
    /// <returns>A handle to the composite node.</returns>
    public static TransformNodeHandle<TIn, TOut> AddComposite<TIn, TOut, TDefinition>(
        this PipelineBuilder builder,
        string? name = null,
        CompositeContextConfiguration? contextConfiguration = null)
        where TDefinition : IPipelineDefinition, new()
    {
        ArgumentNullException.ThrowIfNull(builder);

        var nodeName = name ?? typeof(TDefinition).Name;
        var handle = builder.AddTransform<CompositeTransformNode<TIn, TOut, TDefinition>, TIn, TOut>(nodeName);

        // Configure the node with shared pipeline runner and context configuration
        var node = new CompositeTransformNode<TIn, TOut, TDefinition>(
            SharedRunner.Value,
            contextConfiguration ?? CompositeContextConfiguration.Default);

        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }

    /// <summary>
    ///     Adds a composite node with a custom context configuration builder.
    /// </summary>
    /// <typeparam name="TIn">The input type to the composite node.</typeparam>
    /// <typeparam name="TOut">The output type from the composite node.</typeparam>
    /// <typeparam name="TDefinition">The sub-pipeline definition type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="configureContext">Action to configure the sub-pipeline context.</param>
    /// <param name="name">Optional node name.</param>
    /// <returns>A handle to the composite node.</returns>
    public static TransformNodeHandle<TIn, TOut> AddComposite<TIn, TOut, TDefinition>(
        this PipelineBuilder builder,
        Action<CompositeContextConfiguration> configureContext,
        string? name = null)
        where TDefinition : IPipelineDefinition, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureContext);

        var contextConfig = new CompositeContextConfiguration();
        configureContext(contextConfig);

        return builder.AddComposite<TIn, TOut, TDefinition>(name, contextConfig);
    }
}
