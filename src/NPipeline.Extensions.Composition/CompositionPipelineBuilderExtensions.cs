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
    /// <param name="serviceProvider">Optional service provider for resolving DI-managed child definitions.</param>
    /// <param name="fallbackToParameterlessWhenServiceMissing">
    ///     If true and <paramref name="serviceProvider" /> cannot resolve <typeparamref name="TDefinition" />,
    ///     attempts to create the definition using a parameterless constructor.
    /// </param>
    /// <returns>A handle to the composite node.</returns>
    public static TransformNodeHandle<TIn, TOut> AddComposite<TIn, TOut, TDefinition>(
        this PipelineBuilder builder,
        string? name = null,
        CompositeContextConfiguration? contextConfiguration = null,
        IServiceProvider? serviceProvider = null,
        bool fallbackToParameterlessWhenServiceMissing = false)
        where TDefinition : IPipelineDefinition
    {
        ArgumentNullException.ThrowIfNull(builder);

        var nodeName = name ?? typeof(TDefinition).Name;

        // Use Composite kind instead of plain Transform
        var handle = builder.AddTransformWithKind<CompositeTransformNode<TIn, TOut, TDefinition>, TIn, TOut>(
            NodeKind.Composite, nodeName);

        // Record the child definition type on the node
        builder.SetNodeChildDefinitionType(handle.Id, typeof(TDefinition));

        // Configure the node with shared pipeline runner and context configuration
        var node = new CompositeTransformNode<TIn, TOut, TDefinition>(
            SharedRunner.Value,
            contextConfiguration ?? CompositeContextConfiguration.Default,
            serviceProvider,
            fallbackToParameterlessWhenServiceMissing);

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
    /// <param name="serviceProvider">Optional service provider for resolving DI-managed child definitions.</param>
    /// <param name="fallbackToParameterlessWhenServiceMissing">
    ///     If true and <paramref name="serviceProvider" /> cannot resolve <typeparamref name="TDefinition" />,
    ///     attempts to create the definition using a parameterless constructor.
    /// </param>
    /// <returns>A handle to the composite node.</returns>
    public static TransformNodeHandle<TIn, TOut> AddComposite<TIn, TOut, TDefinition>(
        this PipelineBuilder builder,
        Action<CompositeContextConfiguration> configureContext,
        string? name = null,
        IServiceProvider? serviceProvider = null,
        bool fallbackToParameterlessWhenServiceMissing = false)
        where TDefinition : IPipelineDefinition
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureContext);

        var contextConfig = new CompositeContextConfiguration();
        configureContext(contextConfig);

        return builder.AddComposite<TIn, TOut, TDefinition>(name, contextConfig, serviceProvider, fallbackToParameterlessWhenServiceMissing);
    }

    /// <summary>
    ///     Adds a <see cref="PipelineInputSource{T}" /> node with <see cref="NodeKind.CompositeInput" /> kind,
    ///     identifying it as a bridge node that reads input from a parent composite node's context.
    /// </summary>
    /// <typeparam name="T">The data type received from the parent pipeline.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name. Defaults to "input".</param>
    /// <returns>A source node handle for graph connections.</returns>
    public static SourceNodeHandle<T> AddCompositeInput<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddSourceWithKind<PipelineInputSource<T>, T>(NodeKind.CompositeInput, name ?? "input");
    }

    /// <summary>
    ///     Adds a <see cref="PipelineOutputSink{T}" /> node with <see cref="NodeKind.CompositeOutput" /> kind,
    ///     identifying it as a bridge node that writes output back to a parent composite node's context.
    /// </summary>
    /// <typeparam name="T">The data type returned to the parent pipeline.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">Optional node name. Defaults to "output".</param>
    /// <returns>A sink node handle for graph connections.</returns>
    public static SinkNodeHandle<T> AddCompositeOutput<T>(
        this PipelineBuilder builder,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddSinkWithKind<PipelineOutputSink<T>, T>(NodeKind.CompositeOutput, name ?? "output");
    }
}
