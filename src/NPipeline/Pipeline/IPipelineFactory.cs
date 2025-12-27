namespace NPipeline.Pipeline;

/// <summary>
///     Defines the contract for a factory that creates pipeline instances from definitions.
/// </summary>
/// <remarks>
///     <para>
///         The pipeline factory is responsible for transforming a pipeline definition into an executable
///         pipeline object. It handles:
///         - Instantiating the pipeline definition
///         - Building the node graph
///         - Configuring execution strategies
///         - Preparing observability services
///     </para>
///     <para>
///         Typically, you'll use the default <see cref="PipelineFactory" /> implementation.
///         Only implement this interface for advanced scenarios requiring custom pipeline construction logic.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Using the default factory
/// var factory = new PipelineFactory();
/// var context = PipelineContext.Default;
/// var pipeline = factory.Create&lt;MyDataProcessingPipeline&gt;(context);
/// await pipeline.ExecuteAsync();
/// 
/// // Or use the IPipelineRunner directly, which uses a factory internally
/// var runner = PipelineRunner.Create();
/// await runner.RunAsync&lt;MyDataProcessingPipeline&gt;(context);
/// </code>
/// </example>
public interface IPipelineFactory
{
    /// <summary>
    ///     Creates a pipeline using the specified definition.
    /// </summary>
    /// <typeparam name="TDefinition">The type of the pipeline definition to create a pipeline from.</typeparam>
    /// <param name="context">The <see cref="PipelineContext" /> for this pipeline execution.</param>
    /// <returns>The created executable pipeline.</returns>
    /// <remarks>
    ///     This method instantiates the pipeline definition, builds its node graph according to the
    ///     definition's <see cref="IPipelineDefinition.Define" /> method, and prepares all services
    ///     and configurations for execution.
    /// </remarks>
    Pipeline Create<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new();
}
