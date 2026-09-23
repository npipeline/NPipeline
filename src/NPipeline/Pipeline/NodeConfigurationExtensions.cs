using NPipeline.Execution;
using NPipeline.Graph;

namespace NPipeline.Pipeline;

/// <summary>
///     Fluent configuration extension methods for pipeline nodes.
///     These extensions provide a convenient way to configure nodes immediately after adding them to the pipeline builder.
/// </summary>
/// <remarks>
///     Usage pattern:
///     <code>
/// var builder = new PipelineBuilder();
/// var transform = builder.AddTransform&lt;MyTransform, int, string&gt;("transform")
///     .WithExecutionStrategy(builder, new SequentialExecutionStrategy());
/// </code>
///     Note: These extension methods must be used within the context of the builder's fluent API.
///     The builder instance should be available in the scope where these are called.
/// </remarks>
public static class NodeConfigurationExtensions
{
    /// <summary>
    ///     Configures the execution strategy for a transform node.
    /// </summary>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="strategy">The execution strategy to apply.</param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder or strategy is null.</exception>
    public static TransformNodeHandle<TIn, TOut> WithExecutionStrategy<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        IExecutionStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(strategy);

        builder.WithExecutionStrategy(handle, strategy);
        return handle;
    }
}
