using NPipeline.Graph;
using NPipeline.Observability.Configuration;
using NPipeline.Pipeline;

namespace NPipeline.Observability;

/// <summary>
///     Extension methods for configuring observability on pipeline nodes.
/// </summary>
/// <remarks>
///     <para>
///         These extension methods allow you to enable automatic metrics collection
///         on individual nodes in your pipeline using a fluent API.
///     </para>
///     <para>
///         When observability is enabled on a node, the framework automatically records
///         timing, item counts, and performance metrics without requiring any instrumentation
///         code in your node implementation.
///     </para>
/// </remarks>
/// <example>
///     <code>
///     public void Define(PipelineBuilder builder, PipelineContext context)
///     {
///         var source = builder.AddSource&lt;MySource, int&gt;();
///         
///         var transform = builder.AddTransform&lt;MyTransform, int, string&gt;()
///             .WithObservability(builder); // Enable with default options
///         
///         var sink = builder.AddSink&lt;MySink, string&gt;()
///             .WithObservability(builder, ObservabilityOptions.Full); // Full metrics
///         
///         builder.Connect(source, transform);
///         builder.Connect(transform, sink);
///     }
///     </code>
/// </example>
public static class ObservabilityConfigurationExtensions
{
    /// <summary>
    ///     The key used to store observability options in node execution annotations.
    /// </summary>
    public const string ObservabilityOptionsKey = "NPipeline.Observability.Options";

    /// <summary>
    ///     Configures observability metrics collection for a source node.
    /// </summary>
    /// <typeparam name="TOut">The output type of the source node.</typeparam>
    /// <param name="handle">The source node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="options">
    ///     The observability options. If null, <see cref="ObservabilityOptions.Default"/> is used.
    /// </param>
    /// <returns>The same source node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    public static SourceNodeHandle<TOut> WithObservability<TOut>(
        this SourceNodeHandle<TOut> handle,
        PipelineBuilder builder,
        ObservabilityOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var effectiveOptions = options ?? ObservabilityOptions.Default;
        _ = builder.SetNodeExecutionOption($"{ObservabilityOptionsKey}:{handle.Id}", effectiveOptions);
        return handle;
    }

    /// <summary>
    ///     Configures observability metrics collection for a transform node.
    /// </summary>
    /// <typeparam name="TIn">The input type of the transform node.</typeparam>
    /// <typeparam name="TOut">The output type of the transform node.</typeparam>
    /// <param name="handle">The transform node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="options">
    ///     The observability options. If null, <see cref="ObservabilityOptions.Default"/> is used.
    /// </param>
    /// <returns>The same transform node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    public static TransformNodeHandle<TIn, TOut> WithObservability<TIn, TOut>(
        this TransformNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        ObservabilityOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var effectiveOptions = options ?? ObservabilityOptions.Default;
        _ = builder.SetNodeExecutionOption($"{ObservabilityOptionsKey}:{handle.Id}", effectiveOptions);
        return handle;
    }

    /// <summary>
    ///     Configures observability metrics collection for a sink node.
    /// </summary>
    /// <typeparam name="TIn">The input type of the sink node.</typeparam>
    /// <param name="handle">The sink node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="options">
    ///     The observability options. If null, <see cref="ObservabilityOptions.Default"/> is used.
    /// </param>
    /// <returns>The same sink node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    public static SinkNodeHandle<TIn> WithObservability<TIn>(
        this SinkNodeHandle<TIn> handle,
        PipelineBuilder builder,
        ObservabilityOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var effectiveOptions = options ?? ObservabilityOptions.Default;
        _ = builder.SetNodeExecutionOption($"{ObservabilityOptionsKey}:{handle.Id}", effectiveOptions);
        return handle;
    }

    /// <summary>
    ///     Configures observability metrics collection for an aggregate node.
    /// </summary>
    /// <typeparam name="TIn">The input type of the aggregate node.</typeparam>
    /// <typeparam name="TOut">The output type of the aggregate node.</typeparam>
    /// <param name="handle">The aggregate node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="options">
    ///     The observability options. If null, <see cref="ObservabilityOptions.Default"/> is used.
    /// </param>
    /// <returns>The same aggregate node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    public static AggregateNodeHandle<TIn, TOut> WithObservability<TIn, TOut>(
        this AggregateNodeHandle<TIn, TOut> handle,
        PipelineBuilder builder,
        ObservabilityOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var effectiveOptions = options ?? ObservabilityOptions.Default;
        _ = builder.SetNodeExecutionOption($"{ObservabilityOptionsKey}:{handle.Id}", effectiveOptions);
        return handle;
    }

    /// <summary>
    ///     Configures observability metrics collection for a join node.
    /// </summary>
    /// <typeparam name="TIn1">The first input type of the join node.</typeparam>
    /// <typeparam name="TIn2">The second input type of the join node.</typeparam>
    /// <typeparam name="TOut">The output type of the join node.</typeparam>
    /// <param name="handle">The join node handle.</param>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="options">
    ///     The observability options. If null, <see cref="ObservabilityOptions.Default"/> is used.
    /// </param>
    /// <returns>The same join node handle for method chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when builder is null.</exception>
    public static JoinNodeHandle<TIn1, TIn2, TOut> WithObservability<TIn1, TIn2, TOut>(
        this JoinNodeHandle<TIn1, TIn2, TOut> handle,
        PipelineBuilder builder,
        ObservabilityOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var effectiveOptions = options ?? ObservabilityOptions.Default;
        _ = builder.SetNodeExecutionOption($"{ObservabilityOptionsKey}:{handle.Id}", effectiveOptions);
        return handle;
    }

    /// <summary>
    ///     Tries to get the observability options for a node from the execution annotations.
    /// </summary>
    /// <param name="executionAnnotations">The execution annotations dictionary.</param>
    /// <param name="nodeId">The node ID to look up.</param>
    /// <param name="options">The observability options if found.</param>
    /// <returns>True if observability options were found; otherwise, false.</returns>
    public static bool TryGetObservabilityOptions(
        this IReadOnlyDictionary<string, object> executionAnnotations,
        string nodeId,
        out ObservabilityOptions? options)
    {
        var key = $"{ObservabilityOptionsKey}:{nodeId}";
        if (executionAnnotations.TryGetValue(key, out var value) && value is ObservabilityOptions obs)
        {
            options = obs;
            return true;
        }

        options = null;
        return false;
    }
}
