namespace NPipeline.Observability.Tracing;

/// <summary>
///     Provides tracing capabilities for pipeline execution to track activity flow and timing.
/// </summary>
/// <remarks>
///     <para>
///         The pipeline tracer enables distributed tracing of pipeline execution. It tracks
///         pipeline activities (phases of execution) and can integrate with observability
///         platforms like Application Insights, Jaeger, or OpenTelemetry.
///     </para>
///     <para>
///         For testing or scenarios without external tracing, use <see cref="NullPipelineTracer.Instance" />,
///         which provides a no-op implementation.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Use a custom tracer that logs to console
/// public class ConsoleTracer : IPipelineTracer
/// {
///     public IPipelineActivity? CurrentActivity { get; set; }
/// 
///     public IPipelineActivity StartActivity(string name)
///     {
///         Console.WriteLine($"→ Starting: {name}");
///         var activity = new ConsoleActivity(name);
///         CurrentActivity = activity;
///         return activity;
///     }
/// }
/// 
/// // Use in pipeline context
/// var context = new PipelineContext(
///     PipelineContextConfiguration.WithObservability(tracer: new ConsoleTracer()));
/// </code>
/// </example>
public interface IPipelineTracer
{
    /// <summary>
    ///     Gets the current pipeline activity, if any.
    /// </summary>
    /// <remarks>
    ///     Returns null if no activity is currently active. Use this to add context
    ///     to other observability signals (logging, metrics).
    /// </remarks>
    IPipelineActivity? CurrentActivity { get; }

    /// <summary>
    ///     Starts a new pipeline activity.
    /// </summary>
    /// <param name="name">The name of the activity (e.g., "ExecuteSource", "TransformBatch").</param>
    /// <returns>A new <see cref="IPipelineActivity" /> instance that tracks this activity's execution.</returns>
    /// <remarks>
    ///     Activities are hierarchical. Starting a new activity while one is active creates
    ///     a child activity. When the activity is disposed, the parent becomes the current activity again.
    /// </remarks>
    IPipelineActivity StartActivity(string name);
}
