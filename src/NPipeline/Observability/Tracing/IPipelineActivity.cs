namespace NPipeline.Observability.Tracing;

/// <summary>
///     Represents a single operation within a distributed trace.
/// </summary>
/// <remarks>
///     <para>
///         Pipeline activities represent distinct phases of execution (e.g., "ExecuteSource", "TransformBatch").
///         Each activity can record timing, tags, and exceptions for detailed observability.
///     </para>
///     <para>
///         Activities support hierarchical nesting—activities can span child activities, creating
///         a tree structure that represents the execution flow.
///     </para>
///     <para>
///         Implement <see cref="IDisposable" /> to mark the end of the activity when disposed.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Using activities for distributed tracing
/// public class CustomTracer : IPipelineTracer
/// {
///     public IPipelineActivity StartActivity(string name)
///     {
///         var activity = System.Diagnostics.Activity.StartActivity(name);
///         return new ActivityAdapter(activity);
///     }
/// }
/// 
/// // Activity implementation
/// public class ActivityAdapter : IPipelineActivity
/// {
///     private readonly System.Diagnostics.Activity _activity;
/// 
///     public ActivityAdapter(System.Diagnostics.Activity activity) => _activity = activity;
/// 
///     public void SetTag(string key, object value)
///     {
///         _activity?.SetTag(key, value);
///     }
/// 
///     public void RecordException(Exception exception)
///     {
///         _activity?.AddEvent(new System.Diagnostics.ActivityEvent(
///             "exception",
///             new System.Collections.Generic.Dictionary&lt;string, object?&gt;
///             {
///                 { "exception.type", exception.GetType().Name },
///                 { "exception.message", exception.Message }
///             }));
///     }
/// 
///     public void Dispose()
///     {
///         _activity?.Dispose();
///     }
/// }
/// </code>
/// </example>
public interface IPipelineActivity : IDisposable
{
    /// <summary>
    ///     Sets a tag on the activity.
    /// </summary>
    /// <param name="key">The key of the tag (e.g., "node_id", "item_count", "duration_ms").</param>
    /// <param name="value">The value of the tag.</param>
    /// <remarks>
    ///     Tags are arbitrary key-value pairs that provide context for the activity.
    ///     Use them to record node IDs, batch sizes, performance metrics, or other relevant data.
    /// </remarks>
    void SetTag(string key, object value);

    /// <summary>
    ///     Records an exception against the activity.
    /// </summary>
    /// <param name="exception">The exception to record.</param>
    /// <remarks>
    ///     This is typically called from error handlers to associate failures with the current activity,
    ///     enabling root-cause analysis in distributed tracing systems.
    /// </remarks>
    void RecordException(Exception exception);
}
