using System.Diagnostics;
using NPipeline.Observability.Tracing;

namespace NPipeline.Extensions.Observability.Tracing
{
    /// <summary>
    ///     An implementation of <see cref="IPipelineActivity" /> that wraps a <see cref="Activity" /> instance.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         This class provides a bridge between NPipeline's activity abstraction and System.Diagnostics.Activity,
    ///         enabling OpenTelemetry-compatible distributed tracing for pipelines.
    ///     </para>
    ///     <para>
    ///         When a <see cref="PipelineActivity" /> is disposed, the underlying <see cref="Activity" /> is also disposed,
    ///         which marks the end of the activity in the distributed trace.
    ///     </para>
    ///     <para>
    ///         Tags and exceptions are propagated to the underlying activity for collection by tracing instrumentation.
    ///     </para>
    /// </remarks>
    /// <example>
    ///     <code>
    /// // Create a custom tracer that uses PipelineActivity
    /// public class SystemDiagnosticsTracer : IPipelineTracer
    /// {
    ///     public IPipelineActivity StartActivity(string name)
    ///     {
    ///         var activity = System.Diagnostics.Activity.StartActivity(name);
    ///         return activity != null 
    ///             ? new PipelineActivity(activity)
    ///             : new NullPipelineActivity();
    ///     }
    /// }
    /// 
    /// // Use it in your pipeline context
    /// var context = new PipelineContext(
    ///     PipelineContextConfiguration.WithObservability(tracer: new SystemDiagnosticsTracer())
    /// );
    /// </code>
    /// </example>
    public sealed class PipelineActivity : IPipelineActivity
    {
        private readonly Activity _activity;

        /// <summary>
        ///     Creates a new instance of <see cref="PipelineActivity" /> wrapping the provided activity.
        /// </summary>
        /// <param name="activity">The underlying activity from System.Diagnostics.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="activity" /> is null.</exception>
#pragma warning disable IDE0290 // Use primary constructor (suppressed to allow null validation)
        public PipelineActivity(Activity activity)
#pragma warning restore IDE0290
        {
            _activity = activity ?? throw new ArgumentNullException(nameof(activity));
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _activity.Dispose();
        }

        /// <inheritdoc />
        public void SetTag(string key, object value)
        {
            _ = _activity.SetTag(key, value);
        }

        /// <inheritdoc />
        public void RecordException(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);

            // Set activity status to error
            _ = _activity.SetStatus(ActivityStatusCode.Error, exception.Message);

            // Add exception event with detailed information
            _ = _activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
            {
                { "exception.type", exception.GetType().FullName },
                { "exception.message", exception.Message },
                { "exception.stacktrace", exception.ToString() },
            }));
        }
    }
}
