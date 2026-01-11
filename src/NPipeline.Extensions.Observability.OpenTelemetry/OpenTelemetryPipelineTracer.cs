using System.Diagnostics;
using NPipeline.Extensions.Observability.Tracing;
using NPipeline.Observability.Tracing;

namespace NPipeline.Extensions.Observability.OpenTelemetry;

/// <summary>
///     An implementation of <see cref="IPipelineTracer" /> that integrates with OpenTelemetry via
///     <see cref="ActivitySource" /> and <see cref="Activity" />.
/// </summary>
/// <remarks>
///     <para>
///         This tracer creates <see cref="Activity" /> instances from an <see cref="ActivitySource" />
///         whose name matches the configured service name. This is the recommended pattern for
///         OpenTelemetry .NET integration and ensures activities are captured by providers
///         configured with <c>AddSource(serviceName)</c>.
///     </para>
///     <para>
///         Activities created by this tracer can be exported to various backends including
///         Jaeger, Zipkin, Azure Monitor, AWS X-Ray, and others via OpenTelemetry SDKs.
///     </para>
///     <para>
///         The tracer respects the current <see cref="Activity.Current" /> context, automatically
///         establishing parent-child relationships for hierarchical tracing.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Register with dependency injection
/// var services = new ServiceCollection();
/// services.AddNPipelineObservability();
/// services.AddOpenTelemetryPipelineTracer("MyPipeline");
/// 
/// // Configure OpenTelemetry export (example with Jaeger)
/// using var tracerProvider = new TracerProviderBuilder()
///     .AddSource("MyPipeline")
///     .AddJaegerExporter()
///     .Build();
/// 
/// var provider = services.BuildServiceProvider();
/// 
/// // Use in pipeline context
/// var context = new PipelineContext(
///     PipelineContextConfiguration.WithObservability(
///         tracer: provider.GetRequiredService&lt;IPipelineTracer&gt;()
///     )
/// );
/// </code>
/// </example>
public sealed class OpenTelemetryPipelineTracer : IPipelineTracer, IDisposable
{
    private readonly ActivitySource _activitySource;
    private readonly string _serviceName;

    /// <summary>
    ///     Creates a new instance of <see cref="OpenTelemetryPipelineTracer" />.
    /// </summary>
    /// <param name="serviceName">
    ///     The service/application name to use for activities. Used to prefix activity names
    ///     for better identification in trace visualizers.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="serviceName" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="serviceName" /> is empty or whitespace.</exception>
    public OpenTelemetryPipelineTracer(string serviceName)
    {
        ArgumentNullException.ThrowIfNull(serviceName);

        if (string.IsNullOrWhiteSpace(serviceName))
            throw new ArgumentException("Service name cannot be empty or whitespace.", nameof(serviceName));

        _serviceName = serviceName;
        _activitySource = new ActivitySource(_serviceName);
    }

    /// <summary>
    ///     Disposes the <see cref="ActivitySource" /> used by this tracer.
    /// </summary>
    public void Dispose()
    {
        _activitySource?.Dispose();
    }

    /// <inheritdoc />
    public IPipelineActivity? CurrentActivity { get; private set; }

    /// <inheritdoc />
    public IPipelineActivity StartActivity(string name)
    {
        ArgumentNullException.ThrowIfNull(name);

        // Create activity from the ActivitySource so OpenTelemetry providers configured
        // with AddSource(_serviceName) can correctly capture these activities. When
        // there are no listeners or sampling drops the activity, StartActivity returns
        // null, in which case we fall back to the null tracer to avoid unnecessary
        // allocations.
        var activity = _activitySource.StartActivity(name);

        if (activity is null)
            return NullPipelineTracer.Instance.StartActivity(name);

        var pipelineActivity = new PipelineActivity(activity);
        CurrentActivity = pipelineActivity;
        return pipelineActivity;
    }
}
