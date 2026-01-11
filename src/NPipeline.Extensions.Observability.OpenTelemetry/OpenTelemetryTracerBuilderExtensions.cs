using System.Diagnostics;
using OpenTelemetry.Trace;

namespace NPipeline.Extensions.Observability.OpenTelemetry;

/// <summary>
///     Extension methods for configuring OpenTelemetry <see cref="TracerProviderBuilder" /> to work with NPipeline.
/// </summary>
/// <remarks>
///     <para>
///         These extensions provide convenience methods to configure OpenTelemetry exporters
///         to capture traces from NPipeline pipelines. Use in combination with
///         <see
///             cref="DependencyInjection.OpenTelemetryObservabilityExtensions.AddOpenTelemetryPipelineTracer(Microsoft.Extensions.DependencyInjection.IServiceCollection, string)" />
///         to enable end-to-end OpenTelemetry integration.
///     </para>
///     <para>
///         Activities are emitted from an <see cref="ActivitySource" /> whose
///         <see cref="ActivitySource.Name" /> matches the service name used when constructing
///         <see cref="OpenTelemetryPipelineTracer" />. The same service name must be passed to
///         <see cref="AddNPipelineSource(TracerProviderBuilder,string)" /> for traces to be captured.
///     </para>
/// </remarks>
public static class OpenTelemetryTracerBuilderExtensions
{
    /// <summary>
    ///     Adds activity sources for NPipeline to the OpenTelemetry tracer provider.
    /// </summary>
    /// <param name="builder">The <see cref="TracerProviderBuilder" /> to configure.</param>
    /// <param name="serviceName">
    ///     The service name used when creating the <see cref="OpenTelemetryPipelineTracer" />.
    ///     This must match the <see cref="ActivitySource.Name" /> used by the tracer
    ///     for traces to be captured.
    /// </param>
    /// <returns>The <see cref="TracerProviderBuilder" /> for method chaining.</returns>
    /// <remarks>
    ///     <para>
    ///         This extension configures the tracer provider to listen for activities created by NPipeline
    ///         with the specified service name prefix. Activities created by <see cref="OpenTelemetryPipelineTracer" />
    ///         with matching service names will be automatically captured and exported to configured backends.
    ///     </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder" /> or <paramref name="serviceName" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="serviceName" /> is empty or whitespace.</exception>
    /// <example>
    ///     <code>
    /// // Configure OpenTelemetry with Jaeger exporter for NPipeline
    /// using var tracerProvider = new TracerProviderBuilder()
    ///     .AddNPipelineSource("MyPipeline")
    ///     .AddJaegerExporter()
    ///     .Build();
    /// 
    /// // Now all activities from OpenTelemetryPipelineTracer("MyPipeline") will be exported to Jaeger
    /// var tracer = new OpenTelemetryPipelineTracer("MyPipeline");
    /// </code>
    /// </example>
    public static TracerProviderBuilder AddNPipelineSource(
        this TracerProviderBuilder builder,
        string serviceName)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(serviceName);

        if (string.IsNullOrWhiteSpace(serviceName))
            throw new ArgumentException("Service name cannot be empty or whitespace.", nameof(serviceName));

        return builder.AddSource(serviceName);
    }

    /// <summary>
    ///     Adds activity sources for multiple NPipeline services to the OpenTelemetry tracer provider.
    /// </summary>
    /// <param name="builder">The <see cref="TracerProviderBuilder" /> to configure.</param>
    /// <param name="serviceNames">
    ///     An enumerable of service names used when creating <see cref="OpenTelemetryPipelineTracer" /> instances.
    ///     All specified service names will be added as activity sources.
    /// </param>
    /// <returns>The <see cref="TracerProviderBuilder" /> for method chaining.</returns>
    /// <remarks>
    ///     <para>
    ///         This is a convenience overload for scenarios where you have multiple NPipeline services
    ///         running with different service names that all need to be traced through a single
    ///         OpenTelemetry tracer provider.
    ///     </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="builder" /> or <paramref name="serviceNames" /> is null.</exception>
    /// <example>
    ///     <code>
    /// // Configure OpenTelemetry to capture traces from multiple pipeline services
    /// using var tracerProvider = new TracerProviderBuilder()
    ///     .AddNPipelineSources(new[] { "PipelineA", "PipelineB", "PipelineC" })
    ///     .AddJaegerExporter()
    ///     .Build();
    /// </code>
    /// </example>
    public static TracerProviderBuilder AddNPipelineSources(
        this TracerProviderBuilder builder,
        IEnumerable<string> serviceNames)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(serviceNames);

        foreach (var serviceName in serviceNames)
        {
            if (string.IsNullOrWhiteSpace(serviceName))
            {
                throw new ArgumentException(
                    "Service names cannot be null, empty, or whitespace.",
                    nameof(serviceNames));
            }

            _ = builder.AddSource(serviceName);
        }

        return builder;
    }

    /// <summary>
    ///     Gets information about activities created by an NPipeline tracer.
    /// </summary>
    /// <param name="activity">The activity to inspect.</param>
    /// <returns>
    ///     An <see cref="NPipelineActivityInfo" /> containing metadata about the NPipeline activity,
    ///     or null if the activity was not created by an NPipeline tracer.
    /// </returns>
    /// <remarks>
    ///     <para>
    ///         This helper method can be used to extract NPipeline-specific metadata from activities
    ///         in custom exporters or processors.
    ///     </para>
    /// </remarks>
    public static NPipelineActivityInfo? GetNPipelineInfo(this Activity activity)
    {
        ArgumentNullException.ThrowIfNull(activity);

        // Preferred path: activities created from ActivitySource where Source.Name
        // represents the service name used by OpenTelemetryPipelineTracer.
        var sourceName = activity.Source?.Name;

        if (!string.IsNullOrEmpty(sourceName))
        {
            var activityName = string.IsNullOrEmpty(activity.DisplayName)
                ? activity.OperationName
                : activity.DisplayName;

            return new NPipelineActivityInfo(sourceName, activityName, activity);
        }

        // Backwards-compatible fallback for activities whose DisplayName encodes
        // "Service.Activity" in a single string.
        var displayName = activity.DisplayName;
        var dotIndex = displayName.IndexOf('.');

        if (dotIndex <= 0)
            return null;

        var serviceName = displayName[..dotIndex];
        var name = displayName[(dotIndex + 1)..];

        return new NPipelineActivityInfo(serviceName, name, activity);
    }
}

/// <summary>
///     Information about an activity created by an NPipeline tracer.
/// </summary>
/// <param name="ServiceName">The service name prefix used by the tracer.</param>
/// <param name="ActivityName">The activity name (without service prefix).</param>
/// <param name="Activity">The underlying <see cref="System.Diagnostics.Activity" /> instance.</param>
public record NPipelineActivityInfo(string ServiceName, string ActivityName, Activity Activity);
