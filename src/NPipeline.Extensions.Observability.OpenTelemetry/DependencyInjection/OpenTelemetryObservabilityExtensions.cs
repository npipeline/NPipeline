using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.Tracing;

namespace NPipeline.Extensions.Observability.OpenTelemetry.DependencyInjection;

/// <summary>
///     Extension methods for integrating OpenTelemetry-based pipeline tracing with dependency injection.
/// </summary>
public static class OpenTelemetryObservabilityExtensions
{
    /// <summary>
    ///     Adds <see cref="OpenTelemetryPipelineTracer" /> as the <see cref="IPipelineTracer" /> implementation.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="serviceName">
    ///     The service/application name to use for activities. This name will be prefixed to all
    ///     activity names to help identify spans in trace visualizers. Defaults to "NPipeline" if not provided.
    /// </param>
    /// <returns>The <see cref="IServiceCollection" /> for method chaining.</returns>
    /// <remarks>
    ///     <para>
    ///         This extension registers a singleton <see cref="OpenTelemetryPipelineTracer" /> instance
    ///         that will be used for all pipeline tracing. The tracer automatically creates activities
    ///         compatible with OpenTelemetry instrumentation.
    ///     </para>
    ///     <para>
    ///         To export traces to OpenTelemetry backends (Jaeger, Zipkin, etc.), configure an
    ///         OpenTelemetry <see cref="global::OpenTelemetry.Trace.TracerProvider" /> separately
    ///         with sources matching your service name.
    ///     </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="services" /> or <paramref name="serviceName" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="serviceName" /> is empty or whitespace.</exception>
    /// <example>
    ///     <code>
    /// var services = new ServiceCollection();
    /// 
    /// // Add observability with OpenTelemetry tracing
    /// services.AddNPipelineObservability();
    /// services.AddOpenTelemetryPipelineTracer("MyPipelineService");
    /// 
    /// // Configure OpenTelemetry export (example with Jaeger)
    /// using var tracerProvider = new TracerProviderBuilder()
    ///     .AddSource("MyPipelineService")
    ///     .AddJaegerExporter()
    ///     .Build();
    /// 
    /// var provider = services.BuildServiceProvider();
    /// var tracer = provider.GetRequiredService&lt;IPipelineTracer&gt;();
    /// 
    /// // Use in pipeline context
    /// var context = new PipelineContext(
    ///     PipelineContextConfiguration.WithObservability(tracer: tracer)
    /// );
    /// </code>
    /// </example>
    public static IServiceCollection AddOpenTelemetryPipelineTracer(
        this IServiceCollection services,
        string serviceName = "NPipeline")
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(serviceName);

        if (string.IsNullOrWhiteSpace(serviceName))
            throw new ArgumentException("Service name cannot be empty or whitespace.", nameof(serviceName));

        services.AddSingleton<IPipelineTracer>(new OpenTelemetryPipelineTracer(serviceName));

        return services;
    }

    /// <summary>
    ///     Adds <see cref="OpenTelemetryPipelineTracer" /> as the <see cref="IPipelineTracer" /> implementation
    ///     with a custom factory delegate.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add services to.</param>
    /// <param name="factory">
    ///     A factory delegate that creates the <see cref="OpenTelemetryPipelineTracer" /> instance.
    ///     This allows for custom initialization logic.
    /// </param>
    /// <returns>The <see cref="IServiceCollection" /> for method chaining.</returns>
    /// <remarks>
    ///     <para>
    ///         This overload is useful when you need custom initialization logic for the tracer,
    ///         such as configuring the service name dynamically based on configuration or environment variables.
    ///     </para>
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="services" /> or <paramref name="factory" /> is null.</exception>
    /// <example>
    ///     <code>
    /// var services = new ServiceCollection();
    /// services.AddOpenTelemetryPipelineTracer(sp =>
    /// {
    ///     var config = sp.GetRequiredService&lt;IConfiguration&gt;();
    ///     var serviceName = config["ServiceName"] ?? "DefaultService";
    ///     return new OpenTelemetryPipelineTracer(serviceName);
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection AddOpenTelemetryPipelineTracer(
        this IServiceCollection services,
        Func<IServiceProvider, OpenTelemetryPipelineTracer> factory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(factory);

        _ = services.AddSingleton<IPipelineTracer>(sp => factory(sp));

        return services;
    }
}
