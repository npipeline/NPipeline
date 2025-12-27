using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Configuration;

/// <summary>
///     Configuration object for creating a <see cref="PipelineContext" />.
///     Groups all pipeline context parameters into a self-documenting record.
/// </summary>
/// <remarks>
///     <para>
///         <strong>Thread Safety:</strong>
///         <see cref="PipelineContextConfiguration" /> is immutable (record type) and thread-safe.
///         However, the dictionaries passed to it (Parameters, Items, Properties) should be considered
///         as becoming owned by the context, and their thread-safety depends on the context's usage.
///     </para>
///     <para>
///         See <see cref="PipelineContext" /> for detailed thread-safety requirements and recommendations.
///     </para>
/// </remarks>
public sealed record PipelineContextConfiguration(
    Dictionary<string, object>? Parameters = null,
    Dictionary<string, object>? Items = null,
    Dictionary<string, object>? Properties = null,
    PipelineRetryOptions? RetryOptions = null,
    IErrorHandlerFactory? ErrorHandlerFactory = null,
    IPipelineErrorHandler? PipelineErrorHandler = null,
    IDeadLetterSink? DeadLetterSink = null,
    IPipelineLoggerFactory? LoggerFactory = null,
    IPipelineTracer? Tracer = null,
    IObservabilityFactory? ObservabilityFactory = null,
    ILineageFactory? LineageFactory = null,
    CancellationToken CancellationToken = default)
{
    /// <summary>
    ///     Creates a default configuration with all null/default values.
    /// </summary>
    public static PipelineContextConfiguration Default => new();

    /// <summary>
    ///     Creates a configuration with specific factories.
    ///     Useful for tests that need to mock observability and error handling components.
    /// </summary>
    /// <param name="errorHandlerFactory">The error handler factory, or null to use the default.</param>
    /// <param name="lineageFactory">The lineage factory, or null to use the default.</param>
    /// <param name="observabilityFactory">The observability factory, or null to use the default.</param>
    /// <returns>A new configuration with the specified factories.</returns>
    public static PipelineContextConfiguration WithFactories(
        IErrorHandlerFactory? errorHandlerFactory = null,
        ILineageFactory? lineageFactory = null,
        IObservabilityFactory? observabilityFactory = null)
    {
        return new PipelineContextConfiguration(
            ErrorHandlerFactory: errorHandlerFactory,
            LineageFactory: lineageFactory,
            ObservabilityFactory: observabilityFactory);
    }

    /// <summary>
    ///     Creates a configuration with retry options.
    ///     Useful for configuring pipeline resilience behavior without affecting other settings.
    /// </summary>
    /// <param name="retries">The retry options to apply globally across the pipeline.</param>
    /// <returns>A new configuration with the specified retry options.</returns>
    public static PipelineContextConfiguration WithRetry(PipelineRetryOptions retries)
    {
        return Default with { RetryOptions = retries };
    }

    /// <summary>
    ///     Creates a configuration with logger factory.
    ///     Useful for configuring custom logging without affecting other observability settings.
    /// </summary>
    /// <param name="loggerFactory">The logger factory for the pipeline.</param>
    /// <returns>A new configuration with the specified logger factory.</returns>
    public static PipelineContextConfiguration WithLogging(IPipelineLoggerFactory loggerFactory)
    {
        return Default with { LoggerFactory = loggerFactory };
    }

    /// <summary>
    ///     Creates a configuration with parameters.
    ///     Useful for passing runtime data through the pipeline context.
    /// </summary>
    /// <param name="parameters">The parameters dictionary to attach to the context.</param>
    /// <returns>A new configuration with the specified parameters.</returns>
    public static PipelineContextConfiguration WithParameters(Dictionary<string, object> parameters)
    {
        return new PipelineContextConfiguration(parameters);
    }

    /// <summary>
    ///     Creates a configuration with a cancellation token.
    ///     Useful for controlling pipeline execution lifetime.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the pipeline execution.</param>
    /// <returns>A new configuration with the specified cancellation token.</returns>
    public static PipelineContextConfiguration WithCancellation(CancellationToken cancellationToken)
    {
        return new PipelineContextConfiguration(CancellationToken: cancellationToken);
    }

    /// <summary>
    ///     Creates a configuration with logger and tracer factories.
    ///     Useful for configuring custom observability implementations.
    /// </summary>
    /// <param name="loggerFactory">The logger factory for the pipeline.</param>
    /// <param name="tracer">The tracer for the pipeline.</param>
    /// <returns>A new configuration with the specified observability components.</returns>
    public static PipelineContextConfiguration WithObservability(
        IPipelineLoggerFactory? loggerFactory = null,
        IPipelineTracer? tracer = null)
    {
        return new PipelineContextConfiguration(
            LoggerFactory: loggerFactory,
            Tracer: tracer);
    }

    /// <summary>
    ///     Creates a configuration with error handling components.
    ///     Useful for configuring pipeline error handlers and dead-letter sinks.
    /// </summary>
    /// <param name="pipelineErrorHandler">The pipeline-level error handler.</param>
    /// <param name="deadLetterSink">The dead-letter sink for failed items.</param>
    /// <returns>A new configuration with the specified error handling components.</returns>
    public static PipelineContextConfiguration WithErrorHandling(
        IPipelineErrorHandler? pipelineErrorHandler = null,
        IDeadLetterSink? deadLetterSink = null)
    {
        return new PipelineContextConfiguration(
            PipelineErrorHandler: pipelineErrorHandler,
            DeadLetterSink: deadLetterSink);
    }
}
