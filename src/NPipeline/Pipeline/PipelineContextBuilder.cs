using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;

namespace NPipeline.Pipeline;

/// <summary>
///     Builder for creating <see cref="PipelineContext" /> instances with a fluent API.
///     This is the preferred way to configure complex pipeline context scenarios.
/// </summary>
/// <remarks>
///     <para>
///         The builder provides sensible defaults for all components. You only need to explicitly configure
///         factories if you're providing custom implementations.
///     </para>
///     <para>
///         Default components:
///         <list type="bullet">
///             <item>
///                 <description>ErrorHandlerFactory: <see cref="DefaultErrorHandlerFactory" /></description>
///             </item>
///             <item>
///                 <description>LineageFactory: <see cref="DefaultLineageFactory" /></description>
///             </item>
///             <item>
///                 <description>ObservabilityFactory: <see cref="DefaultObservabilityFactory" /></description>
///             </item>
///             <item>
///                 <description>LoggerFactory: <see cref="NullPipelineLoggerFactory" /> (no-op)</description>
///             </item>
///             <item>
///                 <description>Tracer: <see cref="NullPipelineTracer" /> (no-op)</description>
///             </item>
///             <item>
///                 <description>RetryOptions: <see cref="PipelineRetryOptions.Default" /> (no retries)</description>
///             </item>
///         </list>
///     </para>
///     <para>
///         Example usage (minimal):
///     </para>
///     <code>
///         var context = new PipelineContextBuilder()
///             .WithCancellation(cancellationToken)
///             .Build();
///     </code>
///     <para>
///         Example usage (custom configuration):
///     </para>
///     <code>
///         var context = new PipelineContextBuilder()
///             .WithRetry(retryOptions)
///             .WithCancellation(cancellationToken)
///             .WithParameters(parameters)
///             .WithErrorHandlerFactory(customErrorHandlerFactory)
///             .Build();
///     </code>
/// </remarks>
public sealed class PipelineContextBuilder
{
    private CancellationToken _cancellationToken;
    private IDeadLetterSink? _deadLetterSink;
    private IErrorHandlerFactory? _errorHandlerFactory;
    private Dictionary<string, object>? _items;
    private ILineageFactory? _lineageFactory;
    private IPipelineLoggerFactory? _loggerFactory;
    private IObservabilityFactory? _observabilityFactory;
    private Dictionary<string, object>? _parameters;
    private IPipelineErrorHandler? _pipelineErrorHandler;
    private Dictionary<string, object>? _properties;
    private PipelineRetryOptions? _retryOptions;
    private IPipelineTracer? _tracer;

    /// <summary>
    ///     Creates a new <see cref="PipelineContextBuilder" /> with all defaults configured.
    ///     Use this for the most common case - minimal configuration needed.
    /// </summary>
    /// <remarks>
    ///     This returns a builder ready to use with sensible defaults. You can immediately call
    ///     <see cref="Build" /> to get a basic context, or chain additional configuration methods
    ///     to customize specific components.
    /// </remarks>
    /// <example>
    ///     <code>
    ///         // Quick: Just build with defaults
    ///         var context = new PipelineContextBuilder().Build();
    /// 
    ///         // Flexible: Customize as needed
    ///         var context = new PipelineContextBuilder()
    ///             .WithCancellation(cancellationToken)
    ///             .WithRetry(retryOptions)
    ///             .Build();
    ///     </code>
    /// </example>
    public PipelineContextBuilder()
    {
        // All defaults are set at context creation time via PipelineContext constructor
        // No explicit initialization needed here - nulls will trigger defaults in PipelineContext
    }

    /// <summary>
    ///     Sets the parameters dictionary for the pipeline context.
    /// </summary>
    /// <param name="parameters">The parameters to attach to the context.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithParameters(Dictionary<string, object> parameters)
    {
        _parameters = parameters ?? throw new ArgumentNullException(nameof(parameters));
        return this;
    }

    /// <summary>
    ///     Sets the items dictionary for sharing state between pipeline nodes.
    /// </summary>
    /// <param name="items">The items dictionary to attach to the context.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithItems(Dictionary<string, object> items)
    {
        _items = items ?? throw new ArgumentNullException(nameof(items));
        return this;
    }

    /// <summary>
    ///     Sets the properties dictionary for storing extension-specific data.
    /// </summary>
    /// <param name="properties">The properties to attach to the context.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithProperties(Dictionary<string, object> properties)
    {
        _properties = properties ?? throw new ArgumentNullException(nameof(properties));
        return this;
    }

    /// <summary>
    ///     Sets the cancellation token for the pipeline execution.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token to monitor for pipeline cancellation.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithCancellation(CancellationToken cancellationToken)
    {
        _cancellationToken = cancellationToken;
        return this;
    }

    /// <summary>
    ///     Sets the logger factory and optional tracer for observability.
    /// </summary>
    /// <param name="loggerFactory">The logger factory for the pipeline.</param>
    /// <param name="tracer">The tracer for the pipeline, or null to use default.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithObservability(
        IPipelineLoggerFactory loggerFactory,
        IPipelineTracer? tracer = null)
    {
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));

        if (tracer is not null)
            _tracer = tracer;

        return this;
    }

    /// <summary>
    ///     Sets the tracer for observability.
    /// </summary>
    /// <param name="tracer">The tracer for the pipeline.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithTracer(IPipelineTracer tracer)
    {
        _tracer = tracer ?? throw new ArgumentNullException(nameof(tracer));
        return this;
    }

    /// <summary>
    ///     Sets the logger factory for observability.
    /// </summary>
    /// <param name="loggerFactory">The logger factory for the pipeline.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithLoggerFactory(IPipelineLoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        return this;
    }

    /// <summary>
    ///     Sets the pipeline-level error handler.
    /// </summary>
    /// <param name="errorHandler">The error handler for the pipeline.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithErrorHandler(IPipelineErrorHandler errorHandler)
    {
        _pipelineErrorHandler = errorHandler ?? throw new ArgumentNullException(nameof(errorHandler));
        return this;
    }

    /// <summary>
    ///     Sets the dead-letter sink for failed items.
    /// </summary>
    /// <param name="deadLetterSink">The sink for items that have failed processing.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithDeadLetterSink(IDeadLetterSink deadLetterSink)
    {
        _deadLetterSink = deadLetterSink ?? throw new ArgumentNullException(nameof(deadLetterSink));
        return this;
    }

    /// <summary>
    ///     Sets error handling components (both error handler and dead-letter sink).
    /// </summary>
    /// <param name="errorHandler">The pipeline-level error handler, or null to use existing.</param>
    /// <param name="deadLetterSink">The dead-letter sink, or null to use existing.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithErrorHandling(
        IPipelineErrorHandler? errorHandler = null,
        IDeadLetterSink? deadLetterSink = null)
    {
        if (errorHandler is not null)
            _pipelineErrorHandler = errorHandler;

        if (deadLetterSink is not null)
            _deadLetterSink = deadLetterSink;

        return this;
    }

    /// <summary>
    ///     Sets the factory for creating error handlers.
    /// </summary>
    /// <param name="factory">The error handler factory.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithErrorHandlerFactory(IErrorHandlerFactory factory)
    {
        _errorHandlerFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    /// <summary>
    ///     Sets the factory for creating lineage-related components.
    /// </summary>
    /// <param name="factory">The lineage factory.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithLineageFactory(ILineageFactory factory)
    {
        _lineageFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    /// <summary>
    ///     Sets the factory for resolving observability components.
    /// </summary>
    /// <param name="factory">The observability factory.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithObservabilityFactory(IObservabilityFactory factory)
    {
        _observabilityFactory = factory ?? throw new ArgumentNullException(nameof(factory));
        return this;
    }

    /// <summary>
    ///     Sets all three factories at once (error handling, lineage, and observability).
    /// </summary>
    /// <param name="errorHandlerFactory">The error handler factory, or null to use default.</param>
    /// <param name="lineageFactory">The lineage factory, or null to use default.</param>
    /// <param name="observabilityFactory">The observability factory, or null to use default.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithFactories(
        IErrorHandlerFactory? errorHandlerFactory = null,
        ILineageFactory? lineageFactory = null,
        IObservabilityFactory? observabilityFactory = null)
    {
        if (errorHandlerFactory is not null)
            _errorHandlerFactory = errorHandlerFactory;

        if (lineageFactory is not null)
            _lineageFactory = lineageFactory;

        if (observabilityFactory is not null)
            _observabilityFactory = observabilityFactory;

        return this;
    }

    /// <summary>
    ///     Sets the retry options for the pipeline.
    /// </summary>
    /// <param name="retryOptions">The retry options to apply globally across the pipeline.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineContextBuilder WithRetry(PipelineRetryOptions retryOptions)
    {
        _retryOptions = retryOptions ?? throw new ArgumentNullException(nameof(retryOptions));
        return this;
    }

    /// <summary>
    ///     Builds and returns the <see cref="PipelineContext" /> with the configured settings.
    /// </summary>
    /// <returns>A new pipeline context with the configured parameters.</returns>
    public PipelineContext Build()
    {
        var config = new PipelineContextConfiguration(
            _parameters,
            Items: _items,
            Properties: _properties,
            CancellationToken: _cancellationToken,
            LoggerFactory: _loggerFactory,
            Tracer: _tracer,
            PipelineErrorHandler: _pipelineErrorHandler,
            DeadLetterSink: _deadLetterSink,
            ErrorHandlerFactory: _errorHandlerFactory,
            LineageFactory: _lineageFactory,
            ObservabilityFactory: _observabilityFactory,
            RetryOptions: _retryOptions);

        return new PipelineContext(config);
    }

    /// <summary>
    ///     Creates a minimal pipeline context with just a cancellation token.
    ///     This is the most common use case for basic pipeline execution.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the pipeline execution.</param>
    /// <returns>A configured pipeline context ready for execution.</returns>
    /// <remarks>
    ///     This is a convenience method equivalent to:
    ///     <code>
    ///         new PipelineContext(PipelineContextConfiguration.WithCancellation(cancellationToken))
    ///     </code>
    ///     All other components use their sensible defaults.
    /// </remarks>
    public static PipelineContext CreateDefault(CancellationToken cancellationToken = default)
    {
        return new PipelineContext(PipelineContextConfiguration.WithCancellation(cancellationToken));
    }
}
