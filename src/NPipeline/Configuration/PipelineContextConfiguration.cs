using Microsoft.Extensions.Logging;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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
///         Under <see cref="PipelineOptimizationProfile.Default" />, <see cref="NPipeline.Pipeline.PipelineContext" />
///         copies <c>Parameters</c> and wraps non-concurrent <c>Items</c> and <c>Properties</c> so writes stay safe.
///     </para>
///     <para>
///         <strong>Record Semantics:</strong>
///         Because this is a record, a <c>with</c> expression copies the dictionary references rather than the
///         dictionaries. Two configurations derived from one another therefore share the same instances.
///     </para>
///     <para>
///         <strong>Resilience Policy:</strong>
///         <see cref="ResiliencePolicy" /> decides how node and item failures are handled, when the graph does not
///         configure one. Precedence at run time: a node's own policy, then the graph's policy instance, then the
///         graph's policy type, then this policy, then <see cref="DefaultResiliencePolicy.Instance" />.
///     </para>
///     <para>
///         See <see cref="PipelineContext" /> for detailed thread-safety requirements and recommendations.
///     </para>
/// </remarks>
public sealed record PipelineContextConfiguration(
    IDictionary<string, object>? Parameters = null,
    IDictionary<string, object>? Items = null,
    IDictionary<string, object>? Properties = null,
    IErrorHandlerFactory? ErrorHandlerFactory = null,
    IResiliencePolicy? ResiliencePolicy = null,
    IDeadLetterSink? DeadLetterSink = null,
    ILoggerFactory? LoggerFactory = null,
    IPipelineTracer? Tracer = null,
    IObservabilityFactory? ObservabilityFactory = null,
    ILineageFactory? LineageFactory = null,
    PipelineOptimizationProfile OptimizationProfile = PipelineOptimizationProfile.Default,
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
        IObservabilityFactory? observabilityFactory = null) =>
        new(
            ErrorHandlerFactory: errorHandlerFactory,
            LineageFactory: lineageFactory,
            ObservabilityFactory: observabilityFactory);

    /// <summary>
    ///     Creates a configuration with logger factory.
    ///     Useful for configuring custom logging without affecting other observability settings.
    /// </summary>
    /// <param name="loggerFactory">The logger factory for the pipeline.</param>
    /// <returns>A new configuration with the specified logger factory.</returns>
    public static PipelineContextConfiguration WithLogging(ILoggerFactory loggerFactory) => Default with { LoggerFactory = loggerFactory };

    /// <summary>
    ///     Creates a configuration with parameters.
    ///     Useful for passing runtime data through the pipeline context.
    /// </summary>
    /// <param name="parameters">The parameters dictionary to attach to the context.</param>
    /// <returns>A new configuration with the specified parameters.</returns>
    public static PipelineContextConfiguration WithParameters(IDictionary<string, object> parameters) => new(parameters);

    /// <summary>
    ///     Creates a configuration with a cancellation token.
    ///     Useful for controlling pipeline execution lifetime.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token for the pipeline execution.</param>
    /// <returns>A new configuration with the specified cancellation token.</returns>
    public static PipelineContextConfiguration WithCancellation(CancellationToken cancellationToken) => new(CancellationToken: cancellationToken);

    /// <summary>
    ///     Creates a configuration with logger and tracer factories.
    ///     Useful for configuring custom observability implementations.
    /// </summary>
    /// <param name="loggerFactory">The logger factory for the pipeline.</param>
    /// <param name="tracer">The tracer for the pipeline.</param>
    /// <returns>A new configuration with the specified observability components.</returns>
    public static PipelineContextConfiguration WithObservability(
        ILoggerFactory? loggerFactory = null,
        IPipelineTracer? tracer = null) =>
        new(
            LoggerFactory: loggerFactory,
            Tracer: tracer);

    /// <summary>
    ///     Creates a configuration with dead-letter handling components.
    /// </summary>
    /// <param name="deadLetterSink">The dead-letter sink for failed items.</param>
    /// <returns>A new configuration with the specified dead-letter components.</returns>
    public static PipelineContextConfiguration WithErrorHandling(
        IDeadLetterSink? deadLetterSink = null) =>
        new(
            DeadLetterSink: deadLetterSink);

    /// <summary>
    ///     Creates a configuration with a unified resilience policy.
    /// </summary>
    /// <param name="resiliencePolicy">The resilience policy to use during execution.</param>
    /// <returns>A new configuration with the specified resilience policy.</returns>
    /// <remarks>
    ///     The policy is used when the graph does not configure one. Precedence at run time: a node's own policy, then
    ///     the graph's policy instance, then the graph's policy type, then this policy, then
    ///     <see cref="DefaultResiliencePolicy.Instance" />.
    /// </remarks>
    public static PipelineContextConfiguration WithResilience(IResiliencePolicy resiliencePolicy)
    {
        ArgumentNullException.ThrowIfNull(resiliencePolicy);
        return new PipelineContextConfiguration(ResiliencePolicy: resiliencePolicy);
    }
}
