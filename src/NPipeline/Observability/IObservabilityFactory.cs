using NPipeline.Observability.Metrics;

namespace NPipeline.Observability;

/// <summary>
///     A factory for resolving observability-related components.
/// </summary>
public interface IObservabilityFactory
{
    /// <summary>
    ///     Resolves an optional observability collector for tracking performance metrics.
    /// </summary>
    /// <returns>An <see cref="IObservabilityCollector" /> instance or null if observability is not enabled.</returns>
    IObservabilityCollector? ResolveObservabilityCollector();

    /// <summary>
    ///     Resolves an optional node metrics sink for recording metrics.
    /// </summary>
    /// <returns>An <see cref="IMetricsSink" /> instance or null if no sink is configured.</returns>
    IMetricsSink? ResolveMetricsSink();

    /// <summary>
    ///     Resolves an optional pipeline metrics sink for recording metrics.
    /// </summary>
    /// <returns>An <see cref="IPipelineMetricsSink" /> instance or null if no sink is configured.</returns>
    IPipelineMetricsSink? ResolvePipelineMetricsSink();
}
