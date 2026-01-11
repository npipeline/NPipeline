using NPipeline.Observability.Metrics;

namespace NPipeline.Observability
{
    /// <summary>
    ///     Default implementation of <see cref="IObservabilityFactory" /> that resolves observability-related components
    ///     with proper error handling and logging.
    /// </summary>
    internal sealed class DefaultObservabilityFactory : IObservabilityFactory
    {
        /// <summary>
        ///     Resolves an optional observability collector for tracking performance metrics.
        /// </summary>
        /// <returns>An <see cref="IObservabilityCollector" /> instance or null if observability is not enabled.</returns>
        public IObservabilityCollector? ResolveObservabilityCollector()
        {
            // No DI container available in the default factory; cannot supply a collector.
            // Observability collection requires DI registration through services.AddObservability().
            return null;
        }

        /// <summary>
        ///     Resolves an optional metrics sink for consuming node-level metrics.
        /// </summary>
        /// <returns>An <see cref="IMetricsSink" /> instance or null if not available.</returns>
        public IMetricsSink? ResolveMetricsSink()
        {
            // No DI container available in the default factory; cannot supply a sink.
            // Metrics sinks require DI registration through services.AddObservability().
            return null;
        }

        /// <summary>
        ///     Resolves an optional pipeline metrics sink for consuming pipeline-level metrics.
        /// </summary>
        /// <returns>An <see cref="IPipelineMetricsSink" /> instance or null if not available.</returns>
        public IPipelineMetricsSink? ResolvePipelineMetricsSink()
        {
            // No DI container available in the default factory; cannot supply a sink.
            // Pipeline metrics sinks require DI registration through services.AddObservability().
            return null;
        }
    }
}
