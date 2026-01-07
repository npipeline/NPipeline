using Microsoft.Extensions.DependencyInjection;
using NPipeline.Observability.Metrics;

namespace NPipeline.Observability;

/// <summary>
///     Factory for resolving observability-related components from a dependency injection container.
/// </summary>
public sealed class DiObservabilityFactory : IObservabilityFactory
{
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DiObservabilityFactory" /> class.
    /// </summary>
    /// <param name="serviceProvider">The service provider to resolve components from.</param>
    public DiObservabilityFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    /// <summary>
    ///     Resolves an optional observability collector for tracking performance metrics.
    /// </summary>
    /// <returns>An <see cref="IObservabilityCollector" /> instance or null if observability is not enabled.</returns>
    public IObservabilityCollector? ResolveObservabilityCollector()
    {
        // Try to resolve from the service provider
        // Returns null if not registered, allowing observability to be optional
        return _serviceProvider.GetService<IObservabilityCollector>();
    }

    /// <summary>
    ///     Resolves an optional node metrics sink for recording metrics.
    /// </summary>
    /// <returns>An <see cref="IMetricsSink" /> instance or null if no sink is configured.</returns>
    public IMetricsSink? ResolveMetricsSink()
    {
        return _serviceProvider.GetService<IMetricsSink>();
    }

    /// <summary>
    ///     Resolves an optional pipeline metrics sink for recording metrics.
    /// </summary>
    /// <returns>An <see cref="IPipelineMetricsSink" /> instance or null if no sink is configured.</returns>
    public IPipelineMetricsSink? ResolvePipelineMetricsSink()
    {
        return _serviceProvider.GetService<IPipelineMetricsSink>();
    }
}