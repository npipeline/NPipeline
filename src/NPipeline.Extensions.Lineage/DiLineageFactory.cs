using Microsoft.Extensions.DependencyInjection;

namespace NPipeline.Lineage;

/// <summary>
///     Factory for resolving lineage-related components from a dependency injection container.
/// </summary>
public sealed class DiLineageFactory : ILineageFactory
{
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DiLineageFactory" /> class.
    /// </summary>
    /// <param name="serviceProvider">The service provider to resolve components from.</param>
    public DiLineageFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    /// <summary>
    ///     Creates an instance of the specified lineage sink type.
    /// </summary>
    /// <param name="sinkType">The type of the lineage sink to create.</param>
    /// <returns>An instance of <see cref="ILineageSink" />, or null if it cannot be created.</returns>
    public ILineageSink? CreateLineageSink(Type sinkType)
    {
        ArgumentNullException.ThrowIfNull(sinkType);

        if (!typeof(ILineageSink).IsAssignableFrom(sinkType))
            return null;

        try
        {
            return (ILineageSink?)_serviceProvider.GetService(sinkType)
                   ?? (ILineageSink?)ActivatorUtilities.CreateInstance(_serviceProvider, sinkType);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Creates an instance of the specified pipeline lineage sink type (explicit configuration path).
    /// </summary>
    /// <remarks>
    ///     Use this when a concrete sink type is explicitly configured via the builder or context. This is an imperative,
    ///     unambiguous request to construct that sink (typically via DI and falling back to ActivatorUtilities).
    /// </remarks>
    /// <param name="sinkType">The type of the pipeline lineage sink to create.</param>
    /// <returns>An instance of <see cref="IPipelineLineageSink" />, or null if it cannot be created.</returns>
    public IPipelineLineageSink? CreatePipelineLineageSink(Type sinkType)
    {
        ArgumentNullException.ThrowIfNull(sinkType);

        if (!typeof(IPipelineLineageSink).IsAssignableFrom(sinkType))
            return null;

        try
        {
            return (IPipelineLineageSink?)_serviceProvider.GetService(sinkType)
                   ?? (IPipelineLineageSink?)ActivatorUtilities.CreateInstance(_serviceProvider, sinkType);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Resolves an optional provider capable of supplying a default pipeline lineage sink (implicit default path).
    /// </summary>
    /// <remarks>
    ///     This is consulted only when no explicit sink (instance or type) is configured and item-level lineage is enabled.
    ///     It allows optional packages (e.g., NPipeline.Lineage) to supply a sensible default without reflection.
    ///     Returns null when no provider is registered or available.
    /// </remarks>
    /// <returns>An <see cref="IPipelineLineageSinkProvider" /> instance or null.</returns>
    public IPipelineLineageSinkProvider? ResolvePipelineLineageSinkProvider()
    {
        return _serviceProvider.GetService<IPipelineLineageSinkProvider>();
    }

    /// <summary>
    ///     Resolves an optional lineage collector for tracking data lineage.
    /// </summary>
    /// <returns>An <see cref="ILineageCollector" /> instance or null if lineage is not enabled.</returns>
    public ILineageCollector? ResolveLineageCollector()
    {
        // Try to resolve from the service provider
        // Returns null if not registered, allowing lineage to be optional
        return _serviceProvider.GetService<ILineageCollector>();
    }
}
