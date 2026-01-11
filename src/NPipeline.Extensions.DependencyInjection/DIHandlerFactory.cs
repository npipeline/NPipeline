using Microsoft.Extensions.DependencyInjection;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Observability;
using NPipeline.Observability.Metrics;

namespace NPipeline.Extensions.DependencyInjection;

/// <summary>
///     A dependency injection-based factory for creating instances of error handlers, lineage components, and observability components.
///     This implementation provides focused factories for different concerns while maintaining the same DI-based functionality.
/// </summary>
public sealed class DiHandlerFactory(IServiceProvider serviceProvider) : IErrorHandlerFactory, ILineageFactory, IObservabilityFactory
{
    /// <summary>
    ///     Creates an instance of the specified error handler type.
    /// </summary>
    /// <param name="handlerType">The type of the error handler to create.</param>
    /// <returns>An instance of <see cref="IPipelineErrorHandler" />, or null if it cannot be created.</returns>
    public IPipelineErrorHandler? CreateErrorHandler(Type handlerType)
    {
        // Prefer registered instance; fallback to DI construction for better DX.
        var instance = serviceProvider.GetService(handlerType);

        if (instance is IPipelineErrorHandler eh)
            return eh;

        try
        {
            return ActivatorUtilities.CreateInstance(serviceProvider, handlerType) as IPipelineErrorHandler;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Creates an instance of the specified node error handler type.
    /// </summary>
    /// <param name="handlerType">The type of the error handler to create.</param>
    /// <returns>An instance of <see cref="INodeErrorHandler" />, or null if it cannot be created.</returns>
    public INodeErrorHandler? CreateNodeErrorHandler(Type handlerType)
    {
        var instance = serviceProvider.GetService(handlerType);

        if (instance is INodeErrorHandler neh)
            return neh;

        try
        {
            return ActivatorUtilities.CreateInstance(serviceProvider, handlerType) as INodeErrorHandler;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Creates an instance of the specified dead-letter sink type.
    /// </summary>
    /// <param name="sinkType">The type of the dead-letter sink to create.</param>
    /// <returns>An instance of <see cref="IDeadLetterSink" />, or null if it cannot be created.</returns>
    public IDeadLetterSink? CreateDeadLetterSink(Type sinkType)
    {
        var instance = serviceProvider.GetService(sinkType);

        if (instance is IDeadLetterSink dls)
            return dls;

        try
        {
            return ActivatorUtilities.CreateInstance(serviceProvider, sinkType) as IDeadLetterSink;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Creates an instance of the specified lineage sink type.
    /// </summary>
    /// <param name="sinkType">The type of the lineage sink to create.</param>
    /// <returns>An instance of <see cref="ILineageSink" />, or null if it cannot be created.</returns>
    public ILineageSink? CreateLineageSink(Type sinkType)
    {
        var instance = serviceProvider.GetService(sinkType);

        if (instance is ILineageSink ls)
            return ls;

        try
        {
            return ActivatorUtilities.CreateInstance(serviceProvider, sinkType) as ILineageSink;
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
        var instance = serviceProvider.GetService(sinkType);

        if (instance is IPipelineLineageSink pls)
            return pls;

        try
        {
            return ActivatorUtilities.CreateInstance(serviceProvider, sinkType) as IPipelineLineageSink;
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
        try
        {
            return serviceProvider.GetService<IPipelineLineageSinkProvider>();
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Resolves an optional lineage collector for tracking data lineage.
    /// </summary>
    /// <returns>An <see cref="ILineageCollector" /> instance or null if lineage is not enabled.</returns>
    public ILineageCollector? ResolveLineageCollector()
    {
        try
        {
            return serviceProvider.GetService<ILineageCollector>();
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Resolves an optional observability collector for tracking performance metrics.
    /// </summary>
    /// <returns>An <see cref="IObservabilityCollector" /> instance or null if observability is not enabled.</returns>
    public IObservabilityCollector? ResolveObservabilityCollector()
    {
        try
        {
            return serviceProvider.GetService<IObservabilityCollector>();
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Resolves an optional metrics sink for recording node-level metrics.
    /// </summary>
    /// <returns>An <see cref="IMetricsSink" /> instance or null if no sink is registered.</returns>
    public IMetricsSink? ResolveMetricsSink()
    {
        try
        {
            return serviceProvider.GetService<IMetricsSink>();
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Resolves an optional pipeline metrics sink for recording pipeline-level metrics.
    /// </summary>
    /// <returns>An <see cref="IPipelineMetricsSink" /> instance or null if no sink is registered.</returns>
    public IPipelineMetricsSink? ResolvePipelineMetricsSink()
    {
        try
        {
            return serviceProvider.GetService<IPipelineMetricsSink>();
        }
        catch
        {
            return null;
        }
    }
}
