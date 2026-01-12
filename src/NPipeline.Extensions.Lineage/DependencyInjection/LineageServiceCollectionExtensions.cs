using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace NPipeline.Lineage.DependencyInjection;

/// <summary>
///     Provides extension methods for setting up NPipeline lineage services in an <see cref="IServiceCollection" />.
/// </summary>
public static class LineageServiceCollectionExtensions
{
    /// <summary>
    ///     Adds NPipeline lineage services with default logging sinks.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineLineage(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.AddNPipelineLineage<LoggingPipelineLineageSink>();
    }

    /// <summary>
    ///     Adds NPipeline lineage services with a specified pipeline lineage sink type.
    /// </summary>
    /// <typeparam name="TPipelineLineageSink">The type of the pipeline lineage sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineLineage<TPipelineLineageSink>(this IServiceCollection services)
        where TPipelineLineageSink : class, IPipelineLineageSink
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register the lineage collector as scoped (per pipeline run)
        services.TryAddScoped<ILineageCollector, LineageCollector>();

        // Register the pipeline lineage sink as scoped (per pipeline run)
        services.TryAddScoped<IPipelineLineageSink, TPipelineLineageSink>();

        RegisterCoreLineageServices(services);

        return services;
    }

    /// <summary>
    ///     Adds NPipeline lineage services using a factory delegate for creating the pipeline lineage sink.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="pipelineLineageSinkFactory">A factory delegate to create the pipeline lineage sink.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineLineage(
        this IServiceCollection services,
        Func<IServiceProvider, IPipelineLineageSink> pipelineLineageSinkFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(pipelineLineageSinkFactory);

        // Register the lineage collector as scoped (per pipeline run)
        services.TryAddScoped<ILineageCollector, LineageCollector>();

        // Register the pipeline lineage sink as scoped (per pipeline run) using factory delegate
        services.TryAddScoped<IPipelineLineageSink>(pipelineLineageSinkFactory);

        RegisterCoreLineageServices(services);

        return services;
    }

    /// <summary>
    ///     Adds NPipeline lineage services with a custom collector implementation.
    /// </summary>
    /// <typeparam name="TLineageCollector">The type of the lineage collector.</typeparam>
    /// <typeparam name="TPipelineLineageSink">The type of the pipeline lineage sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineLineage<TLineageCollector, TPipelineLineageSink>(
        this IServiceCollection services)
        where TLineageCollector : class, ILineageCollector
        where TPipelineLineageSink : class, IPipelineLineageSink
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register the custom lineage collector
        services.TryAddScoped<ILineageCollector, TLineageCollector>();

        // Register the pipeline lineage sink as scoped (per pipeline run)
        services.TryAddScoped<IPipelineLineageSink, TPipelineLineageSink>();

        RegisterCoreLineageServices(services);

        return services;
    }

    /// <summary>
    ///     Adds NPipeline lineage services with a custom collector implementation using a factory delegate.
    /// </summary>
    /// <typeparam name="TPipelineLineageSink">The type of the pipeline lineage sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="collectorFactory">A factory delegate to create the lineage collector.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineLineage<TPipelineLineageSink>(
        this IServiceCollection services,
        Func<IServiceProvider, ILineageCollector> collectorFactory)
        where TPipelineLineageSink : class, IPipelineLineageSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(collectorFactory);

        // Register the lineage collector using factory delegate
        services.TryAddScoped<ILineageCollector>(collectorFactory);

        // Register the pipeline lineage sink as scoped (per pipeline run)
        services.TryAddScoped<IPipelineLineageSink, TPipelineLineageSink>();

        RegisterCoreLineageServices(services);

        return services;
    }

    /// <summary>
    ///     Registers the shared core services used by all lineage configurations.
    /// </summary>
    private static void RegisterCoreLineageServices(IServiceCollection services)
    {
        // Register the factory for DI resolution
        services.TryAddScoped<ILineageFactory, DiLineageFactory>();

        // Register the default pipeline lineage sink provider
        services.TryAddScoped<IPipelineLineageSinkProvider, DefaultPipelineLineageSinkProvider>();
    }
}