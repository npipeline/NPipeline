using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Execution;
using NPipeline.Observability.Metrics;

namespace NPipeline.Observability.DependencyInjection;

/// <summary>
///     Provides extension methods for setting up NPipeline observability services in an <see cref="IServiceCollection" />.
/// </summary>
public static class ObservabilityServiceCollectionExtensions
{
    /// <summary>
    ///     Adds NPipeline observability services with default logging sinks.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        return services.AddNPipelineObservability<LoggingMetricsSink, LoggingPipelineMetricsSink>();
    }

    /// <summary>
    ///     Adds NPipeline observability services with specified metrics sinks.
    /// </summary>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(this IServiceCollection services)
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register the observability collector as scoped (per pipeline run)
        services.TryAddScoped<IObservabilityCollector, ObservabilityCollector>();

        // Register the metrics sinks
        services.TryAddTransient<IMetricsSink, TMetricsSink>();
        services.TryAddTransient<IPipelineMetricsSink, TPipelineMetricsSink>();

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // Register the execution observer that bridges core events to the collector
        services.TryAddScoped<IExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(sp.GetRequiredService<IObservabilityCollector>()));

        // Register the context factory for automatic observer configuration
        services.TryAddScoped<IObservablePipelineContextFactory, ObservablePipelineContextFactory>();

        return services;
    }

    /// <summary>
    ///     Adds NPipeline observability services using factory delegates for creating sinks.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="metricsSinkFactory">A factory delegate to create the node metrics sink.</param>
    /// <param name="pipelineMetricsSinkFactory">A factory delegate to create the pipeline metrics sink.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability(
        this IServiceCollection services,
        Func<IServiceProvider, IMetricsSink> metricsSinkFactory,
        Func<IServiceProvider, IPipelineMetricsSink> pipelineMetricsSinkFactory)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(metricsSinkFactory);
        ArgumentNullException.ThrowIfNull(pipelineMetricsSinkFactory);

        // Register the observability collector as scoped (per pipeline run)
        services.TryAddScoped<IObservabilityCollector, ObservabilityCollector>();

        // Register the metrics sinks using factory delegates
        services.TryAddTransient<IMetricsSink>(metricsSinkFactory);
        services.TryAddTransient<IPipelineMetricsSink>(pipelineMetricsSinkFactory);

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // Register the execution observer that bridges core events to the collector
        services.TryAddScoped<IExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(sp.GetRequiredService<IObservabilityCollector>()));

        // Register the context factory for automatic observer configuration
        services.TryAddScoped<IObservablePipelineContextFactory, ObservablePipelineContextFactory>();

        return services;
    }

    /// <summary>
    ///     Adds NPipeline observability services with a custom collector implementation.
    /// </summary>
    /// <typeparam name="TObservabilityCollector">The type of the observability collector.</typeparam>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TObservabilityCollector, TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services)
        where TObservabilityCollector : class, IObservabilityCollector
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register the custom observability collector
        services.TryAddScoped<IObservabilityCollector, TObservabilityCollector>();

        // Register the metrics sinks
        services.TryAddTransient<IMetricsSink, TMetricsSink>();
        services.TryAddTransient<IPipelineMetricsSink, TPipelineMetricsSink>();

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // Register the execution observer that bridges core events to the collector
        services.TryAddScoped<IExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(sp.GetRequiredService<IObservabilityCollector>()));

        // Register the context factory for automatic observer configuration
        services.TryAddScoped<IObservablePipelineContextFactory, ObservablePipelineContextFactory>();

        return services;
    }

    /// <summary>
    ///     Adds NPipeline observability services with a custom collector implementation using a factory delegate.
    /// </summary>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="collectorFactory">A factory delegate to create the observability collector.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services,
        Func<IServiceProvider, IObservabilityCollector> collectorFactory)
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(collectorFactory);

        // Register the observability collector using factory delegate
        services.TryAddScoped<IObservabilityCollector>(collectorFactory);

        // Register the metrics sinks
        services.TryAddTransient<IMetricsSink, TMetricsSink>();
        services.TryAddTransient<IPipelineMetricsSink, TPipelineMetricsSink>();

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // Register the execution observer that bridges core events to the collector
        services.TryAddScoped<IExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(sp.GetRequiredService<IObservabilityCollector>()));

        // Register the context factory for automatic observer configuration
        services.TryAddScoped<IObservablePipelineContextFactory, ObservablePipelineContextFactory>();

        return services;
    }
}