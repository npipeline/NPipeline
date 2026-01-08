using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Execution;
using NPipeline.Extensions.Observability;
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

        return services.AddNPipelineObservability<LoggingMetricsSink, LoggingPipelineMetricsSink>(ObservabilityExtensionOptions.Default);
    }

    /// <summary>
    ///     Adds NPipeline observability services with default logging sinks and custom configuration.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability(this IServiceCollection services, ObservabilityExtensionOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        return services.AddNPipelineObservability<LoggingMetricsSink, LoggingPipelineMetricsSink>(options);
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

        return services.AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(ObservabilityExtensionOptions.Default);
    }

    /// <summary>
    ///     Adds NPipeline observability services with specified metrics sinks and custom configuration.
    /// </summary>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services,
        ObservabilityExtensionOptions options)
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        // Register the observability collector as scoped (per pipeline run)
        services.TryAddScoped<IObservabilityCollector, ObservabilityCollector>();

        // Register the metrics sinks as scoped (per pipeline run)
        services.TryAddScoped<IMetricsSink, TMetricsSink>();
        services.TryAddScoped<IPipelineMetricsSink, TPipelineMetricsSink>();

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // Register the execution observer that bridges core events to the collector
        services.TryAddScoped<IExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(
                sp.GetRequiredService<IObservabilityCollector>(),
                options.EnableMemoryMetrics));

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

        return services.AddNPipelineObservability(metricsSinkFactory, pipelineMetricsSinkFactory, ObservabilityExtensionOptions.Default);
    }

    /// <summary>
    ///     Adds NPipeline observability services using factory delegates for creating sinks and custom configuration.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="metricsSinkFactory">A factory delegate to create the node metrics sink.</param>
    /// <param name="pipelineMetricsSinkFactory">A factory delegate to create the pipeline metrics sink.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability(
        this IServiceCollection services,
        Func<IServiceProvider, IMetricsSink> metricsSinkFactory,
        Func<IServiceProvider, IPipelineMetricsSink> pipelineMetricsSinkFactory,
        ObservabilityExtensionOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(metricsSinkFactory);
        ArgumentNullException.ThrowIfNull(pipelineMetricsSinkFactory);
        ArgumentNullException.ThrowIfNull(options);

        // Register the observability collector as scoped (per pipeline run)
        services.TryAddScoped<IObservabilityCollector, ObservabilityCollector>();

        // Register the metrics sinks as scoped (per pipeline run) using factory delegates
        services.TryAddScoped<IMetricsSink>(metricsSinkFactory);
        services.TryAddScoped<IPipelineMetricsSink>(pipelineMetricsSinkFactory);

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // Register the execution observer that bridges core events to the collector
        services.TryAddScoped<IExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(
                sp.GetRequiredService<IObservabilityCollector>(),
                options.EnableMemoryMetrics));

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

        return services.AddNPipelineObservability<TObservabilityCollector, TMetricsSink, TPipelineMetricsSink>(ObservabilityExtensionOptions.Default);
    }

    /// <summary>
    ///     Adds NPipeline observability services with a custom collector implementation and custom configuration.
    /// </summary>
    /// <typeparam name="TObservabilityCollector">The type of the observability collector.</typeparam>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TObservabilityCollector, TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services,
        ObservabilityExtensionOptions options)
        where TObservabilityCollector : class, IObservabilityCollector
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        // Register the custom observability collector
        services.TryAddScoped<IObservabilityCollector, TObservabilityCollector>();

        // Register the metrics sinks as scoped (per pipeline run)
        services.TryAddScoped<IMetricsSink, TMetricsSink>();
        services.TryAddScoped<IPipelineMetricsSink, TPipelineMetricsSink>();

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // Register the execution observer that bridges core events to the collector
        services.TryAddScoped<IExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(
                sp.GetRequiredService<IObservabilityCollector>(),
                options.EnableMemoryMetrics));

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

        return services.AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(collectorFactory, ObservabilityExtensionOptions.Default);
    }

    /// <summary>
    ///     Adds NPipeline observability services with a custom collector implementation using a factory delegate and custom configuration.
    /// </summary>
    /// <typeparam name="TMetricsSink">The type of the node metrics sink.</typeparam>
    /// <typeparam name="TPipelineMetricsSink">The type of the pipeline metrics sink.</typeparam>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="collectorFactory">A factory delegate to create the observability collector.</param>
    /// <param name="options">Configuration options for the observability extension.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipelineObservability<TMetricsSink, TPipelineMetricsSink>(
        this IServiceCollection services,
        Func<IServiceProvider, IObservabilityCollector> collectorFactory,
        ObservabilityExtensionOptions options)
        where TMetricsSink : class, IMetricsSink
        where TPipelineMetricsSink : class, IPipelineMetricsSink
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(collectorFactory);
        ArgumentNullException.ThrowIfNull(options);

        // Register the observability collector using factory delegate
        services.TryAddScoped<IObservabilityCollector>(collectorFactory);

        // Register the metrics sinks as scoped (per pipeline run)
        services.TryAddScoped<IMetricsSink, TMetricsSink>();
        services.TryAddScoped<IPipelineMetricsSink, TPipelineMetricsSink>();

        // Register the factory for DI resolution
        services.TryAddScoped<IObservabilityFactory, DiObservabilityFactory>();

        // Register the execution observer that bridges core events to the collector
        services.TryAddScoped<IExecutionObserver>(sp =>
            new MetricsCollectingExecutionObserver(
                sp.GetRequiredService<IObservabilityCollector>(),
                options.EnableMemoryMetrics));

        // Register the context factory for automatic observer configuration
        services.TryAddScoped<IObservablePipelineContextFactory, ObservablePipelineContextFactory>();

        return services;
    }
}
