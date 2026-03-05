using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.Azure.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Connection;
using NPipeline.Connectors.Azure.ServiceBus.Exceptions;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;

namespace NPipeline.Connectors.Azure.ServiceBus.DependencyInjection;

/// <summary>
///     Extension methods for registering Azure Service Bus connector services.
/// </summary>
public static class ServiceBusServiceCollectionExtensions
{
    /// <summary>
    ///     Registers the Azure Service Bus connector infrastructure:
    ///     <see cref="IServiceBusConnectionPool" /> and <see cref="ServiceBusTransientErrorDetector" />.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional action to configure <see cref="ServiceBusConfiguration" />.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddServiceBusConnector(
        this IServiceCollection services,
        Action<ServiceBusConfiguration>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register the connection pool, optionally resolving AzureConnectionOptions from the DI container
        services.AddSingleton<IServiceBusConnectionPool>(sp =>
        {
            var azureOptions = sp.GetService<AzureConnectionOptions>();

            return azureOptions != null
                ? new ServiceBusConnectionPool(azureOptions)
                : new ServiceBusConnectionPool();
        });

        services.AddSingleton<ServiceBusTransientErrorDetector>(_ => ServiceBusTransientErrorDetector.Instance);

        if (configure != null)
        {
            var config = new ServiceBusConfiguration();
            configure(config);
            services.AddSingleton(config);
        }

        return services;
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusQueueSourceNode{T}" /> with a specific configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusQueueSource<T>(
        this IServiceCollection services,
        Action<ServiceBusConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var config = new ServiceBusConfiguration();
        configure(config);
        config.ValidateSource();
        services.AddSingleton(_ => new ServiceBusQueueSourceNode<T>(config));
        return services;
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusQueueSourceNode{T}" /> for a queue name,
    ///     with optional additional configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusQueueSource<T>(
        this IServiceCollection services,
        string queueName,
        Action<ServiceBusConfiguration>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        return services.AddServiceBusQueueSource<T>(config =>
        {
            config.QueueName = queueName;
            configure?.Invoke(config);
        });
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusSubscriptionSourceNode{T}" /> with a specific configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusSubscriptionSource<T>(
        this IServiceCollection services,
        Action<ServiceBusConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var config = new ServiceBusConfiguration();
        configure(config);
        config.ValidateSource();
        services.AddSingleton(_ => new ServiceBusSubscriptionSourceNode<T>(config));
        return services;
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusSubscriptionSourceNode{T}" /> for a topic subscription,
    ///     with optional additional configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusSubscriptionSource<T>(
        this IServiceCollection services,
        string topicName,
        string subscriptionName,
        Action<ServiceBusConfiguration>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionName);

        return services.AddServiceBusSubscriptionSource<T>(config =>
        {
            config.TopicName = topicName;
            config.SubscriptionName = subscriptionName;
            configure?.Invoke(config);
        });
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusSessionSourceNode{T}" /> with a specific configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusSessionSource<T>(
        this IServiceCollection services,
        Action<ServiceBusConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var config = new ServiceBusConfiguration();
        configure(config);
        config.ValidateSource();
        services.AddSingleton(_ => new ServiceBusSessionSourceNode<T>(config));
        return services;
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusSessionSourceNode{T}" /> for a session-enabled queue,
    ///     with optional additional configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusSessionSource<T>(
        this IServiceCollection services,
        string queueName,
        Action<ServiceBusConfiguration>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        return services.AddServiceBusSessionSource<T>(config =>
        {
            config.QueueName = queueName;
            config.EnableSessions = true;
            configure?.Invoke(config);
        });
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusSessionSourceNode{T}" /> for a session-enabled topic subscription,
    ///     with optional additional configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusSessionSource<T>(
        this IServiceCollection services,
        string topicName,
        string subscriptionName,
        Action<ServiceBusConfiguration>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscriptionName);

        return services.AddServiceBusSessionSource<T>(config =>
        {
            config.TopicName = topicName;
            config.SubscriptionName = subscriptionName;
            config.EnableSessions = true;
            configure?.Invoke(config);
        });
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusQueueSinkNode{T}" /> with a specific configuration.
    ///     Uses the shared <see cref="IServiceBusConnectionPool" /> when available.
    /// </summary>
    public static IServiceCollection AddServiceBusQueueSink<T>(
        this IServiceCollection services,
        Action<ServiceBusConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var config = new ServiceBusConfiguration();
        configure(config);
        config.ValidateSink();

        services.AddSingleton(sp =>
        {
            var pool = sp.GetService<IServiceBusConnectionPool>();

            if (pool != null && !string.IsNullOrWhiteSpace(config.QueueName))
            {
                var sender = pool.GetOrCreateSender(config, config.QueueName!);
                return new ServiceBusQueueSinkNode<T>(sender, config);
            }

            return new ServiceBusQueueSinkNode<T>(config);
        });

        return services;
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusQueueSinkNode{T}" /> for a queue name,
    ///     with optional additional configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusQueueSink<T>(
        this IServiceCollection services,
        string queueName,
        Action<ServiceBusConfiguration>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(queueName);

        return services.AddServiceBusQueueSink<T>(config =>
        {
            config.QueueName = queueName;
            configure?.Invoke(config);
        });
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusTopicSinkNode{T}" /> with a specific configuration.
    ///     Uses the shared <see cref="IServiceBusConnectionPool" /> when available.
    /// </summary>
    public static IServiceCollection AddServiceBusTopicSink<T>(
        this IServiceCollection services,
        Action<ServiceBusConfiguration> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var config = new ServiceBusConfiguration();
        configure(config);
        config.ValidateSink();

        services.AddSingleton(sp =>
        {
            var pool = sp.GetService<IServiceBusConnectionPool>();

            if (pool != null && !string.IsNullOrWhiteSpace(config.TopicName))
            {
                var sender = pool.GetOrCreateSender(config, config.TopicName!);
                return new ServiceBusTopicSinkNode<T>(sender, config);
            }

            return new ServiceBusTopicSinkNode<T>(config);
        });

        return services;
    }

    /// <summary>
    ///     Registers a typed <see cref="ServiceBusTopicSinkNode{T}" /> for a topic name,
    ///     with optional additional configuration.
    /// </summary>
    public static IServiceCollection AddServiceBusTopicSink<T>(
        this IServiceCollection services,
        string topicName,
        Action<ServiceBusConfiguration>? configure = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topicName);

        return services.AddServiceBusTopicSink<T>(config =>
        {
            config.TopicName = topicName;
            configure?.Invoke(config);
        });
    }
}
