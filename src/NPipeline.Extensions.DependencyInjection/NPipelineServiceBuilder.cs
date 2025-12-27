using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.DependencyInjection;

/// <summary>
///     Fluent builder for configuring NPipeline services in a dependency injection container.
/// </summary>
public sealed class NPipelineServiceBuilder
{
    private readonly IServiceCollection _services;

    /// <summary>
    ///     Initializes a new instance of the <see cref="NPipelineServiceBuilder" /> class.
    /// </summary>
    /// <param name="services">The service collection to register NPipeline services into.</param>
    public NPipelineServiceBuilder(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
    }

    /// <summary>
    ///     Registers a node type for dependency injection.
    /// </summary>
    /// <typeparam name="TNode">The node type to register.</typeparam>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddNode<TNode>() where TNode : class, INode
    {
        _services.TryAddTransient<TNode>();
        return this;
    }

    /// <summary>
    ///     Registers a node type with a specific service lifetime.
    /// </summary>
    /// <typeparam name="TNode">The node type to register.</typeparam>
    /// <param name="lifetime">The lifetime of the service.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddNode<TNode>(ServiceLifetime lifetime) where TNode : class, INode
    {
        var descriptor = new ServiceDescriptor(typeof(TNode), typeof(TNode), lifetime);
        _services.TryAdd(descriptor);
        return this;
    }

    /// <summary>
    ///     Registers a pipeline definition type for dependency injection.
    /// </summary>
    /// <typeparam name="TPipeline">The pipeline definition type to register.</typeparam>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddPipeline<TPipeline>() where TPipeline : class, IPipelineDefinition
    {
        _services.TryAddTransient<TPipeline>();
        return this;
    }

    /// <summary>
    ///     Registers a pipeline definition type with a specific service lifetime.
    /// </summary>
    /// <typeparam name="TPipeline">The pipeline definition type to register.</typeparam>
    /// <param name="lifetime">The lifetime of the service.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddPipeline<TPipeline>(ServiceLifetime lifetime)
        where TPipeline : class, IPipelineDefinition
    {
        var descriptor = new ServiceDescriptor(typeof(TPipeline), typeof(TPipeline), lifetime);
        _services.TryAdd(descriptor);
        return this;
    }

    /// <summary>
    ///     Registers an error handler type for dependency injection.
    /// </summary>
    /// <typeparam name="THandler">The error handler type to register.</typeparam>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddErrorHandler<THandler>() where THandler : class, INodeErrorHandler
    {
        _services.TryAddTransient<THandler>();
        return this;
    }

    /// <summary>
    ///     Registers an error handler type with a specific service lifetime.
    /// </summary>
    /// <typeparam name="THandler">The error handler type to register.</typeparam>
    /// <param name="lifetime">The lifetime of the service.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddErrorHandler<THandler>(ServiceLifetime lifetime)
        where THandler : class, INodeErrorHandler
    {
        var descriptor = new ServiceDescriptor(typeof(THandler), typeof(THandler), lifetime);
        _services.TryAdd(descriptor);
        return this;
    }

    /// <summary>
    ///     Registers a pipeline error handler type for dependency injection.
    /// </summary>
    /// <typeparam name="THandler">The pipeline error handler type to register.</typeparam>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddPipelineErrorHandler<THandler>() where THandler : class, IPipelineErrorHandler
    {
        _services.TryAddTransient<THandler>();
        return this;
    }

    /// <summary>
    ///     Registers a pipeline error handler type with a specific service lifetime.
    /// </summary>
    /// <typeparam name="THandler">The pipeline error handler type to register.</typeparam>
    /// <param name="lifetime">The lifetime of the service.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddPipelineErrorHandler<THandler>(ServiceLifetime lifetime)
        where THandler : class, IPipelineErrorHandler
    {
        var descriptor = new ServiceDescriptor(typeof(THandler), typeof(THandler), lifetime);
        _services.TryAdd(descriptor);
        return this;
    }

    /// <summary>
    ///     Registers a dead letter sink type for dependency injection.
    /// </summary>
    /// <typeparam name="TSink">The dead letter sink type to register.</typeparam>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddDeadLetterSink<TSink>() where TSink : class, IDeadLetterSink
    {
        _services.TryAddTransient<TSink>();
        return this;
    }

    /// <summary>
    ///     Registers a dead letter sink type with a specific service lifetime.
    /// </summary>
    /// <typeparam name="TSink">The dead letter sink type to register.</typeparam>
    /// <param name="lifetime">The lifetime of the service.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddDeadLetterSink<TSink>(ServiceLifetime lifetime)
        where TSink : class, IDeadLetterSink
    {
        var descriptor = new ServiceDescriptor(typeof(TSink), typeof(TSink), lifetime);
        _services.TryAdd(descriptor);
        return this;
    }

    /// <summary>
    ///     Registers a lineage sink type for dependency injection.
    /// </summary>
    /// <typeparam name="TSink">The lineage sink type to register.</typeparam>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddLineageSink<TSink>() where TSink : class, ILineageSink
    {
        _services.TryAddTransient<TSink>();
        return this;
    }

    /// <summary>
    ///     Registers a lineage sink type with a specific service lifetime.
    /// </summary>
    /// <typeparam name="TSink">The lineage sink type to register.</typeparam>
    /// <param name="lifetime">The lifetime of the service.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddLineageSink<TSink>(ServiceLifetime lifetime) where TSink : class, ILineageSink
    {
        var descriptor = new ServiceDescriptor(typeof(TSink), typeof(TSink), lifetime);
        _services.TryAdd(descriptor);
        return this;
    }

    /// <summary>
    ///     Registers a pipeline lineage sink type for dependency injection.
    /// </summary>
    /// <typeparam name="TSink">The pipeline lineage sink type to register.</typeparam>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddPipelineLineageSink<TSink>() where TSink : class, IPipelineLineageSink
    {
        _services.TryAddTransient<TSink>();
        return this;
    }

    /// <summary>
    ///     Registers a pipeline lineage sink type with a specific service lifetime.
    /// </summary>
    /// <typeparam name="TSink">The pipeline lineage sink type to register.</typeparam>
    /// <param name="lifetime">The lifetime of the service.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddPipelineLineageSink<TSink>(ServiceLifetime lifetime)
        where TSink : class, IPipelineLineageSink
    {
        var descriptor = new ServiceDescriptor(typeof(TSink), typeof(TSink), lifetime);
        _services.TryAdd(descriptor);
        return this;
    }

    /// <summary>
    ///     Registers a pipeline lineage sink provider type for dependency injection.
    /// </summary>
    /// <typeparam name="TProvider">The pipeline lineage sink provider type to register.</typeparam>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddLineageSinkProvider<TProvider>()
        where TProvider : class, IPipelineLineageSinkProvider
    {
        _services.TryAddTransient<TProvider>();
#pragma warning disable CA2263
        _services.TryAddTransient(typeof(IPipelineLineageSinkProvider), typeof(TProvider));
#pragma warning restore CA2263
        return this;
    }

    /// <summary>
    ///     Registers a pipeline lineage sink provider type with a specific service lifetime.
    /// </summary>
    /// <typeparam name="TProvider">The pipeline lineage sink provider type to register.</typeparam>
    /// <param name="lifetime">The lifetime of the service.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder AddLineageSinkProvider<TProvider>(ServiceLifetime lifetime)
        where TProvider : class, IPipelineLineageSinkProvider
    {
        var descriptor = new ServiceDescriptor(typeof(TProvider), typeof(TProvider), lifetime);
        _services.TryAdd(descriptor);

        var providerDescriptor = new ServiceDescriptor(typeof(IPipelineLineageSinkProvider), typeof(TProvider), lifetime);
        _services.TryAdd(providerDescriptor);

        return this;
    }

    /// <summary>
    ///     Scans the specified assemblies for NPipeline components and registers them automatically.
    /// </summary>
    /// <param name="assemblies">The assemblies to scan for components.</param>
    /// <returns>The current builder instance for fluent chaining.</returns>
    public NPipelineServiceBuilder ScanAssemblies(params Assembly[] assemblies)
    {
        ArgumentNullException.ThrowIfNull(assemblies);

        // Register Nodes and Definitions (safe assembly scanning)
        var typesToRegister = assemblies
            .SelectMany(GetLoadableTypes)
            .Where(t => t is { IsClass: true, IsAbstract: false } &&
                        (typeof(INode).IsAssignableFrom(t) ||
                         typeof(IPipelineDefinition).IsAssignableFrom(t) ||
                         typeof(INodeErrorHandler).IsAssignableFrom(t) ||
                         typeof(IPipelineErrorHandler).IsAssignableFrom(t) ||
                         typeof(IDeadLetterSink).IsAssignableFrom(t) ||
                         typeof(ILineageSink).IsAssignableFrom(t) ||
                         typeof(IPipelineLineageSink).IsAssignableFrom(t) ||
                         typeof(IPipelineLineageSinkProvider).IsAssignableFrom(t)))
            .Distinct();

        foreach (var type in typesToRegister)
        {
            _services.TryAddTransient(type);

            // If the discovered type implements IPipelineLineageSinkProvider, also register it against the interface
            // so it can be resolved by the runner via the focused factory interfaces without reflection.
            if (typeof(IPipelineLineageSinkProvider).IsAssignableFrom(type))
            {
#pragma warning disable CA2263
                _services.TryAddTransient(typeof(IPipelineLineageSinkProvider), type);
#pragma warning restore CA2263
            }
        }

        return this;
    }

    private static IEnumerable<Type> GetLoadableTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(t => t is not null)!;
        }
    }
}
