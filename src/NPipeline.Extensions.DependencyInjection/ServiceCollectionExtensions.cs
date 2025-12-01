using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.DependencyInjection;

/// <summary>
///     Provides extension methods for setting up NPipeline services in an <see cref="IServiceCollection" />.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds NPipeline services and configures them using a fluent API.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="configure">A delegate to configure the NPipeline services.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipeline(this IServiceCollection services,
        Action<NPipelineServiceBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // Register Core Services (per-run scoped where appropriate)
        RegisterCoreServices(services);

        // Configure using the fluent builder
        var builder = new NPipelineServiceBuilder(services);
        configure(builder);

        return services;
    }

    /// <summary>
    ///     Scans the specified assemblies for pipeline components and registers them with the service collection.
    /// </summary>
    /// <param name="services">The <see cref="IServiceCollection" /> to add the services to.</param>
    /// <param name="assembliesToScan">The assemblies to scan for nodes and definitions.</param>
    /// <returns>The <see cref="IServiceCollection" /> so that additional calls can be chained.</returns>
    public static IServiceCollection AddNPipeline(this IServiceCollection services, params Assembly[] assembliesToScan)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(assembliesToScan);

        // Register Core Services (per-run scoped where appropriate)
        RegisterCoreServices(services);

        // Register Nodes and Definitions (safe assembly scanning)
        var typesToRegister = assembliesToScan
            .SelectMany(GetLoadableTypes)
            .Where(t => t is { IsClass: true, IsAbstract: false } &&
                        (typeof(INode).IsAssignableFrom(t) ||
                         typeof(IPipelineDefinition).IsAssignableFrom(t) ||
                         typeof(INodeErrorHandler).IsAssignableFrom(t) ||
                         typeof(IPipelineErrorHandler).IsAssignableFrom(t) ||
                         typeof(IDeadLetterSink).IsAssignableFrom(t) ||
                         typeof(ILineageSink).IsAssignableFrom(t) ||
                         typeof(IPipelineLineageSink).IsAssignableFrom(t) ||
                         typeof(IPipelineLineageSinkProvider).IsAssignableFrom(t)));

        foreach (var type in typesToRegister)
        {
            services.TryAddTransient(type);

            // If the discovered type implements IPipelineLineageSinkProvider, also register it against the interface
            // so it can be resolved by the runner via the focused factory interfaces without reflection.
            if (typeof(IPipelineLineageSinkProvider).IsAssignableFrom(type))
                services.TryAddTransient(typeof(IPipelineLineageSinkProvider), type);
        }

        return services;
    }

    /// <summary>
    ///     Registers the core NPipeline services that are required for all configurations.
    /// </summary>
    private static void RegisterCoreServices(IServiceCollection services)
    {
        // Register Core Services (per-run scoped where appropriate)
        services.TryAddTransient<PipelineBuilder>();
        services.TryAddSingleton<IPipelineFactory, PipelineFactory>();
        services.TryAddScoped<INodeFactory, DiContainerNodeFactory>();
        services.TryAddScoped<IPipelineRunner, PipelineRunner>();

        // Register the focused factory interfaces with the same implementation
        services.TryAddScoped<IErrorHandlerFactory, DiHandlerFactory>();
        services.TryAddScoped<ILineageFactory, DiHandlerFactory>();
        services.TryAddScoped<IObservabilityFactory, DiHandlerFactory>();

        // Core execution/observability/persistence services required by the primary PipelineRunner ctor.
        // Without these registrations the container falls back to the parameterless ctor which injects DefaultNodeFactory
        // causing MissingMethodException for nodes with DI-only constructors (ConcurrentQueue<>, etc.).
        services.TryAddScoped<ICountingService, CountingService>();
        services.TryAddScoped<IMergeStrategySelector, MergeStrategySelector>();
        services.TryAddScoped<IPipeMergeService>(sp => new PipeMergeService(sp.GetRequiredService<IMergeStrategySelector>()));
        services.TryAddScoped<ILineageService, LineageService>();
        services.TryAddScoped<IBranchService, BranchService>();
        services.TryAddScoped<INodeExecutor, NodeExecutor>();
        services.TryAddScoped<IExecutionAnnotationsService, ExecutionAnnotationsService>();
        services.TryAddScoped<ITopologyService, TopologyService>();
        services.TryAddScoped<INodeInstantiationService, NodeInstantiationService>();
        services.TryAddScoped<IPipelineExecutionCoordinator, PipelineExecutionCoordinator>();
        services.TryAddSingleton<IErrorHandlingService>(ErrorHandlingService.Instance);
        services.TryAddSingleton<IPersistenceService>(PersistenceService.Instance);
        services.TryAddScoped<IPipelineInfrastructureService, PipelineInfrastructureService>();
        services.TryAddScoped<IObservabilitySurface, ObservabilitySurface>();
    }

    /// <summary>
    ///     Runs the specified pipeline definition.
    /// </summary>
    /// <typeparam name="TDefinition">The type of the pipeline definition to run.</typeparam>
    /// <param name="serviceProvider">The service provider to resolve the runner from.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    public static Task RunPipelineAsync<TDefinition>(this IServiceProvider serviceProvider, CancellationToken cancellationToken = default)
        where TDefinition : IPipelineDefinition, new()
    {
        return serviceProvider.RunPipelineAsync<TDefinition>(null, cancellationToken);
    }

    /// <summary>
    ///     Runs the specified pipeline definition with the given parameters.
    /// </summary>
    /// <typeparam name="TDefinition">The type of the pipeline definition to run.</typeparam>
    /// <param name="serviceProvider">The service provider to resolve the runner from.</param>
    /// <param name="parameters">A dictionary of parameters to pass to the pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    public static async Task RunPipelineAsync<TDefinition>(this IServiceProvider serviceProvider, Dictionary<string, object>? parameters,
        CancellationToken cancellationToken = default)
        where TDefinition : IPipelineDefinition, new()
    {
        await using var scope = serviceProvider.CreateAsyncScope();
        var sp = scope.ServiceProvider;

        var runner = sp.GetRequiredService<IPipelineRunner>();
        var errorHandlerFactory = sp.GetRequiredService<IErrorHandlerFactory>();
        var lineageFactory = sp.GetRequiredService<ILineageFactory>();
        var observabilityFactory = sp.GetRequiredService<IObservabilityFactory>();

        var config = new PipelineContextConfiguration(
            parameters,
            ErrorHandlerFactory: errorHandlerFactory,
            LineageFactory: lineageFactory,
            ObservabilityFactory: observabilityFactory,
            CancellationToken: cancellationToken);

        var context = new PipelineContext(config);

        // Indicate DI owns node disposal to avoid double-dispose in runner.
        context.Items[PipelineContextKeys.DiOwnedNodes] = true;

        await runner.RunAsync<TDefinition>(context).ConfigureAwait(false);
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
