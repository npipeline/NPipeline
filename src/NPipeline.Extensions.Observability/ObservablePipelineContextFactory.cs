using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Observability;

/// <summary>
///     Factory for creating pipeline contexts with observability pre-configured.
/// </summary>
/// <remarks>
///     This factory creates <see cref="PipelineContext" /> instances that are automatically
///     wired up with <see cref="MetricsCollectingExecutionObserver" /> to enable automatic
///     metrics collection during pipeline execution.
/// </remarks>
public sealed class ObservablePipelineContextFactory : IObservablePipelineContextFactory
{
    private readonly IExecutionObserver _executionObserver;
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ObservablePipelineContextFactory" /> class.
    /// </summary>
    /// <param name="serviceProvider">The service provider for resolving dependencies.</param>
    /// <param name="executionObserver">The execution observer to use for the context.</param>
    public ObservablePipelineContextFactory(IServiceProvider serviceProvider, IExecutionObserver executionObserver)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _executionObserver = executionObserver ?? throw new ArgumentNullException(nameof(executionObserver));
    }

    /// <inheritdoc />
    public PipelineContext Create(CancellationToken cancellationToken = default)
    {
        var config = cancellationToken == default
            ? PipelineContextConfiguration.Default
            : PipelineContextConfiguration.WithCancellation(cancellationToken);

        return Create(config);
    }

    /// <inheritdoc />
    public PipelineContext Create(PipelineContextConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // Resolve observability factory from the current scoped service provider
        // This ensures we get the correct scoped instance of IObservabilityCollector
        var observabilityFactory = _serviceProvider.GetService<IObservabilityFactory>();

        // Create a new configuration with the observability factory
        var configWithObservability = configuration with
        {
            ObservabilityFactory = observabilityFactory ?? new DiObservabilityFactory(_serviceProvider),
        };

        var context = new PipelineContext(configWithObservability)
        {
            ExecutionObserver = _executionObserver,
        };

        return context;
    }
}
