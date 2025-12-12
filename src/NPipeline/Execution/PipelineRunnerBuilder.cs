using NPipeline.Execution.Caching;
using NPipeline.Execution.Factories;
using NPipeline.Execution.Services;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Builder for creating <see cref="PipelineRunner" /> instances with custom dependencies.
/// </summary>
public sealed class PipelineRunnerBuilder
{
    private IPipelineExecutionCoordinator? _executionCoordinator;
    private IPipelineExecutionPlanCache? _executionPlanCache;
    private IPipelineInfrastructureService? _infrastructureService;
    private INodeFactory? _nodeFactory;
    private IObservabilitySurface? _observabilitySurface;
    private IPipelineFactory? _pipelineFactory;

    /// <summary>
    ///     Sets the pipeline factory.
    /// </summary>
    public PipelineRunnerBuilder WithPipelineFactory(IPipelineFactory pipelineFactory)
    {
        _pipelineFactory = pipelineFactory;
        return this;
    }

    /// <summary>
    ///     Sets the node factory.
    /// </summary>
    public PipelineRunnerBuilder WithNodeFactory(INodeFactory nodeFactory)
    {
        _nodeFactory = nodeFactory;
        return this;
    }

    /// <summary>
    ///     Sets the execution coordinator.
    /// </summary>
    public PipelineRunnerBuilder WithExecutionCoordinator(IPipelineExecutionCoordinator executionCoordinator)
    {
        _executionCoordinator = executionCoordinator;
        return this;
    }

    /// <summary>
    ///     Sets the infrastructure service.
    /// </summary>
    public PipelineRunnerBuilder WithInfrastructureService(IPipelineInfrastructureService infrastructureService)
    {
        _infrastructureService = infrastructureService;
        return this;
    }

    /// <summary>
    ///     Sets the observability surface.
    /// </summary>
    public PipelineRunnerBuilder WithObservabilitySurface(IObservabilitySurface observabilitySurface)
    {
        _observabilitySurface = observabilitySurface;
        return this;
    }

    /// <summary>
    ///     Sets the execution plan cache. Use <see cref="NullPipelineExecutionPlanCache.Instance" /> to disable caching.
    /// </summary>
    /// <param name="executionPlanCache">The cache implementation to use, or null to use the default in-memory cache.</param>
    /// <returns>This builder instance for method chaining.</returns>
    public PipelineRunnerBuilder WithExecutionPlanCache(IPipelineExecutionPlanCache? executionPlanCache)
    {
        _executionPlanCache = executionPlanCache;
        return this;
    }

    /// <summary>
    ///     Disables execution plan caching by setting the cache to a null implementation.
    /// </summary>
    /// <returns>This builder instance for method chaining.</returns>
    /// <remarks>
    ///     When caching is disabled, execution plans will be rebuilt on every pipeline run.
    ///     This may be useful for testing or when node behavior changes frequently.
    /// </remarks>
    public PipelineRunnerBuilder WithoutExecutionPlanCache()
    {
        _executionPlanCache = NullPipelineExecutionPlanCache.Instance;
        return this;
    }

    /// <summary>
    ///     Builds the <see cref="PipelineRunner" /> instance.
    /// </summary>
    public PipelineRunner Build()
    {
        var pipelineFactory = _pipelineFactory ?? new PipelineFactory();
        var nodeFactory = _nodeFactory ?? new DefaultNodeFactory();

        var executionCoordinator = _executionCoordinator ?? new PipelineExecutionCoordinator(
            new NodeExecutor(
                new LineageService(),
                new CountingService(),
                new PipeMergeService(new MergeStrategySelector()),
                new BranchService()),
            new TopologyService(),
            new NodeInstantiationService());

        var infrastructureService = _infrastructureService ?? new PipelineInfrastructureService(
            ErrorHandlingService.Instance,
            PersistenceService.Instance);

        var observabilitySurface = _observabilitySurface ?? new ObservabilitySurface();

        return new PipelineRunner(
            pipelineFactory,
            nodeFactory,
            executionCoordinator,
            infrastructureService,
            observabilitySurface,
            _executionPlanCache);
    }
}
