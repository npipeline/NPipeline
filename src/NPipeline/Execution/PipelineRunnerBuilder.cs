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
    private IErrorHandlingService? _errorHandlingService;
    private IPipelineExecutionPlanCache? _executionPlanCache;
    private INodeFactory? _nodeFactory;
    private INodeExecutor? _nodeExecutor;
    private INodeInstantiationService? _nodeInstantiationService;
    private IObservabilitySurface? _observabilitySurface;
    private IPersistenceService? _persistenceService;
    private IPipelineFactory? _pipelineFactory;
    private IRuntimePipelineBinder? _runtimePipelineBinder;
    private ITopologyService? _topologyService;

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
    ///     Sets the node executor.
    /// </summary>
    public PipelineRunnerBuilder WithNodeExecutor(INodeExecutor nodeExecutor)
    {
        _nodeExecutor = nodeExecutor;
        return this;
    }

    /// <summary>
    ///     Sets the topology service.
    /// </summary>
    public PipelineRunnerBuilder WithTopologyService(ITopologyService topologyService)
    {
        _topologyService = topologyService;
        return this;
    }

    /// <summary>
    ///     Sets the node instantiation service.
    /// </summary>
    public PipelineRunnerBuilder WithNodeInstantiationService(INodeInstantiationService nodeInstantiationService)
    {
        _nodeInstantiationService = nodeInstantiationService;
        return this;
    }

    /// <summary>
    ///     Sets the error handling service.
    /// </summary>
    public PipelineRunnerBuilder WithErrorHandlingService(IErrorHandlingService errorHandlingService)
    {
        _errorHandlingService = errorHandlingService;
        return this;
    }

    /// <summary>
    ///     Sets the persistence service.
    /// </summary>
    public PipelineRunnerBuilder WithPersistenceService(IPersistenceService persistenceService)
    {
        _persistenceService = persistenceService;
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
    ///     Sets the runtime pipeline binder.
    /// </summary>
    public PipelineRunnerBuilder WithRuntimePipelineBinder(IRuntimePipelineBinder runtimePipelineBinder)
    {
        _runtimePipelineBinder = runtimePipelineBinder;
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

        var nodeExecutor = _nodeExecutor ?? new NodeExecutor(
            NullLineageService.Instance,
            new PipeMergeService(new MergeStrategySelector()),
            new DataStreamWrapperService());

        var topologyService = _topologyService ?? new TopologyService();
        var nodeInstantiationService = _nodeInstantiationService ?? new NodeInstantiationService();
        var errorHandlingService = _errorHandlingService ?? new ErrorHandlingService();
        var persistenceService = _persistenceService ?? new PersistenceService();
        var runtimePipelineBinder = _runtimePipelineBinder ?? RuntimePipelineBinder.Instance;

        var observabilitySurface = _observabilitySurface ?? NullObservabilitySurface.Instance;

        return new PipelineRunner(
            pipelineFactory,
            nodeFactory,
            nodeExecutor,
            topologyService,
            nodeInstantiationService,
            errorHandlingService,
            persistenceService,
            observabilitySurface,
            _executionPlanCache,
            runtimePipelineBinder);
    }
}
