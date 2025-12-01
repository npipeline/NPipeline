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
            observabilitySurface);
    }
}
