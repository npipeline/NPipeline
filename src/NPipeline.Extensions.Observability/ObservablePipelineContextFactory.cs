using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Extensions.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Observability;

/// <summary>
///     Factory for creating pipeline contexts with observability pre-configured.
/// </summary>
/// <remarks>
///     This factory creates <see cref="PipelineContext"/> instances that are automatically
///     wired up with <see cref="MetricsCollectingExecutionObserver"/> to enable automatic
///     metrics collection during pipeline execution.
/// </remarks>
public sealed class ObservablePipelineContextFactory : IObservablePipelineContextFactory
{
    private readonly IExecutionObserver _executionObserver;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ObservablePipelineContextFactory"/> class.
    /// </summary>
    /// <param name="executionObserver">The execution observer for metrics collection.</param>
    public ObservablePipelineContextFactory(IExecutionObserver executionObserver)
    {
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

        var context = new PipelineContext(configuration)
        {
            ExecutionObserver = _executionObserver
        };

        return context;
    }
}
