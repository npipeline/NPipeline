using NPipeline.Configuration;
using NPipeline.Extensions.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Observability;

/// <summary>
///     Factory for creating pipeline contexts with observability pre-configured.
/// </summary>
/// <remarks>
///     This factory creates <see cref="PipelineContext" /> instances that are automatically
///     wired up with <see cref="MetricsCollectingExecutionObserver" /> to enable automatic
///     metrics collection during pipeline execution.
/// </remarks>
public interface IObservablePipelineContextFactory
{
    /// <summary>
    ///     Creates a new pipeline context with observability pre-configured.
    /// </summary>
    /// <param name="cancellationToken">Optional cancellation token for the pipeline execution.</param>
    /// <returns>A pipeline context with the execution observer set for metrics collection.</returns>
    PipelineContext Create(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a new pipeline context from existing configuration with observability added.
    /// </summary>
    /// <param name="configuration">The base pipeline context configuration.</param>
    /// <returns>A pipeline context with the execution observer set for metrics collection.</returns>
    PipelineContext Create(PipelineContextConfiguration configuration);
}
