using Microsoft.Extensions.Logging;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Observability;
using NPipeline.Observability.Tracing;

namespace NPipeline.Pipeline;

/// <summary>
///     Observability surface for logging, tracing, metrics, and execution observation.
/// </summary>
public sealed class PipelineObservabilityContext
{
    private IExecutionObserver _executionObserver = NullExecutionObserver.Instance;

    internal PipelineObservabilityContext(
        ILoggerFactory loggerFactory,
        IPipelineTracer tracer,
        IObservabilityFactory observabilityFactory)
    {
        LoggerFactory = loggerFactory;
        Tracer = tracer;
        ObservabilityFactory = observabilityFactory;
        ProcessedItemsCounter = new StatsCounter();
    }

    /// <summary>
    ///     The logger factory for this pipeline run.
    /// </summary>
    public ILoggerFactory LoggerFactory { get; }

    /// <summary>
    ///     The tracer for this pipeline run.
    /// </summary>
    public IPipelineTracer Tracer { get; }

    /// <summary>
    ///     The factory for resolving observability-related components.
    /// </summary>
    public IObservabilityFactory ObservabilityFactory { get; }

    /// <summary>
    ///     Framework-managed processed items counter for the current run.
    /// </summary>
    public StatsCounter ProcessedItemsCounter { get; internal set; }

    /// <summary>
    ///     Execution observer for instrumentation.
    /// </summary>
    public IExecutionObserver ExecutionObserver
    {
        get => _executionObserver;
        set => _executionObserver = value ?? NullExecutionObserver.Instance;
    }
}
