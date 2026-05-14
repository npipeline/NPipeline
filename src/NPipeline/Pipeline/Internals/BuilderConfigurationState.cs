using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Resilience;
using NPipeline.Visualization;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Encapsulates configuration-related state in the PipelineBuilder.
///     This internal class groups all error handling, lineage, and observability configuration.
/// </summary>
internal sealed class BuilderConfigurationState
{
    /// <summary>
    ///     Unified resilience policy instance.
    /// </summary>
    public IResiliencePolicy? ResiliencePolicy { get; set; }

    /// <summary>
    ///     Unified resilience policy type (for lazy initialization).
    /// </summary>
    public Type? ResiliencePolicyType { get; set; }

    /// <summary>
    ///     Dead letter sink instance for failed items.
    /// </summary>
    public IDeadLetterSink? DeadLetterSink { get; set; }

    /// <summary>
    ///     Dead letter sink type (for lazy initialization).
    /// </summary>
    public Type? DeadLetterSinkType { get; set; }

    /// <summary>
    ///     Lineage sink instance for tracking data lineage.
    /// </summary>
    public ILineageSink? LineageSink { get; set; }

    /// <summary>
    ///     Lineage sink type (for lazy initialization).
    /// </summary>
    public Type? LineageSinkType { get; set; }

    /// <summary>
    ///     Pipeline-level lineage sink instance.
    /// </summary>
    public IPipelineLineageSink? PipelineLineageSink { get; set; }

    /// <summary>
    ///     Pipeline-level lineage sink type (for lazy initialization).
    /// </summary>
    public Type? PipelineLineageSinkType { get; set; }

    /// <summary>
    ///     Pipeline visualizer instance for graph visualization.
    /// </summary>
    public IPipelineVisualizer? Visualizer { get; set; }

    /// <summary>
    ///     Global execution observer for monitoring pipeline execution.
    /// </summary>
    public object? GlobalExecutionObserver { get; set; }

    /// <summary>
    ///     Clears all configuration state.
    /// </summary>
    public void Clear()
    {
        ResiliencePolicy = null;
        ResiliencePolicyType = null;
        DeadLetterSink = null;
        DeadLetterSinkType = null;
        LineageSink = null;
        LineageSinkType = null;
        PipelineLineageSink = null;
        PipelineLineageSinkType = null;
        Visualizer = null;
        GlobalExecutionObserver = null;
    }
}
