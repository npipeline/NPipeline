using NPipeline.ErrorHandling;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution;

/// <summary>
///     Binds runtime execution services and runtime graph overrides for a pipeline run.
/// </summary>
public interface IRuntimePipelineBinder
{
    /// <summary>
    ///     Applies runtime graph overrides and resolves runtime handlers/sinks.
    /// </summary>
    /// <param name="graph">Pipeline graph for the run.</param>
    /// <param name="context">Pipeline execution context.</param>
    /// <returns>The effective graph and resolved runtime bindings.</returns>
    Task<RuntimePipelineBindingResult> BindAsync(PipelineGraph graph, PipelineContext context);
}

/// <summary>
///     Result of runtime binding for a single pipeline run.
/// </summary>
/// <param name="Graph">Effective graph after runtime overrides are applied.</param>
/// <param name="DeadLetterSink">Resolved dead-letter sink (if any).</param>
/// <param name="LineageSink">Resolved item-level lineage sink (if any).</param>
/// <param name="PipelineLineageSink">Resolved pipeline-level lineage sink (if any).</param>
/// <param name="ResiliencePolicy">Resolved resilience policy for execution.</param>
/// <param name="LineageCollector">Resolved item-level lineage collector (if any).</param>
/// <param name="RunOwnedInstances">
///     Instances the binder created from a configured type and the run owns: the run disposes them when it ends,
///     however it ends. Instances passed in directly, or owned by a container, are not included.
/// </param>
public readonly record struct RuntimePipelineBindingResult(
    PipelineGraph Graph,
    IDeadLetterSink? DeadLetterSink,
    ILineageSink? LineageSink,
    IPipelineLineageSink? PipelineLineageSink,
    IResiliencePolicy ResiliencePolicy,
    ILineageCollector? LineageCollector = null,
    IReadOnlyList<object>? RunOwnedInstances = null);
