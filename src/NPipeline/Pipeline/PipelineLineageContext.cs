using NPipeline.Execution.Lineage;
using NPipeline.Lineage;

namespace NPipeline.Pipeline;

/// <summary>
///     Lineage services and resolved sinks for a pipeline run.
/// </summary>
public sealed class PipelineLineageContext
{
    internal PipelineLineageContext(ILineageFactory lineageFactory)
    {
        LineageFactory = lineageFactory;
    }

    /// <summary>
    ///     The factory for creating lineage-related components.
    /// </summary>
    public ILineageFactory LineageFactory { get; }

    /// <summary>
    ///     The lineage module driving this run.
    /// </summary>
    /// <remarks>
    ///     Set by the runner from its own <see cref="ILineage" /> before the pipeline is built, so that build-time
    ///     lineage adapters and runtime lineage handling always come from the same instance. Defaults to
    ///     <see cref="NullLineage.Instance" /> for a context that is not being run by a runner.
    /// </remarks>
    public ILineage Module { get; internal set; } = NullLineage.Instance;

    /// <summary>
    ///     Item-level lineage sink resolved for the current run.
    /// </summary>
    public ILineageSink? LineageSink { get; internal set; }

    /// <summary>
    ///     Pipeline-level lineage sink resolved for the current run.
    /// </summary>
    public IPipelineLineageSink? PipelineLineageSink { get; internal set; }

    /// <summary>
    ///     Item-level lineage collector resolved for the current run.
    /// </summary>
    public ILineageCollector? LineageCollector { get; internal set; }

    /// <summary>
    ///     Per-node item lineage state for the current run. Replaced at the start of each run, so a context reused for
    ///     several runs never mixes their state.
    /// </summary>
    internal LineageNodeOutcomeRegistry Outcomes { get; private set; } = new();

    /// <summary>
    ///     Starts a fresh outcome registry for a new run.
    /// </summary>
    internal void ResetOutcomes() => Outcomes = new LineageNodeOutcomeRegistry();
}
