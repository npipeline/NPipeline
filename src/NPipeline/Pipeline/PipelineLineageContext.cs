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
}
