using NPipeline.Graph;

namespace NPipeline.Lineage;

/// <summary>
///     A factory for creating instances of lineage-related components.
/// </summary>
public interface ILineageFactory
{
    /// <summary>
    ///     Creates an instance of the specified lineage sink type.
    /// </summary>
    /// <param name="sinkType">The type of the lineage sink to create.</param>
    /// <returns>An instance of <see cref="ILineageSink" />, or null if it cannot be created.</returns>
    ILineageSink? CreateLineageSink(Type sinkType);

    /// <summary>
    ///     Creates an instance of the specified pipeline lineage sink type (explicit configuration path).
    /// </summary>
    /// <remarks>
    ///     Use this when a concrete sink type is explicitly configured via the builder or context. This is an imperative,
    ///     unambiguous request to construct that sink (typically via DI and falling back to ActivatorUtilities).
    /// </remarks>
    /// <param name="sinkType">The type of the pipeline lineage sink to create.</param>
    /// <returns>An instance of <see cref="IPipelineLineageSink" />, or null if it cannot be created.</returns>
    IPipelineLineageSink? CreatePipelineLineageSink(Type sinkType);

    /// <summary>
    ///     Resolves an optional provider capable of supplying a default pipeline lineage sink (implicit default path).
    /// </summary>
    /// <remarks>
    ///     This is consulted only when no explicit sink (instance or type) is configured and item-level lineage is enabled.
    ///     It allows optional packages (e.g., NPipeline.Lineage) to supply a sensible default without reflection.
    ///     Returns null when no provider is registered or available.
    /// </remarks>
    /// <returns>An <see cref="IPipelineLineageSinkProvider" /> instance or null.</returns>
    IPipelineLineageSinkProvider? ResolvePipelineLineageSinkProvider();

    /// <summary>
    ///     Resolves an optional lineage collector for tracking data lineage.
    /// </summary>
    /// <returns>An <see cref="ILineageCollector" /> instance or null if lineage is not enabled.</returns>
    ILineageCollector? ResolveLineageCollector();

    /// <summary>
    ///     Creates a lineage report for a pipeline run.
    /// </summary>
    /// <param name="pipelineName">The name of the pipeline.</param>
    /// <param name="pipelineId">The pipeline identifier.</param>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="runId">The run identifier.</param>
    /// <returns>A <see cref="PipelineLineageReport" />, or null if lineage reporting is not available.</returns>
    PipelineLineageReport? CreateLineageReport(
        string pipelineName, Guid pipelineId, PipelineGraph graph, Guid runId);
}
