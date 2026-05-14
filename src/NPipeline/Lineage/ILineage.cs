using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Graph;
using NPipeline.Graph.PipelineDelegates;
using NPipeline.Pipeline;

namespace NPipeline.Lineage;

/// <summary>
///     Unified lineage contract spanning build-time adapter creation, item-level lineage handling,
///     and pipeline-level report recording.
/// </summary>
public interface ILineage
{
    /// <summary>
    ///     Indicates whether this lineage module supports item-level lineage end to end.
    /// </summary>
    bool SupportsItemLevelLineage { get; }

    /// <summary>
    ///     Builds a lineage adapter delegate that transforms lineage data from one type to another.
    /// </summary>
    /// <typeparam name="TIn">The input type for the lineage data.</typeparam>
    /// <typeparam name="TOut">The output type for the lineage data.</typeparam>
    /// <param name="lineageMapperType">The optional type of the lineage mapper to use.</param>
    /// <returns>A lineage adapter delegate, or null if no adapter can be built.</returns>
    LineageAdapterDelegate? BuildLineageAdapter<TIn, TOut>(Type? lineageMapperType);

    /// <summary>
    ///     Builds a delegate for unwrapping lineage data at sink nodes.
    /// </summary>
    /// <typeparam name="TIn">The input type of lineage data to unwrap.</typeparam>
    /// <returns>A sink lineage unwrap delegate, or null if no delegate can be built.</returns>
    SinkLineageUnwrapDelegate? BuildSinkLineageUnwrapDelegate<TIn>();

    /// <summary>
    ///     Builds a lineage adapter delegate for runtime node input/output types.
    /// </summary>
    /// <param name="inType">The input payload type for the node.</param>
    /// <param name="outType">The output payload type for the node.</param>
    /// <param name="lineageMapperType">Optional mapper type declared on the node.</param>
    /// <returns>A lineage adapter delegate, or null when no adapter is available.</returns>
    LineageAdapterDelegate? BuildLineageAdapter(Type? inType, Type? outType, Type? lineageMapperType);

    /// <summary>
    ///     Builds a sink lineage unwrap delegate for a runtime sink input type.
    /// </summary>
    /// <param name="inType">The sink input payload type.</param>
    /// <returns>A sink lineage unwrap delegate, or null when no delegate is available.</returns>
    SinkLineageUnwrapDelegate? BuildSinkLineageUnwrap(Type? inType);

    /// <summary>
    ///     Wraps the output of a source node with an initial lineage packet.
    /// </summary>
    IDataStream WrapSourceStream(IDataStream sourcePipe, string nodeId, Guid pipelineId, string? pipelineName, LineageOptions? options);

    /// <summary>
    ///     Unwraps a stream of lineage packets to extract the raw data.
    /// </summary>
    IAsyncEnumerable<object> UnwrapLineageStream(IAsyncEnumerable<object?> source, CancellationToken ct = default);

    /// <summary>
    ///     Prepares an input stream for node execution while preserving lineage context.
    /// </summary>
    (IDataStream unwrappedInput, IAsyncEnumerable<object?> inputLineageContext) PrepareInputWithLineageContext(
        IDataStream source,
        CancellationToken ct = default);

    /// <summary>
    ///     Wraps the output of a join or aggregate node with lineage information.
    /// </summary>
    IDataStream WrapNodeOutput(IDataStream output, string currentNodeId, Guid pipelineId, string? pipelineName, LineageOptions? options,
        LineageOutcomeReason outcome, CancellationToken ct = default);

    /// <summary>
    ///     Wraps node output using upstream lineage context.
    /// </summary>
    IDataStream WrapNodeOutputFromInputLineage(
        IDataStream output,
        IAsyncEnumerable<object?> inputLineageContext,
        string currentNodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? options,
        LineageOutcomeReason outcome,
        Type? lineageMapperType = null,
        CancellationToken ct = default);

    /// <summary>
    ///     Records pipeline-level lineage for a completed run when supported.
    /// </summary>
    /// <param name="definitionType">The pipeline definition type.</param>
    /// <param name="graph">The effective runtime graph for the run.</param>
    /// <param name="context">The pipeline context for the run.</param>
    /// <param name="pipelineLineageSink">Resolved pipeline lineage sink for this run.</param>
    /// <returns>A task that completes when recording is finished.</returns>
    Task RecordPipelineAsync(Type definitionType, PipelineGraph graph, PipelineContext context, IPipelineLineageSink? pipelineLineageSink);
}