using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Lineage;

namespace NPipeline.Execution;

/// <summary>
///     Provides services for managing item-level lineage throughout a pipeline execution.
/// </summary>
public interface ILineageService
{
    /// <summary>
    ///     Wraps the output of a source node with an initial lineage packet.
    /// </summary>
    /// <param name="sourcePipe">The original data pipe from the source.</param>
    /// <param name="nodeId">The ID of the source node.</param>
    /// <param name="pipelineId">The unique pipeline identity for this execution context.</param>
    /// <param name="pipelineName">The logical pipeline name for this execution context.</param>
    /// <param name="options">The lineage options for the pipeline.</param>
    /// <returns>A new data pipe containing items wrapped in <see cref="LineagePacket{T}" />.</returns>
    IDataStream WrapSourceStream(IDataStream sourcePipe, string nodeId, Guid pipelineId, string? pipelineName, LineageOptions? options);

    /// <summary>
    ///     Unwraps a stream of lineage packets to extract the raw data.
    /// </summary>
    /// <param name="source">The source enumerable of lineage packets.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>An asynchronous enumerable of the unwrapped data objects.</returns>
    IAsyncEnumerable<object> UnwrapLineageStream(IAsyncEnumerable<object?> source, CancellationToken ct = default);

    /// <summary>
    ///     Prepares an input stream for node execution while preserving original lineage packet context.
    /// </summary>
    /// <param name="source">The lineage-wrapped source stream.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>
    ///     A tuple containing:
    ///     - an unwrapped stream for node execution
    ///     - a stream of input context objects used for output lineage mapping
    /// </returns>
    (IDataStream unwrappedInput, IAsyncEnumerable<object?> inputLineageContext) PrepareInputWithLineageContext(
        IDataStream source,
        CancellationToken ct = default);

    /// <summary>
    ///     Wraps the output of a join or aggregate node with a new lineage packet.
    /// </summary>
    /// <param name="output">The output stream from the node.</param>
    /// <param name="currentNodeId">The ID of the current (join or aggregate) node.</param>
    /// <param name="pipelineId">The unique pipeline identity for this execution context.</param>
    /// <param name="pipelineName">The logical pipeline name for this execution context.</param>
    /// <param name="options">The lineage options for the pipeline.</param>
    /// <param name="outcome">The outcome of the operation (e.g., Joined, Aggregated).</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>A new data pipe containing the wrapped output items.</returns>
    IDataStream WrapNodeOutput(IDataStream output, string currentNodeId, Guid pipelineId, string? pipelineName, LineageOptions? options,
        LineageOutcomeReason outcome, CancellationToken ct = default);

    /// <summary>
    ///     Wraps node output using upstream lineage context rather than minting fresh lineage by default.
    /// </summary>
    /// <param name="output">The output stream from the node.</param>
    /// <param name="inputLineageContext">Input lineage context captured from the node's original input stream.</param>
    /// <param name="currentNodeId">The ID of the current node.</param>
    /// <param name="pipelineId">The unique pipeline identity for this execution context.</param>
    /// <param name="pipelineName">The logical pipeline name for this execution context.</param>
    /// <param name="options">The lineage options for the pipeline.</param>
    /// <param name="outcome">The outcome of the operation (e.g., Joined, Aggregated).</param>
    /// <param name="lineageMapperType">Optional lineage mapper type declared for the current node.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>A new data pipe containing lineage-wrapped output items mapped from upstream lineage context.</returns>
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
}
