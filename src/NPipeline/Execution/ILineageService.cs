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
    /// <param name="options">The lineage options for the pipeline.</param>
    /// <returns>A new data pipe containing items wrapped in <see cref="LineagePacket{T}" />.</returns>
    IDataPipe WrapSourceStream(IDataPipe sourcePipe, string nodeId, LineageOptions? options);

    /// <summary>
    ///     Unwraps a stream of lineage packets to extract the raw data.
    /// </summary>
    /// <param name="source">The source enumerable of lineage packets.</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>An asynchronous enumerable of the unwrapped data objects.</returns>
    IAsyncEnumerable<object> UnwrapLineageStream(IAsyncEnumerable<object?> source, CancellationToken ct = default);

    /// <summary>
    ///     Wraps the output of a join or aggregate node with a new lineage packet.
    /// </summary>
    /// <param name="output">The output stream from the node.</param>
    /// <param name="currentNodeId">The ID of the current (join or aggregate) node.</param>
    /// <param name="options">The lineage options for the pipeline.</param>
    /// <param name="outcome">The outcome of the operation (e.g., Joined, Aggregated).</param>
    /// <param name="ct">A cancellation token.</param>
    /// <returns>A new data pipe containing the wrapped output items.</returns>
    IDataPipe WrapNodeOutput(IDataPipe output, string currentNodeId, LineageOptions? options, HopDecisionFlags outcome, CancellationToken ct = default);
}
