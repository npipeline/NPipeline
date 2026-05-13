using System.Runtime.CompilerServices;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Lineage;

namespace NPipeline.Execution.Services;

/// <summary>
/// A null implementation of <see cref="ILineageService"/> that does not perform any lineage tracking.
/// </summary>
public sealed class NullLineageService : ILineageService
{
    /// <summary>
    /// Gets the singleton instance of <see cref="NullLineageService"/>.
    /// </summary>
    public static readonly NullLineageService Instance = new();

    /// <summary>
    /// Wraps a source data stream with lineage tracking (no-op in null implementation).
    /// </summary>
    /// <param name="sourcePipe">The source data stream to wrap.</param>
    /// <param name="nodeId">The ID of the source node.</param>
    /// <param name="pipelineId">The ID of the pipeline.</param>
    /// <param name="pipelineName">The optional name of the pipeline.</param>
    /// <param name="options">The optional lineage options.</param>
    /// <returns>The unwrapped source data stream.</returns>
    public IDataStream WrapSourceStream(IDataStream sourcePipe, string nodeId, Guid pipelineId, string? pipelineName, LineageOptions? options)
        => sourcePipe;

    /// <summary>
    /// Unwraps lineage information from a data stream (no-op in null implementation).
    /// </summary>
    /// <param name="source">The source data stream containing lineage information.</param>
    /// <param name="ct">The cancellation token.</param>
    /// <returns>An async enumerable of unwrapped data items.</returns>
    public async IAsyncEnumerable<object> UnwrapLineageStream(IAsyncEnumerable<object?> source, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
            yield return item!;
    }

    /// <summary>
    /// Prepares input data with lineage context for processing (no-op in null implementation).
    /// </summary>
    /// <param name="source">The source data stream.</param>
    /// <param name="ct">The cancellation token.</param>
    /// <returns>A tuple containing the unwrapped input and the lineage context.</returns>
    public (IDataStream unwrappedInput, IAsyncEnumerable<object?> inputLineageContext) PrepareInputWithLineageContext(
        IDataStream source, CancellationToken ct = default)
        => (source, source.ToAsyncEnumerable(ct));

    /// <summary>
    /// Wraps node output with lineage tracking (no-op in null implementation).
    /// </summary>
    /// <param name="output">The output data stream from the node.</param>
    /// <param name="currentNodeId">The ID of the current node.</param>
    /// <param name="pipelineId">The ID of the pipeline.</param>
    /// <param name="pipelineName">The optional name of the pipeline.</param>
    /// <param name="options">The optional lineage options.</param>
    /// <param name="outcome">The outcome reason of the node execution.</param>
    /// <param name="ct">The cancellation token.</param>
    /// <returns>The unwrapped output data stream.</returns>
    public IDataStream WrapNodeOutput(IDataStream output, string currentNodeId, Guid pipelineId, string? pipelineName,
        LineageOptions? options, LineageOutcomeReason outcome, CancellationToken ct = default)
        => output;

    /// <summary>
    /// Wraps node output with lineage tracking derived from input lineage (no-op in null implementation).
    /// </summary>
    /// <param name="output">The output data stream from the node.</param>
    /// <param name="inputLineageContext">The lineage context from the input.</param>
    /// <param name="currentNodeId">The ID of the current node.</param>
    /// <param name="pipelineId">The ID of the pipeline.</param>
    /// <param name="pipelineName">The optional name of the pipeline.</param>
    /// <param name="options">The optional lineage options.</param>
    /// <param name="outcome">The outcome reason of the node execution.</param>
    /// <param name="lineageMapperType">The optional lineage mapper type.</param>
    /// <param name="ct">The cancellation token.</param>
    /// <returns>The unwrapped output data stream.</returns>
    public IDataStream WrapNodeOutputFromInputLineage(IDataStream output, IAsyncEnumerable<object?> inputLineageContext,
        string currentNodeId, Guid pipelineId, string? pipelineName, LineageOptions? options, LineageOutcomeReason outcome,
        Type? lineageMapperType = null, CancellationToken ct = default)
        => output;
}
