using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Represents a node that joins two input streams into a single output stream.
///     Join nodes process a combined stream of items from multiple sources.
/// </summary>
public interface IJoinNode : INode
{
    /// <summary>
    ///     Processes a combined input stream and returns a joined output stream.
    ///     The input stream contains items from multiple upstream sources interleaved.
    ///     When item-level lineage is enabled, implementations should propagate lineage by emitting LineagePacket{TOut}.
    /// </summary>
    /// <param name="inputStream">The combined input stream.</param>
    /// <param name="context">Pipeline context (provides CurrentNodeId, lineage flags, etc.).</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>An asynchronous stream of joined output items (as object?).</returns>
    ValueTask<IAsyncEnumerable<object?>> ExecuteAsync(
        IAsyncEnumerable<object?> inputStream,
        PipelineContext context,
        CancellationToken cancellationToken = default);
}
