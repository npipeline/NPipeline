using NPipeline.DataFlow;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Execution;

/// <summary>
///     Service for merging data pipes using typed strategies without reflection.
/// </summary>
public interface IPipeMergeService
{
    /// <summary>
    ///     Merges input pipes according to the node definition's merge strategy.
    /// </summary>
    /// <param name="nodeDef">The node definition requiring merging.</param>
    /// <param name="nodeInstance">The instantiated node instance for custom merge operations.</param>
    /// <param name="inputPipes">The input data pipes to be merged.</param>
    /// <param name="mergeCapacity">The maximum number of items a buffering strategy may buffer, or null to use the default.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation, containing the merged data pipe.</returns>
    Task<IDataStream> MergeAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        IEnumerable<IDataStream> inputPipes,
        int? mergeCapacity,
        CancellationToken cancellationToken = default);
}
