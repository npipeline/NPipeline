using NPipeline.DataFlow;

namespace NPipeline.Nodes;

/// <summary>
///     Non-generic abstraction enabling custom merge nodes to be invoked without reflection.
/// </summary>
public interface ICustomMergeNodeUntyped : INode
{
    /// <summary>
    ///     Merges multiple input data pipes into a single output pipe without type constraints.
    /// </summary>
    /// <param name="pipes">The input data pipes to merge.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A data pipe containing the merged output stream.</returns>
    Task<IDataPipe> MergeAsyncUntyped(IEnumerable<IDataPipe> pipes, CancellationToken cancellationToken);
}
