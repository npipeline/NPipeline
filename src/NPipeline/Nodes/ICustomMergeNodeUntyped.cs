using NPipeline.DataFlow;

namespace NPipeline.Nodes;

/// <summary>
///     Non-generic abstraction enabling custom merge nodes to be invoked without reflection.
/// </summary>
public interface ICustomMergeNodeUntyped : INode
{
    Task<IDataPipe> MergeAsyncUntyped(IEnumerable<IDataPipe> pipes, CancellationToken cancellationToken);
}
