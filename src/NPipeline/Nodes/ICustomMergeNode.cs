using NPipeline.DataFlow;

namespace NPipeline.Nodes;

/// <summary>
///     Represents a node that provides custom logic for merging multiple input streams.
///     When a node implements this interface, the runner will
///     invoke <see cref="MergeAsync" /> instead of using a predefined merge strategy.
/// </summary>
/// <typeparam name="TIn">The type of data in the input streams.</typeparam>
public interface ICustomMergeNode<TIn> : INode
{
    /// <summary>
    ///     Asynchronously merges multiple input data pipes into a single data pipe.
    /// </summary>
    /// <param name="pipes">The collection of input data pipes to merge.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous merge operation, resulting in a single <see cref="IDataPipe{T}" />.</returns>
    Task<IDataPipe<TIn>> MergeAsync(IEnumerable<IDataPipe> pipes, CancellationToken cancellationToken);
}
