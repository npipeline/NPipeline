using NPipeline.DataFlow;

namespace NPipeline.Execution;

/// <summary>
///     Defines a strategy for merging multiple data pipes of the same type.
/// </summary>
/// <typeparam name="T">The type of data to merge.</typeparam>
public interface IMergeStrategy<T>
{
    /// <summary>
    ///     Merges multiple data pipes into a single data pipe using the specific strategy.
    /// </summary>
    /// <param name="pipes">The data pipes to merge.</param>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>A merged data pipe containing all items from the input pipes.</returns>
    IDataPipe<T> Merge(IEnumerable<IDataPipe<T>> pipes, CancellationToken cancellationToken);
}
