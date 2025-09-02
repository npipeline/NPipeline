namespace NPipeline.Nodes;

/// <summary>
///     Represents a node that performs aggregation operations on streams of data.
///     Aggregate nodes process entire streams rather than individual items.
/// </summary>
public interface IAggregateNode : INode
{
    /// <summary>
    ///     Processes a stream of data and returns the aggregated result.
    /// </summary>
    /// <param name="inputStream">The input stream to aggregate.</param>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    /// <returns>The aggregated result.</returns>
    ValueTask<object?> ExecuteAsync(
        IAsyncEnumerable<object?> inputStream,
        CancellationToken cancellationToken = default);
}
