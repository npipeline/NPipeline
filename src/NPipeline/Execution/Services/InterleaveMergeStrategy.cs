using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Execution.Services;

/// <summary>
///     Implements interleave merge strategy for merging multiple data streams.
/// </summary>
public sealed class InterleaveMergeStrategy<T> : IMergeStrategy<T>
{
    /// <inheritdoc />
    public IDataStream<T> Merge(IEnumerable<IDataStream<T>> pipes, CancellationToken cancellationToken) =>
        Merge(pipes, null, cancellationToken);

    /// <inheritdoc />
    public IDataStream<T> Merge(IEnumerable<IDataStream<T>> pipes, int? capacity, CancellationToken cancellationToken)
    {
        var typedPipes = pipes as IReadOnlyList<IDataStream<T>> ?? pipes.ToList();
        return new DataStream<T>(StreamInterleaver.Interleave(typedPipes, capacity, cancellationToken), "InterleavedStream");
    }
}
