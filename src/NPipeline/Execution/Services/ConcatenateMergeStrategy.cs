using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Execution.Services;

/// <summary>
///     Implements concatenate merge strategy for merging multiple data streams.
/// </summary>
public sealed class ConcatenateMergeStrategy<T> : IMergeStrategy<T>
{
    /// <inheritdoc />
    public IDataStream<T> Merge(IEnumerable<IDataStream<T>> pipes, CancellationToken cancellationToken)
    {
        List<IDataStream<T>> typedPipes = [..pipes];
        var concatenatedStream = ConcatenateStreams(typedPipes, cancellationToken);
        return new DataStream<T>(concatenatedStream, "ConcatenatedStream");
    }

    private static async IAsyncEnumerable<T> ConcatenateStreams(
        IReadOnlyList<IDataStream<T>> dataPipes,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var pipe in dataPipes)
        await foreach (var item in pipe.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }
}
