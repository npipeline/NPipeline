using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;

namespace NPipeline.Execution.Services;

/// <summary>
///     Implements concatenate merge strategy for merging multiple data streams.
/// </summary>
public sealed class ConcatenateMergeStrategy<T> : IMergeStrategy<T>
{
    /// <inheritdoc />
    public IDataPipe<T> Merge(IEnumerable<IDataPipe<T>> pipes, CancellationToken cancellationToken)
    {
        List<IDataPipe<T>> typedPipes = [..pipes];
        var concatenatedStream = ConcatenateStreams(typedPipes, cancellationToken);
        return new StreamingDataPipe<T>(concatenatedStream, "ConcatenatedStream");
    }

    private static async IAsyncEnumerable<T> ConcatenateStreams(
        IReadOnlyList<IDataPipe<T>> dataPipes,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var pipe in dataPipes)
        await foreach (var item in pipe.WithCancellation(cancellationToken))
        {
            yield return item;
        }
    }
}
