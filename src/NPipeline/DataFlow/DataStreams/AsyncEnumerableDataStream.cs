using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.DataStreams;

/// <summary>
///     Simple adapter that wraps an IAsyncEnumerable&lt;T&gt; to implement IDataStream&lt;T&gt;.
/// </summary>
internal sealed class AsyncEnumerableDataStream<T>(IAsyncEnumerable<T> source, string streamName) : IForwardOnlyDataStream<T>
{
    public string StreamName { get; } = streamName;

    public Type GetDataType() => typeof(T);

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) => source.GetAsyncEnumerator(cancellationToken);

    public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (source is IAsyncDisposable disposable)
            await disposable.DisposeAsync().ConfigureAwait(false);
    }
}
