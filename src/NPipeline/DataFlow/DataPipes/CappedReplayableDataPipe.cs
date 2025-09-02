using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.DataPipes;

internal sealed class CappedReplayableDataPipe<T> : DataPipeBase<T>
{
    private readonly List<T> _buffer;
    private readonly int? _cap;
    private bool _sourceDrained;

    public CappedReplayableDataPipe(IDataPipe<T> source, int? cap, string name) : base(source)
    {
        _cap = cap;
        StreamName = name;

        _buffer = cap is { } c and > 0
            ? new List<T>(c)
            : [];
    }

    public override string StreamName { get; }

    public override IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return WithCancellation(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    public async IAsyncEnumerable<T> WithCancellation([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // First replay any already buffered items
        for (var i = 0; i < _buffer.Count; i++)
        {
            yield return _buffer[i];
        }

        if (_sourceDrained)
            yield break;

        // Continue consuming underlying source lazily, buffering until drained or cap exceeded
        await foreach (var item in Inner.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (_cap is not null && _buffer.Count >= _cap)
                throw new InvalidOperationException($"Resilience materialization exceeded MaxMaterializedItems={_cap}.");

            _buffer.Add(item);
            yield return item;
        }

        _sourceDrained = true;
    }

    // IDataPipe<T>
    public IAsyncEnumerable<T> ToAsyncEnumerableTyped()
    {
        // Intentionally not propagating a caller token here; explicit None clarifies choice per CA2016 guidance.
        return WithCancellation(CancellationToken.None);
    }
}
