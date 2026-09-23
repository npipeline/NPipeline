using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.DataStreams;

internal sealed class CappedReplayableDataStream<T> : DataStreamBase<T>
{
    private readonly List<T> _buffer;
    private readonly int? _cap;
    private bool _sourceDrained;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CappedReplayableDataStream{T}" /> class.
    /// </summary>
    /// <param name="source">The source data pipe to wrap.</param>
    /// <param name="cap">The maximum number of items to buffer. When null, buffers all items.</param>
    /// <param name="name">The name of this data pipe for identification and tracing purposes.</param>
    public CappedReplayableDataStream(IDataStream<T> source, int? cap, string name) : base(source)
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
        return Replay(cancellationToken).GetAsyncEnumerator(cancellationToken);
    }

    /// <summary>
    ///     Replays what has already been buffered, then keeps consuming and buffering the source.
    /// </summary>
    /// <remarks>
    ///     Deliberately not named <c>WithCancellation</c>: an instance method of that name beats the BCL extension in
    ///     overload resolution, so identical-looking call sites would bind to different things depending on the static
    ///     type of the variable.
    /// </remarks>
    private async IAsyncEnumerable<T> Replay([EnumeratorCancellation] CancellationToken cancellationToken)
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
                throw new InvalidOperationException($"Resilience materialization exceeded NodeRestart.MaxReplayWindow={_cap}.");

            _buffer.Add(item);
            yield return item;
        }

        _sourceDrained = true;
    }

    // IDataStream<T>
    public IAsyncEnumerable<T> ToAsyncEnumerableTyped()
    {
        // Intentionally not propagating a caller token here; explicit None clarifies choice per CA2016 guidance.
        return Replay(CancellationToken.None);
    }
}
