using System.Runtime.CompilerServices;

namespace NPipeline.DataFlow.DataPipes;

/// <summary>
///     Base class for decorating data pipes with cross-cutting concerns (counting, tracing, etc).
/// </summary>
/// <typeparam name="T">The type of data flowing through the pipe.</typeparam>
public abstract class DataPipeBase<T> : IDataPipe<T>, IStreamingDataPipe
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="DataPipeBase{T}" /> class.
    /// </summary>
    /// <param name="inner">The inner data pipe to decorate.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="inner" /> is null.</exception>
    protected DataPipeBase(IDataPipe<T> inner)
    {
        Inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    /// <summary>
    ///     The inner data pipe being decorated.
    /// </summary>
    protected IDataPipe<T> Inner { get; }

    /// <summary>
    ///     Gets the name of the stream. By default, returns the inner pipe's stream name.
    /// </summary>
    public virtual string StreamName => Inner.StreamName;

    /// <summary>
    ///     Gets the data type of the pipe.
    /// </summary>
    /// <returns>The type of data flowing through the pipe.</returns>
    public virtual Type GetDataType()
    {
        return typeof(T);
    }

    /// <summary>
    ///     Internal method that converts the pipe to a non-generic async enumerable.
    ///     This provides efficient non-generic access for internal framework code.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable of objects.</returns>
    public virtual async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in this.WithCancellation(cancellationToken))
        {
            if (item is not null)
                yield return item;
        }
    }

    /// <summary>
    ///     Asynchronously disposes of the data pipe and its inner pipe.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public virtual async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        if (Inner is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Gets the asynchronous enumerator that iterates through the pipe.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An asynchronous enumerator for the pipe.</returns>
    public abstract IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default);
}
