namespace NPipeline.DataFlow;

/// <summary>
///     A marker interface to identify data pipes that stream their data and may not be replayable.
/// </summary>
public interface IForwardOnlyDataStream : IDataStream
{
}

/// <summary>
///     A strongly-typed marker interface to identify data pipes that stream their data and may not be replayable.
/// </summary>
/// <typeparam name="T">The type of the data being carried through the pipe.</typeparam>
/// <remarks>
///     <para>
///         Streaming data pipes provide lazy evaluation of data without buffering.
///         They are not replayable - once consumed, the data is gone.
///         This interface combines <see cref="IDataStream{T}" /> with the streaming marker.
///     </para>
/// </remarks>
public interface IForwardOnlyDataStream<out T> : IDataStream<T>, IForwardOnlyDataStream
{
}
