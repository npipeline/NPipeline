namespace NPipeline.Observability;

/// <summary>
///     Interface for automatic observability metrics collection during node execution.
/// </summary>
/// <remarks>
///     <para>
///         This interface allows the core pipeline runner to manage observability scopes
///         without depending on the NPipeline.Extensions.Observability implementation.
///     </para>
///     <para>
///         Implementations are provided by the observability extension package
///         and created dynamically when nodes have observability configured.
///     </para>
/// </remarks>
public interface IAutoObservabilityScope : IDisposable
{
    /// <summary>
    ///     Records count of items processed and emitted by the node.
    /// </summary>
    /// <param name="processed">The number of items processed (received as input).</param>
    /// <param name="emitted">The number of items emitted (sent as output).</param>
    void RecordItemCount(long processed, long emitted);

    /// <summary>
    ///     Increments count of items processed by one.
    /// </summary>
    void IncrementProcessed();

    /// <summary>
    ///     Increments count of items emitted by one.
    /// </summary>
    void IncrementEmitted();

    /// <summary>
    ///     Records that the node execution failed with an exception.
    /// </summary>
    /// <param name="exception">The exception that caused failure.</param>
    void RecordFailure(Exception exception);
}