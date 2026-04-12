using System.Collections.Concurrent;
using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     A dead-letter sink that stores failed items in a bounded in-memory queue.
///     If the queue reaches capacity, it will throw an exception to fail the pipeline, preventing memory overflow.
/// </summary>
/// <remarks>
///     <para>
///         This implementation provides a memory-bounded dead-letter queue that prevents unbounded memory growth
///         by enforcing a strict capacity limit. When the capacity is reached, the sink throws an
///         <see cref="InvalidOperationException" /> to fail the pipeline immediately.
///     </para>
///     <para>
///         The default capacity of 1000 items was chosen based on typical production workloads where:
///         - It provides sufficient buffer for transient error spikes
///         - Memory usage remains reasonable (approximately 8-16MB for most error objects)
///         - It's large enough to allow investigation without overwhelming the system
///     </para>
///     <para>
///         Consider adjusting the capacity when:
///         - Processing high-volume data streams where errors are more frequent
///         - Running in memory-constrained environments (decrease capacity)
///         - Storing large error objects (decrease capacity)
///         - Implementing automated error recovery workflows (increase capacity)
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Use default capacity of 1000
/// var defaultSink = new BoundedInMemoryDeadLetterSink();
/// 
/// // Use custom capacity for high-volume scenarios
/// var highVolumeSink = new BoundedInMemoryDeadLetterSink(5000);
/// 
/// // Use reduced capacity for memory-constrained environments
/// var constrainedSink = new BoundedInMemoryDeadLetterSink(100);
///     </code>
/// </example>
public class BoundedInMemoryDeadLetterSink : IDeadLetterSink
{
    private readonly int _capacity;
    private readonly object _lock = new();
    private readonly ConcurrentQueue<DeadLetterEnvelope> _queue = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="BoundedInMemoryDeadLetterSink" /> class.
    /// </summary>
    /// <param name="capacity">
    ///     The maximum number of items to store in the queue.
    ///     Defaults to 1000, which provides a balance between memory usage and error retention capacity.
    ///     Must be greater than 0.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when capacity is less than or equal to 0.</exception>
    public BoundedInMemoryDeadLetterSink(int capacity = 1000)
    {
        if (capacity <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be greater than 0.");

        _capacity = capacity;
    }

    /// <summary>
    ///     Gets the items that have been collected in the dead-letter sink.
    /// </summary>
    /// <value>
    ///     A read-only collection of envelopes containing the failed item, exception, and failure attribution.
    ///     The collection size will never exceed the configured capacity.
    /// </value>
    public IReadOnlyCollection<DeadLetterEnvelope> Items => _queue;

    /// <inheritdoc />
    public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_queue.Count >= _capacity)
            {
                throw new InvalidOperationException(
                    $"Dead Letter Queue has exceeded its capacity of {_capacity}. Failing pipeline to prevent memory overflow.");
            }

            _queue.Enqueue(envelope);
        }

        return Task.CompletedTask;
    }
}
