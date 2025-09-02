using System.Collections.Concurrent;
using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     A dead-letter sink that stores failed items in a bounded in-memory queue.
///     If the queue reaches capacity, it will throw an exception to fail the pipeline, preventing memory overflow.
/// </summary>
public class BoundedInMemoryDeadLetterSink : IDeadLetterSink
{
    private readonly int _capacity;
    private readonly object _lock = new();
    private readonly ConcurrentQueue<(object Item, Exception Error)> _queue = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="BoundedInMemoryDeadLetterSink" /> class.
    /// </summary>
    /// <param name="capacity">The maximum number of items to store in the queue. Defaults to 1000.</param>
    public BoundedInMemoryDeadLetterSink(int capacity = 1000)
    {
        _capacity = capacity;
    }

    /// <summary>
    ///     Gets the items that have been collected in the dead-letter sink.
    /// </summary>
    public IReadOnlyCollection<(object Item, Exception Error)> Items => _queue;

    /// <inheritdoc />
    public Task HandleAsync(string nodeId, object item, Exception error, PipelineContext context, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            if (_queue.Count >= _capacity)
            {
                throw new InvalidOperationException(
                    $"Dead Letter Queue has exceeded its capacity of {_capacity}. Failing pipeline to prevent memory overflow.");
            }

            _queue.Enqueue((item, error));
        }

        return Task.CompletedTask;
    }
}
