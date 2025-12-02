using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A sink node that collects all received items into an in-memory list for inspection.
/// </summary>
/// <typeparam name="T">The type of data to be collected.</typeparam>
public class InMemorySinkNode<T> : SinkNode<T>
{
    private readonly TaskCompletionSource<IReadOnlyList<T>> _completionSource =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly List<T> _items = new();

    /// <summary>
    ///     Gets the list of items that have been received by the sink.
    /// </summary>
    public IReadOnlyList<T> Items => _items;

    /// <summary>
    ///     A task that completes when the sink has finished processing all items.
    ///     The result of the task is a snapshot of the items received.
    /// </summary>
    public Task<IReadOnlyList<T>> Completion => _completionSource.Task;

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Validate parameters early to provide clear ArgumentNullException as expected by tests
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(context);

        // Register this sink in the context before processing items
        RegisterInContext(context);

        // If cancellation was already requested, ensure we surface OperationCanceledException
        if (cancellationToken.IsCancellationRequested)
        {
            var oce = new OperationCanceledException(cancellationToken);
            _completionSource.TrySetException(oce);
            throw oce;
        }

        try
        {
            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _items.Add(item);
            }

            // Snapshot to avoid external mutation after completion
            _completionSource.TrySetResult(_items.ToArray());
        }
        catch (OperationCanceledException)
        {
            // Ensure the completion TCS contains an OperationCanceledException so awaiting it throws the exact type
            var oce = new OperationCanceledException(cancellationToken);
            _completionSource.TrySetException(oce);
            throw oce;
        }
        catch (Exception ex)
        {
            _completionSource.TrySetException(ex);
            throw;
        }
    }

    /// <summary>
    ///     Registers this sink node in the specified pipeline context for testing purposes.
    ///     This allows the sink to be retrieved later for assertions in test code.
    /// </summary>
    /// <param name="context">The pipeline context to register this sink in.</param>
    /// <remarks>
    ///     The sink is registered using both its own type and the data type T as keys,
    ///     enabling flexible retrieval in test scenarios. If this is a sub-pipeline,
    ///     the sink is also registered in the parent context.
    /// </remarks>
    public void RegisterInContext(PipelineContext context)
    {
        // Register in current context
        context.Items[typeof(InMemorySinkNode<T>).FullName!] = this;
        context.Items[typeof(T).FullName!] = this;

        // If this is a sub-pipeline in a composite scenario, also register in the parent context
        if (context.Items.TryGetValue(PipelineContextKeys.TestingParentContext, out var parentContextObj) &&
            parentContextObj is PipelineContext parentContext)
        {
            parentContext.Items[typeof(InMemorySinkNode<T>).FullName!] = this;
            parentContext.Items[typeof(T).FullName!] = this;
        }
    }
}
