using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A general-purpose node that fans out data to multiple downstream pathways for parallel processing.
///     This allows data to be sent to multiple destinations simultaneously without affecting the main data flow.
///     For sink-specific monitoring, use <see cref="TapNode{T}" /> instead.
/// </summary>
/// <typeparam name="T">The type of data being processed.</typeparam>
public sealed class BranchNode<T> : TransformNode<T, T>
{
    private readonly List<Func<T, Task>> _outputHandlers = [];
    private readonly object _syncLock = new();

    /// <summary>
    ///     Adds an output pathway to this branch node.
    /// </summary>
    /// <param name="outputHandler">An async function that processes the data item.</param>
    public void AddOutput(Func<T, Task> outputHandler)
    {
        lock (_syncLock)
        {
            _outputHandlers.Add(outputHandler);
        }
    }

    /// <inheritdoc />
    public override async Task<T> ExecuteAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Fan out to all output handlers in parallel
        List<Func<T, Task>> handlers;

        lock (_syncLock)
        {
            handlers = _outputHandlers.ToList();
        }

        if (handlers.Count > 0)
        {
            var branchTasks = handlers.Select(handler =>
                BranchHandlerAsync(handler, item, context)).ToArray();

            await Task.WhenAll(branchTasks).ConfigureAwait(false);
        }

        // Return the original item unchanged to the main pipeline
        return item;
    }

    private async Task BranchHandlerAsync(Func<T, Task> handler, T item, PipelineContext context)
    {
        var branchActivity = context.Tracer.StartActivity($"Branch_{context.CurrentNodeId}");

        try
        {
            try
            {
                await handler(item).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                var logger = context.LoggerFactory.CreateLogger(typeof(BranchNode<T>).FullName ?? typeof(BranchNode<T>).Name);

                logger.Log(LogLevel.Warning, ex,
                    $"Exception in branch handler for node '{context.CurrentNodeId}'. Exception was swallowed to avoid impacting pipeline flow.");
            }
        }
        finally
        {
            branchActivity.Dispose();
        }
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
