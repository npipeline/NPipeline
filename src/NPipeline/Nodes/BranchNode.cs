using NPipeline.ErrorHandling;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A general-purpose node that fans out data to multiple downstream pathways for parallel processing.
///     This allows data to be sent to multiple destinations simultaneously without affecting the main data flow.
///     For sink-specific monitoring, use <see cref="TapNode{T}" /> instead.
/// </summary>
/// <typeparam name="T">The type of data being processed.</typeparam>
/// <remarks>
///     <para>
///         Branch handlers are user-provided delegates that execute in parallel. When a branch handler throws an exception,
///         the behavior depends on the <see cref="ErrorHandlingMode" /> setting:
///     </para>
///     <list type="bullet">
///         <item>
///             <description>
///                 <see cref="BranchErrorHandlingMode.RouteToErrorHandler" /> (default): Exceptions are wrapped in
///                 <see cref="BranchHandlerException" /> and routed through the pipeline's error handling system.
///             </description>
///         </item>
///         <item>
///             <description>
///                 <see cref="BranchErrorHandlingMode.CollectAndThrow" />: All branch exceptions are collected and thrown
///                 as an <see cref="AggregateException" /> after all branches complete.
///             </description>
///         </item>
///         <item>
///             <description>
///                 <see cref="BranchErrorHandlingMode.LogAndContinue" />: Exceptions are logged but swallowed,
///                 allowing the pipeline to continue. Use with caution.
///             </description>
///         </item>
///     </list>
/// </remarks>
public sealed class BranchNode<T> : TransformNode<T, T>
{
    private readonly List<Func<T, Task>> _outputHandlers = [];
    private readonly object _syncLock = new();
    private List<Func<T, Task>>? _cachedHandlers;
    private bool _handlersFinalized;

    /// <summary>
    ///     Gets or sets the error handling mode for branch handler exceptions.
    ///     Defaults to <see cref="BranchErrorHandlingMode.RouteToErrorHandler" />.
    /// </summary>
    public BranchErrorHandlingMode ErrorHandlingMode { get; set; } = BranchErrorHandlingMode.RouteToErrorHandler;

    /// <summary>
    ///     Adds an output pathway to this branch node.
    /// </summary>
    /// <param name="outputHandler">An async function that processes the data item.</param>
    public void AddOutput(Func<T, Task> outputHandler)
    {
        lock (_syncLock)
        {
            if (_handlersFinalized)
                throw new InvalidOperationException("Cannot add handlers after execution has begun.");

            _outputHandlers.Add(outputHandler);
        }
    }

    /// <inheritdoc />
    public override async Task<T> ExecuteAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Lazily cache handlers on first execute - handlers list is finalized after this point
        List<Func<T, Task>> handlers;

        lock (_syncLock)
        {
            if (!_handlersFinalized)
            {
                _cachedHandlers = new List<Func<T, Task>>(_outputHandlers);
                _handlersFinalized = true;
            }

            handlers = _cachedHandlers!;
        }

        if (handlers.Count > 0)
        {
            if (ErrorHandlingMode == BranchErrorHandlingMode.CollectAndThrow)
            {
                // For CollectAndThrow, we need to collect all exceptions and throw them as an AggregateException
                await ExecuteWithCollectedExceptionsAsync(handlers, item, context, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var branchTasks = handlers.Select((handler, index) =>
                    BranchHandlerAsync(handler, index, item, context, cancellationToken)).ToArray();

                await Task.WhenAll(branchTasks).ConfigureAwait(false);
            }
        }

        // Return the original item unchanged to the main pipeline
        return item;
    }

    private async Task ExecuteWithCollectedExceptionsAsync(
        List<Func<T, Task>> handlers,
        T item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var exceptions = new List<BranchHandlerException>();
        var syncLock = new object();

        var branchTasks = handlers.Select(async (handler, index) =>
        {
            var branchActivity = context.Tracer.StartActivity($"Branch_{context.CurrentNodeId}_{index}");

            try
            {
                try
                {
                    await handler(item).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    var branchException = new BranchHandlerException(context.CurrentNodeId, index, item, ex);

                    lock (syncLock)
                    {
                        exceptions.Add(branchException);
                    }
                }
            }
            finally
            {
                branchActivity.Dispose();
            }
        }).ToArray();

        await Task.WhenAll(branchTasks).ConfigureAwait(false);

        if (exceptions.Count > 0)
            throw new AggregateException("One or more branch handlers failed.", exceptions);
    }

    private async Task BranchHandlerAsync(
        Func<T, Task> handler,
        int branchIndex,
        T item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var branchActivity = context.Tracer.StartActivity($"Branch_{context.CurrentNodeId}_{branchIndex}");

        try
        {
            try
            {
                await handler(item).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                await HandleBranchExceptionAsync(ex, branchIndex, item, context, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            branchActivity.Dispose();
        }
    }

    private async Task HandleBranchExceptionAsync(
        Exception ex,
        int branchIndex,
        T item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var branchException = new BranchHandlerException(context.CurrentNodeId, branchIndex, item, ex);

        switch (ErrorHandlingMode)
        {
            case BranchErrorHandlingMode.RouteToErrorHandler:
                await RouteToPipelineErrorHandlerAsync(branchException, context, cancellationToken).ConfigureAwait(false);
                break;

            case BranchErrorHandlingMode.CollectAndThrow:
                // This should not be reached - CollectAndThrow is handled in ExecuteWithCollectedExceptionsAsync
                throw branchException;

            case BranchErrorHandlingMode.LogAndContinue:
                LogBranchException(branchException, context);
                break;

            default:
                // This should never happen with a valid enum value
                LogBranchException(branchException, context, $"Unknown error handling mode: {ErrorHandlingMode}. Treating as LogAndContinue.");
                break;
        }
    }

    private async Task RouteToPipelineErrorHandlerAsync(
        BranchHandlerException branchException,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (context.PipelineErrorHandler is not null)
        {
            var decision = await context.PipelineErrorHandler.HandleNodeFailureAsync(
                context.CurrentNodeId,
                branchException,
                context,
                cancellationToken).ConfigureAwait(false);

            switch (decision)
            {
                case PipelineErrorDecision.FailPipeline:
                    throw branchException;

                case PipelineErrorDecision.ContinueWithoutNode:
                    // Log and continue - the error handler has decided to skip this branch failure
                    LogBranchException(branchException, context, "Error handler decided to continue without node.");
                    break;

                case PipelineErrorDecision.RestartNode:
                    // For branch handlers, restart doesn't make sense - treat as continue
                    LogBranchException(branchException, context, "RestartNode decision received for branch handler; treating as ContinueWithoutNode.");
                    break;

                default:
                    // Unknown decision - fail to be safe
                    throw branchException;
            }
        }
        else
        {
            // No pipeline error handler configured - throw to fail the pipeline
            throw branchException;
        }
    }

    private void LogBranchException(BranchHandlerException branchException, PipelineContext context, string? additionalMessage = null)
    {
        var logger = context.LoggerFactory.CreateLogger(typeof(BranchNode<T>).FullName ?? typeof(BranchNode<T>).Name);

        if (additionalMessage is not null)
        {
            BranchNodeLogMessages.BranchHandlerExceptionWithMessage(
                logger,
                branchException.InnerException!,
                branchException.BranchIndex,
                context.CurrentNodeId,
                additionalMessage);
        }
        else
        {
            BranchNodeLogMessages.BranchHandlerException(
                logger,
                branchException.InnerException!,
                branchException.BranchIndex,
                context.CurrentNodeId);
        }
    }

    /// <inheritdoc />
    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
///     Specifies how a <see cref="BranchNode{T}" /> handles exceptions from branch handlers.
/// </summary>
public enum BranchErrorHandlingMode
{
    /// <summary>
    ///     Route exceptions through the pipeline's error handling system via <see cref="IPipelineErrorHandler" />.
    ///     This is the default and recommended mode. If no error handler is configured, the exception will propagate.
    /// </summary>
    RouteToErrorHandler,

    /// <summary>
    ///     Collect all branch exceptions and throw them as an <see cref="AggregateException" /> after all branches complete.
    ///     Use this when you want all branches to attempt execution even if some fail.
    /// </summary>
    CollectAndThrow,

    /// <summary>
    ///     Log exceptions but continue processing without propagating them.
    ///     Use with caution as this can hide errors. Consider using <see cref="RouteToErrorHandler" /> instead.
    /// </summary>
    LogAndContinue,
}
