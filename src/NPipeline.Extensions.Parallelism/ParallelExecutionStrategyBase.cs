using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Base class for parallel execution strategies with common retry and handler logic.
/// </summary>
public abstract class ParallelExecutionStrategyBase : IExecutionStrategy
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ParallelExecutionStrategyBase" /> class.
    /// </summary>
    /// <param name="maxDegreeOfParallelism">The optional maximum degree of parallelism. If null, uses default behavior.</param>
    protected ParallelExecutionStrategyBase(int? maxDegreeOfParallelism = null)
    {
        ConfiguredMaxDop = maxDegreeOfParallelism;
    }

    /// <summary>
    ///     Gets the configured maximum degree of parallelism for the strategy.
    /// </summary>
    protected int? ConfiguredMaxDop { get; }

    /// <summary>
    ///     Executes a transform node with parallel processing strategy.
    /// </summary>
    /// <typeparam name="TIn">The type of input data.</typeparam>
    /// <typeparam name="TOut">The type of output data.</typeparam>
    /// <param name="input">The input data pipe.</param>
    /// <param name="node">The transform node to execute.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation with the output data pipe.</returns>
    public abstract Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(IDataPipe<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Gets effective retry options for a node, checking per-node, global, and context fallback.
    /// </summary>
    /// <param name="nodeId">The identifier of the node.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <returns>The effective retry options to use for the node.</returns>
    protected static PipelineRetryOptions GetRetryOptions(string nodeId, PipelineContext context)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ParallelExecutionStrategyBase));

        // Check for per-node retry options first
        if (context.Items.TryGetValue($"retryOptions::{nodeId}", out var perNodeOptions) && perNodeOptions is PipelineRetryOptions nodeOptions)
        {
            logger.Log(LogLevel.Debug, "Node {NodeId}, Found per-node retry options: MaxRetries={MaxRetries}", nodeId, nodeOptions.MaxItemRetries);
            return nodeOptions;
        }

        // Check for global retry options stored by PipelineRunner
        if (context.Items.TryGetValue(PipelineContextKeys.GlobalRetryOptions, out var globalOptions) &&
            globalOptions is PipelineRetryOptions globalRetryOptions)
        {
            logger.Log(LogLevel.Debug, "Node {NodeId}, Using global retry options: MaxItemRetries={MaxRetries}", nodeId, globalRetryOptions.MaxItemRetries);
            return globalRetryOptions;
        }

        // Fall back to context retry options
        logger.Log(LogLevel.Debug, "Node {NodeId}, Using context retry options: MaxItemRetries={MaxRetries}", nodeId, context.RetryOptions.MaxItemRetries);
        return context.RetryOptions;
    }

    /// <summary>
    ///     Executes a transform node on an item with retry logic and error handling using a cached execution context.
    /// </summary>
    /// <typeparam name="TIn">The type of input data.</typeparam>
    /// <typeparam name="TOut">The type of output data.</typeparam>
    /// <param name="item">The item to process.</param>
    /// <param name="node">The transform node to execute.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cached">The cached execution context with pre-resolved configuration.</param>
    /// <param name="metrics">Optional metrics for tracking execution.</param>
    /// <param name="observer">Optional observer for execution events.</param>
    /// <returns>A task representing the asynchronous operation with the processed item, or null if skipped.</returns>
    /// <remarks>
    ///     This overload accepts a pre-created <see cref="CachedNodeExecutionContext" /> to avoid
    ///     per-item dictionary lookups and allocations, improving performance for high-throughput scenarios.
    /// </remarks>
    protected static async Task<TOut?> ExecuteWithRetryAsync<TIn, TOut>(
        TIn item,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CachedNodeExecutionContext cached,
        ParallelExecutionMetrics? metrics = null,
        IExecutionObserver? observer = null)
    {
        var logger = cached.LoggingEnabled
            ? context.LoggerFactory.CreateLogger(nameof(ParallelExecutionStrategyBase))
            : null;

        using var itemActivity = cached.TracingEnabled
            ? context.Tracer.StartActivity("Item.Transform")
            : null;

        var attempt = 0;

        while (true)
        {
            cached.CancellationToken.ThrowIfCancellationRequested();

            try
            {
                return await ExecuteNodeAsync(node, item, context, cached.CancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                itemActivity?.RecordException(ex);

                if (logger is not null)
                    LogNodeFailure(logger, cached.NodeId, attempt + 1, ex);

                if (node.ErrorHandler is null)
                    throw;

                if (node.ErrorHandler is not INodeErrorHandler<ITransformNode<TIn, TOut>, TIn> typedHandler)
                    throw;

                var decision = await HandleNodeErrorAsync(node, cached.NodeId, item, ex, context, typedHandler, cached.CancellationToken).ConfigureAwait(false);

                switch (decision)
                {
                    case NodeErrorDecision.Skip:
                        return default;
                    case NodeErrorDecision.DeadLetter:
                        return default;
                    case NodeErrorDecision.Retry:
                        attempt++;

                        if (attempt > cached.RetryOptions.MaxItemRetries)
                            throw;

                        itemActivity?.SetTag("retry.attempt", attempt.ToString());
                        PublishRetryInstrumentation(metrics, observer, cached.NodeId, attempt, ex);
                        continue;
                    case NodeErrorDecision.Fail:
                    default:
                        throw;
                }
            }
        }
    }

    private static ValueTask<TOut> ExecuteNodeAsync<TIn, TOut>(ITransformNode<TIn, TOut> node, TIn item, PipelineContext context,
        CancellationToken cancellationToken)
    {
        return node is IValueTaskTransform<TIn, TOut> fastPath
            ? fastPath.ExecuteValueTaskAsync(item, context, cancellationToken)
            : new ValueTask<TOut>(node.ExecuteAsync(item, context, cancellationToken));
    }

    private static async Task<NodeErrorDecision> HandleNodeErrorAsync<TIn, TOut>(ITransformNode<TIn, TOut> node, string nodeId, TIn item, Exception exception,
        PipelineContext context, INodeErrorHandler<ITransformNode<TIn, TOut>, TIn> handler, CancellationToken cancellationToken)
    {
        var decision = await handler.HandleAsync(node, item, exception, context, cancellationToken).ConfigureAwait(false);

        if (decision == NodeErrorDecision.DeadLetter && context.DeadLetterSink is not null)
            await context.DeadLetterSink.HandleAsync(nodeId, item!, exception, context, cancellationToken).ConfigureAwait(false);

        return decision;
    }

    private static void PublishRetryInstrumentation(ParallelExecutionMetrics? metrics, IExecutionObserver? observer, string nodeId, int attempt,
        Exception exception)
    {
        metrics?.RecordRetry(attempt);
        observer?.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, attempt, exception));
    }

    private static void LogNodeFailure(IPipelineLogger logger, string nodeId, int attemptNumber, Exception exception)
    {
        if (!logger.IsEnabled(LogLevel.Debug))
            return;

        logger.Log(LogLevel.Debug, exception, "Node {NodeId} failed on attempt {Attempt}.", nodeId, attemptNumber);
    }
}
