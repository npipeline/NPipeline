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
    protected ParallelExecutionStrategyBase(int? maxDegreeOfParallelism = null)
    {
        ConfiguredMaxDop = maxDegreeOfParallelism;
    }

    protected int? ConfiguredMaxDop { get; }

    public abstract Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(
        IDataPipe<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken);

    /// <summary>
    ///     Gets effective retry options for a node, checking per-node, global, and context fallback.
    /// </summary>
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
    ///     Executes a transform node on an item with retry logic and error handling.
    /// </summary>
    protected static async Task<TOut?> ExecuteWithRetryAsync<TIn, TOut>(
        TIn item,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        PipelineRetryOptions effectiveRetries,
        CancellationToken cancellationToken,
        ParallelExecutionMetrics? metrics = null,
        IExecutionObserver? observer = null)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ParallelExecutionStrategyBase));
        using var itemActivity = context.Tracer.StartActivity("Item.Transform");
        var attempt = 0;
        logger.Log(LogLevel.Debug, "Starting processing of item {Item} for node {NodeId}", item?.ToString() ?? "(null)", nodeId);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                logger.Log(LogLevel.Debug, "Attempt {Attempt} for item {Item} for node {NodeId}", attempt + 1, item?.ToString() ?? "(null)", nodeId);
                var valueTaskTransform = node as IValueTaskTransform<TIn, TOut>;

                var work = valueTaskTransform is not null
                    ? valueTaskTransform.ExecuteValueTaskAsync(item, context, cancellationToken)
                    : new ValueTask<TOut>(node.ExecuteAsync(item, context, cancellationToken));

                var result = await work.ConfigureAwait(false);
                logger.Log(LogLevel.Debug, "Success on attempt {Attempt} for item {Item} for node {NodeId}", attempt + 1, item?.ToString() ?? "(null)", nodeId);
                return result;
            }
            catch (Exception ex)
            {
                logger.Log(LogLevel.Debug, ex, "Exception on attempt {Attempt} for item {Item} for node {NodeId}: {Message}", attempt + 1,
                    item?.ToString() ?? "(null)", nodeId, ex.Message);

                itemActivity.RecordException(ex);

                if (node.ErrorHandler is null)
                    throw;

                if (node.ErrorHandler is not INodeErrorHandler<ITransformNode<TIn, TOut>, TIn> typedHandler)
                    throw;

                var decision = await typedHandler.HandleAsync(node, item, ex, context, cancellationToken);

                logger.Log(LogLevel.Debug, "Error handler decision: {Decision} for item {Item} for node {NodeId}", decision, item?.ToString() ?? "(null)",
                    nodeId);

                switch (decision)
                {
                    case NodeErrorDecision.Skip:
                        return default;
                    case NodeErrorDecision.DeadLetter:
                        if (context.DeadLetterSink is not null)
                            await context.DeadLetterSink.HandleAsync(nodeId, item!, ex, context, cancellationToken);

                        return default;
                    case NodeErrorDecision.Retry:
                        attempt++;

                        logger.Log(LogLevel.Debug, "Retry attempt {Attempt} for item {Item} for node {NodeId}, max retries: {MaxRetries}", attempt,
                            item?.ToString() ?? "(null)", nodeId, effectiveRetries.MaxItemRetries);

                        if (attempt > effectiveRetries.MaxItemRetries)
                            throw;

                        itemActivity.SetTag("retry.attempt", attempt.ToString());

                        // Record retry metrics if metrics collection is enabled
                        if (metrics is not null)
                        {
                            logger.Log(LogLevel.Debug, "Recording retry attempt {Attempt} for item {Item} for node {NodeId}", attempt,
                                item?.ToString() ?? "(null)", nodeId);

                            metrics.RecordRetry(attempt);
                        }

                        // Notify observer of retry event (independent of metrics collection)
                        observer?.OnRetry(new NodeRetryEvent(nodeId, RetryKind.ItemRetry, attempt, ex));
                        continue;
                    case NodeErrorDecision.Fail:
                    default:
                        throw;
                }
            }
        }
    }
}
