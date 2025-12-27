using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     Sequential single-threaded execution strategy.
///     Ensures PipelineContext.CurrentNodeId is set to the transform node id while processing each item
///     and restored afterward so downstream nodes (e.g. sinks) don't overwrite attribution for the transform stage.
/// </summary>
public sealed class SequentialExecutionStrategy : IExecutionStrategy
{
    /// <inheritdoc />
    public Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(
        IDataPipe<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var nodeId = context.CurrentNodeId; // capture id for this node once
        var valueTaskTransform = node as IValueTaskTransform<TIn, TOut>;

        // Create cached execution context once per node (optimization: reduces per-item dictionary lookups)
        var cached = CachedNodeExecutionContext.Create(context, nodeId);

        // Create immutability guard (DEBUG-only validation, zero overhead in RELEASE)
        var immutabilityGuard = PipelineContextImmutabilityGuard.Create(context, cached);

        // Use Task.FromResult for already-completed synchronous result
        return Task.FromResult<IDataPipe<TOut>>(new StreamingDataPipe<TOut>(Iterate(cancellationToken)));

        async IAsyncEnumerable<TOut> Iterate([EnumeratorCancellation] CancellationToken ct)
        {
            var tracer = context.Tracer;

            await foreach (var item in input.WithCancellation(ct))
            {
                // Use cached values to avoid per-item dictionary lookups and allocations
                using var itemActivity = cached.TracingEnabled
                    ? tracer.StartActivity("Item.Transform")
                    : null;

                using var _ = context.ScopedNode(cached.NodeId);
                var attempt = 0;
                var effectiveRetries = cached.RetryOptions;

                var produced = false;
                TOut? output = default;
                Exception? originalException;
                var shouldSkipToNextItem = false;

                while (!shouldSkipToNextItem)
                {
                    try
                    {
                        var work = valueTaskTransform?.ExecuteValueTaskAsync(item, context, ct)
                                   ?? new ValueTask<TOut>(node.ExecuteAsync(item, context, ct));

                        output = await work.ConfigureAwait(false);
                        produced = true;
                        break; // success
                    }
                    catch (Exception ex)
                    {
                        originalException = ex;
                        itemActivity?.RecordException(ex);

                        if (node.ErrorHandler is not INodeErrorHandler<ITransformNode<TIn, TOut>, TIn> typedHandler)
                        {
                            // No handler or wrong type, rethrow the original exception
                            throw;
                        }

                        var decision = await typedHandler.HandleAsync(node, item, ex, context, ct);

                        // Handle the error decision using pattern matching
                        var (producedValue, shouldGotoAfterItem) = decision switch
                        {
                            NodeErrorDecision.Skip => HandleSkip(),
                            NodeErrorDecision.DeadLetter => await HandleRedirect(context, nodeId, item, ex, ct),
                            NodeErrorDecision.Retry => HandleRetry(ref attempt, effectiveRetries.MaxItemRetries, itemActivity),
                            NodeErrorDecision.Fail => throw originalException!, // Preserve original exception
                            _ => throw new InvalidOperationException($"Error handling failed for node {nodeId}", ex),
                        };

                        if (shouldGotoAfterItem)
                        {
                            produced = producedValue;
                            shouldSkipToNextItem = true;
                        }

                        // Local functions for handling each decision type
                        static (bool Produced, bool ShouldGotoAfterItem) HandleSkip()
                        {
                            return (false, true);
                        }

                        async Task<(bool Produced, bool ShouldGotoAfterItem)> HandleRedirect(
                            PipelineContext ctx, string id, TIn itm, Exception exception, CancellationToken token)
                        {
                            if (ctx.DeadLetterSink is not null)
                                await ctx.DeadLetterSink.HandleAsync(id, itm!, exception, ctx, token).ConfigureAwait(false);

                            return (false, true);
                        }

                        (bool Produced, bool ShouldGotoAfterItem) HandleRetry(
                            ref int att, int maxRetries, IPipelineActivity? activity)
                        {
                            att++;

                            if (att > maxRetries)
                                throw new InvalidOperationException($"An item failed to process after {att} attempts.", originalException!);

                            activity?.SetTag("retry.attempt", att.ToString());
                            return (false, false);
                        }
                    }
                }

                if (produced)
                    yield return output!;
            }

            // Validate context immutability after processing all items (DEBUG-only, zero overhead in RELEASE)
            immutabilityGuard.Validate(context);
        }
    }
}
