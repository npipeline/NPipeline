using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

internal enum ItemExecutionOutcome
{
    Emitted,
    Skipped,
    DeadLettered,
}

internal readonly record struct ItemExecutionResult<TOut>(
    ItemExecutionOutcome Outcome,
    TOut? Output,
    int RetryCount)
{
    public bool Produced => Outcome == ItemExecutionOutcome.Emitted;

    public static ItemExecutionResult<TOut> Emitted(TOut output, int retryCount)
    {
        return new ItemExecutionResult<TOut>(ItemExecutionOutcome.Emitted, output, retryCount);
    }

    public static ItemExecutionResult<TOut> Skipped(int retryCount)
    {
        return new ItemExecutionResult<TOut>(ItemExecutionOutcome.Skipped, default, retryCount);
    }

    public static ItemExecutionResult<TOut> DeadLettered(int retryCount)
    {
        return new ItemExecutionResult<TOut>(ItemExecutionOutcome.DeadLettered, default, retryCount);
    }
}

internal interface IPerItemRetryExecutor
{
    Task<ItemExecutionResult<TOut>> ExecuteWithRetryAsync<TIn, TOut>(
        TIn item,
        ITransformNode<TIn, TOut> node,
        IValueTaskTransform<TIn, TOut>? valueTaskTransform,
        PipelineContext context,
        string nodeId,
        int maxItemRetries,
        bool hasLineageIndex,
        long lineageInputIndex,
        IPipelineActivity? itemActivity,
        CancellationToken cancellationToken);
}
