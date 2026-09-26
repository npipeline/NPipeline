using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Lineage;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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

    public static ItemExecutionResult<TOut> Emitted(TOut output, int retryCount) => new(ItemExecutionOutcome.Emitted, output, retryCount);

    public static ItemExecutionResult<TOut> Skipped(int retryCount) => new(ItemExecutionOutcome.Skipped, default, retryCount);

    public static ItemExecutionResult<TOut> DeadLettered(int retryCount) => new(ItemExecutionOutcome.DeadLettered, default, retryCount);
}

internal interface IPerItemRetryExecutor
{
    ValueTask<ItemExecutionResult<TOut>> ExecuteWithRetryAsync<TIn, TOut>(
        TIn item,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        PipelineResilienceOptions options,
        bool hasLineageIndex,
        long lineageInputIndex,
        LineageNodeOutcomeWriter lineageOutcomeWriter,
        IPipelineActivity? itemActivity,
        CancellationToken cancellationToken,
        Action<int>? onRetry = null,
        CircuitBreaker? circuitBreaker = null);
}
