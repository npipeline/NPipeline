using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Reliability;

/// <summary>
///     An item's transform failed (L1). Passed to <see cref="IResiliencePolicy.DecideItemFailureAsync{TIn}" />.
/// </summary>
/// <typeparam name="TIn">The item type.</typeparam>
public readonly record struct ItemFailure<TIn>
{
    /// <summary>
    ///     The item whose transform failed.
    /// </summary>
    public TIn Item { get; init; }

    /// <summary>
    ///     The node that failed.
    /// </summary>
    public required INode Node { get; init; }

    /// <summary>
    ///     The id of the node that failed.
    /// </summary>
    public required string NodeId { get; init; }

    /// <summary>
    ///     The failure.
    /// </summary>
    public required Exception Exception { get; init; }

    /// <summary>
    ///     The 1-based number of the attempt that failed.
    /// </summary>
    public int Attempt { get; init; }

    /// <summary>
    ///     The node's <see cref="ItemRetryOptions.MaxRetries" />.
    /// </summary>
    public int MaxRetries { get; init; }

    /// <summary>
    ///     Whether the node's <see cref="ItemRetryOptions.Classifier" /> judged the failure transient.
    /// </summary>
    public bool IsTransient { get; init; }

    /// <summary>
    ///     Whether the attempt was refused by an open circuit breaker rather than made and failed.
    /// </summary>
    public bool IsBreakerOpen { get; init; }

    /// <summary>
    ///     The pipeline context.
    /// </summary>
    public required PipelineContext Context { get; init; }

    /// <summary>
    ///     Whether the node's options allow another attempt: the failure is transient, the breaker is closed, and
    ///     retries remain.
    /// </summary>
    public bool CanRetry => IsTransient && !IsBreakerOpen && Attempt <= MaxRetries;
}

/// <summary>
///     A transform node's output stream failed (L2). Passed to <see cref="IResiliencePolicy.DecideRestartAsync" />.
/// </summary>
public readonly record struct StreamFailure
{
    /// <summary>
    ///     The id of the node whose stream failed.
    /// </summary>
    public required string NodeId { get; init; }

    /// <summary>
    ///     The failure.
    /// </summary>
    public required Exception Exception { get; init; }

    /// <summary>
    ///     The 1-based number of the run that failed. The first run is 1, the run after the first restart is 2.
    /// </summary>
    public int Attempt { get; init; }

    /// <summary>
    ///     The node's <see cref="NodeRestartOptions.MaxRestarts" />.
    /// </summary>
    public int MaxRestarts { get; init; }

    /// <summary>
    ///     The index of the first input item whose outcome has not been delivered. A restart resumes here.
    /// </summary>
    public long Checkpoint { get; init; }

    /// <summary>
    ///     The outputs the node has delivered downstream so far, across all its runs.
    /// </summary>
    public long Delivered { get; init; }

    /// <summary>
    ///     The pipeline context.
    /// </summary>
    public required PipelineContext Context { get; init; }

    /// <summary>
    ///     Whether the node's options allow another restart.
    /// </summary>
    public bool CanRestart => Attempt <= MaxRestarts;
}

/// <summary>
///     A node's execution failed (L3). Passed to <see cref="IResiliencePolicy.DecideNodeFailureAsync" />.
/// </summary>
public readonly record struct NodeFailure
{
    /// <summary>
    ///     The definition of the node that failed.
    /// </summary>
    public required NodeDefinition Definition { get; init; }

    /// <summary>
    ///     The node that failed.
    /// </summary>
    public required INode Node { get; init; }

    /// <summary>
    ///     The id of the node that failed.
    /// </summary>
    public string NodeId => Definition.Id;

    /// <summary>
    ///     The failure.
    /// </summary>
    public required Exception Exception { get; init; }

    /// <summary>
    ///     The 1-based number of the execution that failed.
    /// </summary>
    public int Attempt { get; init; }

    /// <summary>
    ///     The node's <see cref="NodeRetryOptions.MaxRetries" />.
    /// </summary>
    public int MaxRetries { get; init; }

    /// <summary>
    ///     Whether the node's <see cref="NodeRetryOptions.Classifier" /> judged the failure transient.
    /// </summary>
    public bool IsTransient { get; init; }

    /// <summary>
    ///     Whether the node had started reading its input before it failed.
    /// </summary>
    /// <remarks>
    ///     Node retry covers setup only. A node that has read input is not executed again, whatever the policy answers:
    ///     its input cannot be read again from the start, so a second execution would lose or duplicate items. A
    ///     transform recovers mid-stream through node restart, and a sink or source through its connector's retries.
    /// </remarks>
    public bool InputConsumed { get; init; }

    /// <summary>
    ///     The pipeline context.
    /// </summary>
    public required PipelineContext Context { get; init; }

    /// <summary>
    ///     Whether the node can be executed again: the failure is transient, retries remain, and the node has not
    ///     consumed input.
    /// </summary>
    public bool CanRetry => IsTransient && !InputConsumed && Attempt <= MaxRetries;
}
