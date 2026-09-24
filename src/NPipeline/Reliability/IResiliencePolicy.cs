namespace NPipeline.Reliability;

/// <summary>
///     Decides what happens when work fails, at each of the three layers that can repeat it.
/// </summary>
/// <remarks>
///     <para>
///         The policy owns the decision; the runtime carries it out. How many retries the node's options allow is
///         passed to the policy as advice (<c>MaxRetries</c>, <c>CanRetry</c>), and the runtime never overrides the
///         answer. How long to wait before a retry comes from the layer's <see cref="RetryBackoff" />.
///     </para>
///     <para>
///         Derive from <see cref="ResiliencePolicyBase" /> to override only the decisions you care about; the others
///         follow the node's options, as <see cref="DefaultResiliencePolicy" /> does.
///     </para>
///     <para>
///         A policy that answers <see cref="ResilienceDecision.Retry" /> or <see cref="ResilienceDecision.RestartNode" />
///         more than 100 times for one unit of work fails the node, rather than looping forever.
///     </para>
/// </remarks>
public interface IResiliencePolicy
{
    /// <summary>
    ///     Decides what happens to an item whose transform failed (L1). Meaningful answers:
    ///     <see cref="ResilienceDecision.Retry" />, <see cref="ResilienceDecision.Skip" />,
    ///     <see cref="ResilienceDecision.DeadLetter" />, and <see cref="ResilienceDecision.Fail" />.
    /// </summary>
    ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken);

    /// <summary>
    ///     Decides what happens when a transform node's output stream failed (L2). Meaningful answers:
    ///     <see cref="ResilienceDecision.RestartNode" />, <see cref="ResilienceDecision.ContinueWithoutNode" />, and
    ///     <see cref="ResilienceDecision.Fail" />.
    /// </summary>
    ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken);

    /// <summary>
    ///     Decides what happens when a node's execution failed (L3). Meaningful answers:
    ///     <see cref="ResilienceDecision.Retry" /> and <see cref="ResilienceDecision.Fail" />.
    /// </summary>
    ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken);
}
