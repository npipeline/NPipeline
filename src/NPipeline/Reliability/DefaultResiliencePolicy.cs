namespace NPipeline.Reliability;

/// <summary>
///     The policy used when none is registered. It carries out the node's <see cref="PipelineResilienceOptions" />
///     exactly and adds no rules of its own.
/// </summary>
/// <remarks>
///     <list type="bullet">
///         <item>
///             <description>
///                 Item failure: <see cref="ResilienceDecision.Retry" /> while <see cref="ItemFailure{TIn}.CanRetry" />,
///                 otherwise the node's <see cref="PipelineResilienceOptions.OnItemFailure" />. An attempt refused by an
///                 open breaker fails instead, so an outage does not dead-letter or skip the whole input.
///             </description>
///         </item>
///         <item>
///             <description>
///                 Stream failure: <see cref="ResilienceDecision.RestartNode" /> while
///                 <see cref="StreamFailure.CanRestart" />, otherwise <see cref="ResilienceDecision.Fail" />.
///             </description>
///         </item>
///         <item>
///             <description>
///                 Node failure: <see cref="ResilienceDecision.Retry" /> while <see cref="NodeFailure.CanRetry" />,
///                 otherwise <see cref="ResilienceDecision.Fail" />.
///             </description>
///         </item>
///     </list>
/// </remarks>
public sealed class DefaultResiliencePolicy : IResiliencePolicy
{
    /// <summary>
    ///     The shared instance. The policy is stateless.
    /// </summary>
    public static DefaultResiliencePolicy Instance { get; } = new();

    /// <inheritdoc />
    public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
    {
        if (failure.CanRetry)
            return ValueTask.FromResult(ResilienceDecision.Retry);

        if (failure.IsBreakerOpen)
            return ValueTask.FromResult(ResilienceDecision.Fail);

        var action = failure.Context.ExecutionConfiguration.GetResilienceOptions(failure.NodeId).OnItemFailure;

        return ValueTask.FromResult(action switch
        {
            ItemFailureAction.Skip => ResilienceDecision.Skip,
            ItemFailureAction.DeadLetter => ResilienceDecision.DeadLetter,
            _ => ResilienceDecision.Fail,
        });
    }

    /// <inheritdoc />
    public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
    {
        return ValueTask.FromResult(failure.CanRestart ? ResilienceDecision.RestartNode : ResilienceDecision.Fail);
    }

    /// <inheritdoc />
    public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
    {
        return ValueTask.FromResult(failure.CanRetry ? ResilienceDecision.Retry : ResilienceDecision.Fail);
    }
}
