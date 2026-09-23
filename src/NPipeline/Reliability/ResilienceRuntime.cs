using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Pipeline;

namespace NPipeline.Reliability;

/// <summary>
///     What the three retry layers share: which policy decides for a node, and the ceiling on how often a policy
///     may repeat work.
/// </summary>
internal static class ResilienceRuntime
{
    /// <summary>
    ///     The most times one unit of work is repeated at a policy's request before the node fails.
    /// </summary>
    public const int MaxPolicyRepeats = 100;

    /// <summary>
    ///     The policy registered for <paramref name="nodeId" />, or else the pipeline's.
    /// </summary>
    public static IResiliencePolicy ResolvePolicy(PipelineContext context, string nodeId)
    {
        var key = ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId);

        if (context.NodeEnvironment.NodeExecutionScopeRegistry.TryGetRuntimeAnnotation(key, out var annotation) && annotation is IResiliencePolicy nodePolicy)
            return nodePolicy;

        return context.ExecutionConfiguration.ResiliencePolicy;
    }

    /// <summary>
    ///     Raises a retry event for one of the three layers.
    /// </summary>
    public static void ReportRetry(PipelineContext context, string nodeId, RetryKind kind, int attempt, Exception failure)
    {
        context.Observability.ExecutionObserver.OnRetry(
            new NodeRetryEvent(nodeId, kind, attempt, failure, context.RunIdentity.PipelineId, context.RunIdentity.PipelineName));
    }

    /// <summary>
    ///     Raises the event for a layer that stopped retrying after at least one retry.
    /// </summary>
    public static void ReportRetryExhausted(PipelineContext context, string nodeId, RetryKind kind, int attempts, Exception lastFailure)
    {
        context.Observability.ExecutionObserver.OnRetryExhausted(
            new RetryExhaustedEvent(nodeId, kind, attempts, lastFailure, context.RunIdentity.PipelineId, context.RunIdentity.PipelineName));
    }

    /// <summary>
    ///     The failure raised when a policy keeps asking for the same work to be repeated.
    /// </summary>
    public static InvalidOperationException RepeatCeilingExceeded(IResiliencePolicy policy, string nodeId, ResilienceDecision decision, Exception lastFailure)
    {
        return new InvalidOperationException(
            $"Resilience policy '{policy.GetType().FullName}' answered {decision} more than {MaxPolicyRepeats} times for the same work on node " +
            $"'{nodeId}'. A policy should stop repeating once the failure's CanRetry (or CanRestart) is false.",
            lastFailure);
    }
}
