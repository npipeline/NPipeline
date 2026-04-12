using NPipeline.Pipeline;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Resolves failure attribution by inspecting the exception chain for <see cref="NodeExecutionException" />
///     to determine the true origin of a failure versus the node making the error handling decision.
/// </summary>
public static class FailureAttributionResolver
{
    /// <summary>
    ///     Builds a <see cref="NodeFailureAttribution" /> from the exception and pipeline context.
    /// </summary>
    /// <param name="exception">The exception to inspect for origin information.</param>
    /// <param name="context">The active pipeline context.</param>
    /// <param name="decisionNodeId">The node id where the error handling decision is being made.</param>
    /// <param name="retryCount">The current retry count.</param>
    /// <param name="correlationId">An optional item-level correlation identifier.</param>
    /// <returns>A fully populated <see cref="NodeFailureAttribution" />.</returns>
    public static NodeFailureAttribution Resolve(
        Exception exception,
        PipelineContext context,
        string decisionNodeId,
        int retryCount,
        Guid? correlationId = null)
    {
        var originNodeId = ExtractOriginNodeId(exception) ?? decisionNodeId;

        return new NodeFailureAttribution(
            OriginNodeId: originNodeId,
            DecisionNodeId: decisionNodeId,
            OriginPipelineId: context.PipelineId,
            DecisionPipelineId: context.PipelineId,
            RunId: context.RunId == Guid.Empty ? null : context.RunId,
            CorrelationId: correlationId,
            RetryCount: retryCount);
    }

    /// <summary>
    ///     Walks the exception chain (including inner exceptions) to find the first
    ///     <see cref="NodeExecutionException" /> and extract its <see cref="NodeExecutionException.NodeId" />.
    /// </summary>
    private static string? ExtractOriginNodeId(Exception? exception)
    {
        while (exception is not null)
        {
            if (exception is NodeExecutionException nodeEx)
                return nodeEx.NodeId;

            exception = exception.InnerException;
        }

        return null;
    }
}
