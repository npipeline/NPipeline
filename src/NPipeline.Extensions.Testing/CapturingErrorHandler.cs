using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A resilience policy wrapper that captures exceptions for test assertions.
/// </summary>
/// <remarks>
///     The wrapped policy executes first to preserve its side effects; this policy then records
///     the exception and returns the configured decision so tests can control failure flow.
/// </remarks>
internal sealed class CapturingResiliencePolicy(
    IResiliencePolicy originalPolicy,
    List<Exception> errors,
    ResilienceDecision decisionOnError = ResilienceDecision.Skip) : IResiliencePolicy
{
    public async Task<ResilienceDecision> DecideNodeFailureAsync(
        NodeDefinition nodeDefinition,
        INode node,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await InvokeOriginalAsync(
            () => originalPolicy.DecideNodeFailureAsync(nodeDefinition, node, exception, context, cancellationToken))
            .ConfigureAwait(false);

        errors.Add(exception);
        return decisionOnError;
    }

    public async Task<ResilienceDecision> DecidePipelineFailureAsync(
        string nodeId,
        Exception exception,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        await InvokeOriginalAsync(
            () => originalPolicy.DecidePipelineFailureAsync(nodeId, exception, context, cancellationToken))
            .ConfigureAwait(false);

        errors.Add(exception);
        return decisionOnError;
    }

    public async Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(
        ITransformNode<TIn, TOut> node,
        TIn failedItem,
        Exception exception,
        PipelineContext context,
        string nodeId,
        int retryAttempt,
        CancellationToken cancellationToken)
    {
        await InvokeOriginalAsync(
            () => originalPolicy.DecideItemFailureAsync(node, failedItem, exception, context, nodeId, retryAttempt, cancellationToken))
            .ConfigureAwait(false);

        errors.Add(exception);
        return decisionOnError;
    }

    public ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, int attemptNumber, CancellationToken cancellationToken)
    {
        return originalPolicy.GetRetryDelayAsync(context, attemptNumber, cancellationToken);
    }

    public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
    {
        return originalPolicy.GetCircuitBreaker(context, nodeId);
    }

    private static async Task InvokeOriginalAsync(Func<Task<ResilienceDecision>> action)
    {
        try
        {
            _ = await action().ConfigureAwait(false);
        }
        catch
        {
            // Capturing policy should not fail open due to original policy exceptions in tests.
        }
    }
}
