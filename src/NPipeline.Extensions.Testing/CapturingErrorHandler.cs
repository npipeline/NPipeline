using NPipeline.Reliability;

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
    public async ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
    {
        await InvokeOriginalAsync(() => originalPolicy.DecideItemFailureAsync(failure, cancellationToken)).ConfigureAwait(false);

        errors.Add(failure.Exception);
        return decisionOnError;
    }

    public async ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
    {
        await InvokeOriginalAsync(() => originalPolicy.DecideRestartAsync(failure, cancellationToken)).ConfigureAwait(false);

        errors.Add(failure.Exception);
        return decisionOnError;
    }

    public async ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
    {
        await InvokeOriginalAsync(() => originalPolicy.DecideNodeFailureAsync(failure, cancellationToken)).ConfigureAwait(false);

        errors.Add(failure.Exception);
        return decisionOnError;
    }

    private static async Task InvokeOriginalAsync(Func<ValueTask<ResilienceDecision>> action)
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
