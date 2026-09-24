using NPipeline.Reliability;

namespace NPipeline.Tests.Common;

/// <summary>
///     Retries every item failure, transient or not, until the node's <see cref="ItemRetryOptions.MaxRetries" /> runs
///     out, then fails.
/// </summary>
public sealed class RetryHandler : ResiliencePolicyBase
{
    public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
        ValueTask.FromResult(failure.Attempt <= failure.MaxRetries
            ? ResilienceDecision.Retry
            : ResilienceDecision.Fail);
}
