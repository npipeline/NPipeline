using AwesomeAssertions;
using Microsoft.Extensions.Time.Testing;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Services;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Execution.Services;

/// <summary>
///     Item-level retry used to <c>continue</c> straight into the next attempt, so backoff was inert for the most
///     common retry scenario and the pipeline spun against a struggling dependency as fast as the CPU allowed. The
///     delay now comes from the node's <see cref="ItemRetryOptions.Backoff" />.
/// </summary>
public sealed class PerItemRetryDelayTests
{
    private const string NodeId = "transform";

    [Fact]
    public async Task ItemRetry_AsksTheBackoffForADelayBeforeEveryRetry()
    {
        var requests = new List<int>();
        await using var context = CreateContext();

        var result = await ExecuteAsync(context, Options(3, Recording(requests)), 3);

        result.Outcome.Should().Be(ItemExecutionOutcome.Emitted);
        requests.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task ItemRetry_WaitsForTheDelayOnThePipelinesClock()
    {
        var time = new FakeTimeProvider();
        await using var context = CreateContext();
        var options = Options(3, RetryBackoff.Constant(TimeSpan.FromMinutes(5))) with { Time = time };

        var run = ExecuteAsync(context, options, 1);

        await Task.Delay(50);
        run.IsCompleted.Should().BeFalse("the retry waits for the backoff");

        time.Advance(TimeSpan.FromMinutes(5));
        (await run.WaitAsync(TimeSpan.FromSeconds(10))).Outcome.Should().Be(ItemExecutionOutcome.Emitted);
    }

    [Fact]
    public async Task ItemRetry_DoesNotDelayWhenTheItemSucceedsFirstTime()
    {
        var requests = new List<int>();
        await using var context = CreateContext();

        var result = await ExecuteAsync(context, Options(3, Recording(requests)), 0);

        result.Outcome.Should().Be(ItemExecutionOutcome.Emitted);
        requests.Should().BeEmpty("a successful item must not wait");
    }

    [Fact]
    public async Task ItemRetry_DoesNotDelayAfterTheFinalAttemptFails()
    {
        // The delay belongs before a retry, not after the last one: it would be pure dead time.
        var requests = new List<int>();
        await using var context = CreateContext();

        var act = () => ExecuteAsync(context, Options(2, Recording(requests)), 5);

        _ = await act.Should().ThrowAsync<RetryExhaustedException>();
        requests.Should().Equal(1, 2);
    }

    [Fact]
    public async Task ItemRetry_PropagatesCancellationRaisedDuringTheDelay()
    {
        using var cts = new CancellationTokenSource();
        await using var context = CreateContext();

        var backoff = RetryBackoff.Custom(_ =>
        {
            cts.Cancel();
            return TimeSpan.FromSeconds(30);
        });

        var act = () => ExecuteAsync(context, Options(3, backoff), 5, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    private static PipelineResilienceOptions Options(int maxRetries, RetryBackoff backoff) =>

        // The transform's failures are not transient, so the classifier retries everything.
        PipelineResilienceOptions.None with
        {
            ItemRetry = new ItemRetryOptions { MaxRetries = maxRetries, Backoff = backoff, Classifier = RetryClassifier.All },
        };

    private static RetryBackoff Recording(List<int> requests)
    {
        return RetryBackoff.Custom(retry =>
        {
            requests.Add(retry);
            return TimeSpan.Zero;
        });
    }

    private static async Task<ItemExecutionResult<int>> ExecuteAsync(
        PipelineContext context,
        PipelineResilienceOptions options,
        int failures,
        CancellationToken cancellationToken = default) =>
        await PerItemRetryExecutor.Instance.ExecuteWithRetryAsync(
            7,
            new FlakyTransform(failures),
            context,
            NodeId,
            options,
            false,
            0,
            LineageNodeOutcomeRegistry.GetWriter(context.RunIdentity.PipelineId, NodeId),
            null,
            cancellationToken);

    private static PipelineContext CreateContext()
    {
        var context = new PipelineContext();
        context.RunIdentity.PipelineId = Guid.NewGuid();
        context.RunIdentity.RunId = Guid.NewGuid();
        return context;
    }

    private sealed class FlakyTransform(int failuresBeforeSuccess) : TransformNode<int, int>
    {
        private int _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (_attempts++ < failuresBeforeSuccess)
                throw new InvalidOperationException("failure");

            return ValueTask.FromResult(item);
        }
    }
}
