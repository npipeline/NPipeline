using System.Diagnostics;
using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Tests.Execution.Services;

/// <summary>
///     Item-level retry used to <c>continue</c> straight into the next attempt, so the whole retry-delay subsystem —
///     exponential backoff, jitter, the composite strategy — was inert for the most common retry scenario and the
///     pipeline spun against a struggling dependency as fast as the CPU allowed.
/// </summary>
public sealed class PerItemRetryDelayTests
{
    private const string NodeId = "transform";

    [Fact]
    public async Task ItemRetry_AsksThePolicyForADelayOnEveryAttempt()
    {
        var policy = new RecordingDelayPolicy(TimeSpan.Zero);
        var (context, pipelineId) = CreateContext(policy);

        try
        {
            var result = await ExecuteAsync(context, pipelineId, policy, failures: 3, maxItemRetries: 3);

            result.Outcome.Should().Be(ItemExecutionOutcome.Emitted);
            // Each retry must consult the delay strategy, numbered from 1.
            policy.RequestedAttempts.Should().Equal(1, 2, 3);
        }
        finally
        {
            await context.DisposeAsync();
        }
    }

    [Fact]
    public async Task ItemRetry_ActuallyWaitsForTheConfiguredDelay()
    {
        var delay = TimeSpan.FromMilliseconds(120);
        var policy = new RecordingDelayPolicy(delay);
        var (context, pipelineId) = CreateContext(policy);

        try
        {
            var stopwatch = Stopwatch.StartNew();
            _ = await ExecuteAsync(context, pipelineId, policy, failures: 2, maxItemRetries: 3);
            stopwatch.Stop();

            // Two retries at 120ms. Asserting against a fraction of the total keeps this robust on a loaded machine
            // while still failing outright if the delay is skipped, which is what the defect did.
            stopwatch.Elapsed.Should().BeGreaterThan(delay, "the configured backoff must actually be awaited");
        }
        finally
        {
            await context.DisposeAsync();
        }
    }

    [Fact]
    public async Task ItemRetry_DoesNotDelayWhenTheItemSucceedsFirstTime()
    {
        var policy = new RecordingDelayPolicy(TimeSpan.FromSeconds(30));
        var (context, pipelineId) = CreateContext(policy);

        try
        {
            var result = await ExecuteAsync(context, pipelineId, policy, failures: 0, maxItemRetries: 3);

            result.Outcome.Should().Be(ItemExecutionOutcome.Emitted);
            policy.RequestedAttempts.Should().BeEmpty("a successful item must not touch the delay strategy");
        }
        finally
        {
            await context.DisposeAsync();
        }
    }

    [Fact]
    public async Task ItemRetry_DoesNotDelayAfterTheFinalAttemptFails()
    {
        // The delay belongs before a retry, not after the last one: it would be pure dead time.
        var policy = new RecordingDelayPolicy(TimeSpan.Zero);
        var (context, pipelineId) = CreateContext(policy);

        try
        {
            var act = () => ExecuteAsync(context, pipelineId, policy, failures: 5, maxItemRetries: 2);

            _ = await act.Should().ThrowAsync<InvalidOperationException>();
            // Two retries means two delays, not three.
            policy.RequestedAttempts.Should().Equal(1, 2);
        }
        finally
        {
            await context.DisposeAsync();
        }
    }

    [Fact]
    public async Task ItemRetry_PropagatesCancellationRaisedDuringTheDelay()
    {
        using var cts = new CancellationTokenSource();
        var policy = new RecordingDelayPolicy(TimeSpan.FromSeconds(30)) { CancellationSource = cts };
        var (context, pipelineId) = CreateContext(policy);

        try
        {
            var act = () => ExecuteAsync(context, pipelineId, policy, failures: 5, maxItemRetries: 3, cts.Token);

            _ = await act.Should().ThrowAsync<OperationCanceledException>();
        }
        finally
        {
            await context.DisposeAsync();
        }
    }

    [Fact]
    public async Task ItemRetry_SurvivesADelayStrategyThatThrows()
    {
        // A broken delay strategy must not break the retry itself.
        var policy = new RecordingDelayPolicy(TimeSpan.Zero) { ThrowFromDelay = true };
        var (context, pipelineId) = CreateContext(policy);

        try
        {
            var result = await ExecuteAsync(context, pipelineId, policy, failures: 2, maxItemRetries: 3);

            result.Outcome.Should().Be(ItemExecutionOutcome.Emitted);
            policy.RequestedAttempts.Should().Equal(1, 2);
        }
        finally
        {
            await context.DisposeAsync();
        }
    }

    private static Task<ItemExecutionResult<int>> ExecuteAsync(
        PipelineContext context,
        Guid pipelineId,
        IResiliencePolicy policy,
        int failures,
        int maxItemRetries,
        CancellationToken cancellationToken = default)
    {
        return PerItemRetryExecutor.Instance.ExecuteWithRetryAsync(
            item: 7,
            node: new FlakyTransform(failures),
            context,
            NodeId,
            maxItemRetries,
            hasLineageIndex: false,
            lineageInputIndex: 0,
            lineageOutcomeWriter: LineageNodeOutcomeRegistry.GetWriter(pipelineId, NodeId),
            itemActivity: null,
            cancellationToken);
    }

    private static (PipelineContext Context, Guid PipelineId) CreateContext(IResiliencePolicy policy)
    {
        var context = new PipelineContext();
        context.RunIdentity.PipelineId = Guid.NewGuid();
        context.RunIdentity.RunId = Guid.NewGuid();
        context.ExecutionConfiguration.ResiliencePolicy = policy;
        return (context, context.RunIdentity.PipelineId);
    }

    private sealed class FlakyTransform(int failuresBeforeSuccess) : TransformNode<int, int>
    {
        private int _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (_attempts++ < failuresBeforeSuccess)
                throw new InvalidOperationException("transient");

            return ValueTask.FromResult<int>(item);
        }
    }

    private sealed class RecordingDelayPolicy(TimeSpan delay) : IResiliencePolicy
    {
        public List<int> RequestedAttempts { get; } = [];

        public bool ThrowFromDelay { get; init; }

        public CancellationTokenSource? CancellationSource { get; init; }

        public async ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, RetryKind retryKind, int attemptNumber, CancellationToken cancellationToken)
        {
            RequestedAttempts.Add(attemptNumber);

            if (ThrowFromDelay)
                throw new InvalidOperationException("delay strategy is broken");

            if (CancellationSource is not null)
                await CancellationSource.CancelAsync();

            return delay;
        }

        public Task<ResilienceDecision> DecideItemFailureAsync<TIn, TOut>(ITransformNode<TIn, TOut> node, TIn failedItem, Exception exception,
            PipelineContext context, string nodeId, int retryAttempt, CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Retry);
        }

        public Task<ResilienceDecision> DecideNodeFailureAsync(NodeDefinition nodeDefinition, INode node, Exception exception,
            PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public Task<ResilienceDecision> DecidePipelineFailureAsync(string nodeId, Exception exception, PipelineContext context,
            CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Fail);
        }

        public IResilienceCircuitBreaker? GetCircuitBreaker(PipelineContext context, string nodeId)
        {
            return DefaultResiliencePolicy.Instance.GetCircuitBreaker(context, nodeId);
        }
    }
}
