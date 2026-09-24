using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Resilience.Restart;

/// <summary>
///     <see cref="ResilientExecutionStrategy" /> used to exit both of its loops when the token was cancelled, which
///     completed the iterator normally: a cancelled run reported success with a silently truncated result set and
///     downstream sinks committed partial data. Cancellation must surface as an
///     <see cref="OperationCanceledException" /> instead, and must not be mistaken for a node failure.
/// </summary>
public sealed class ResilientCancellationTests
{
    private static readonly PassthroughNode Node = new();

    [Fact]
    public async Task AlreadyCancelledToken_ThrowsBeforeTheFirstAttempt()
    {
        var policy = new RecordingPolicy();
        var context = CreateContext(policy);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var (strategy, inner) = CreateStrategy(_ => Produce([1, 2, 3]));
        var stream = await strategy.ExecuteAsync(Input(), Node, context, "test-node", cts.Token);

        var act = () => DrainAsync(stream, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        inner.Attempts.Should().Be(0, "the node should never have been started");
        policy.PipelineFailureDecisions.Should().Be(0);
    }

    [Fact]
    public async Task CancellationMidStream_ThrowsRatherThanCompletingWithAPartialResult()
    {
        var policy = new RecordingPolicy();
        var context = CreateContext(policy);
        using var cts = new CancellationTokenSource();

        // Cancel once two items have been delivered. Before the fix the inner loop simply exited here and the iterator
        // completed normally, so the caller saw a clean two-item result for a five-item stream.
        var stream = await CreateStrategy(_ => Produce([1, 2, 3, 4, 5])).Strategy.ExecuteAsync(Input(), Node, context, "test-node", cts.Token);

        List<int> received = [];

        var act = async () =>
        {
            await foreach (var item in stream.WithCancellation(cts.Token))
            {
                received.Add(item);

                if (received.Count == 2)
                    await cts.CancelAsync();
            }
        };

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        received.Should().Equal(1, 2);
    }

    [Fact]
    public async Task CancellationRaisedByTheNode_IsNotTreatedAsANodeFailure()
    {
        var policy = new RecordingPolicy();
        var context = CreateContext(policy);
        using var cts = new CancellationTokenSource();

        // The node observes the pipeline token and throws, which is what a well-behaved node does on cancellation.
        var (strategy, inner) = CreateStrategy(ct => ProduceThenCancel([1, 2], cts, ct));
        var stream = await strategy.ExecuteAsync(Input(), Node, context, "test-node", cts.Token);

        var act = () => DrainAsync(stream, cts.Token);

        // Before the fix this surfaced as a RetryExhaustedException: the OperationCanceledException was caught by the
        // generic failure handler, restarted until the limit, then rewritten.
        _ = await act.Should().ThrowAsync<OperationCanceledException>();

        policy.PipelineFailureDecisions.Should().Be(0, "cancellation must not consume a restart attempt");
        inner.Attempts.Should().Be(1, "the node must not be restarted after cancellation");
    }

    [Fact]
    public async Task CancellationDuringTheRetryDelay_ThrowsRatherThanRetrying()
    {
        // Cancellation during the restart delay must propagate instead of being followed by another run.
        var policy = new RecordingPolicy { CancelDuringRetryDelay = true };
        var context = CreateContext(policy, RetryBackoff.Constant(TimeSpan.FromSeconds(30)));
        using var cts = new CancellationTokenSource();
        policy.CancellationSource = cts;

        var (strategy, inner) = CreateStrategy(_ => Fail(new InvalidOperationException("boom")));
        var stream = await strategy.ExecuteAsync(Input(), Node, context, "test-node", cts.Token);

        var act = () => DrainAsync(stream, cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        inner.Attempts.Should().Be(1, "the node must not be restarted once cancellation is requested");
    }

    /// <summary>
    ///     The rethrow is deliberately narrow: it fires only when the pipeline's own token is cancelled. A node that
    ///     uses its own token internally — a per-call timeout, say — still gets the resilience behaviour it asks for.
    /// </summary>
    [Fact]
    public async Task CancellationOfAnUnrelatedToken_IsStillHandledAsAFailure()
    {
        var policy = new RecordingPolicy();
        var context = CreateContext(policy);
        using var cts = new CancellationTokenSource();
        using var unrelated = new CancellationTokenSource();
        await unrelated.CancelAsync();

        var stream = await CreateStrategy(_ => Fail(new OperationCanceledException(unrelated.Token))).Strategy
            .ExecuteAsync(Input(), Node, context, "test-node", cts.Token);

        var act = () => DrainAsync(stream, cts.Token);

        _ = await act.Should().ThrowAsync<RetryExhaustedException>();
        policy.PipelineFailureDecisions.Should().BeGreaterThan(0, "an unrelated timeout is a failure the policy should rule on");
    }

    private static PipelineContext CreateContext(IResiliencePolicy policy, RetryBackoff restartBackoff = default)
    {
        var context = new PipelineContext(new PipelineContextConfiguration(ResiliencePolicy: policy));

        context.ExecutionConfiguration.Resilience = PipelineResilienceOptions.None with
        {
            NodeRestart = new NodeRestartOptions { MaxRestarts = 3, MaxReplayWindow = 128, Backoff = restartBackoff },
        };

        return context;
    }

    private static (ResilientExecutionStrategy Strategy, StubInnerStrategy Inner) CreateStrategy(Func<CancellationToken, IAsyncEnumerable<int>> produce)
    {
        StubInnerStrategy inner = new(produce);
        return (new ResilientExecutionStrategy(inner), inner);
    }

    private static IDataStream<int> Input() =>

        // The stub strategy ignores its input, so the test exercises only the restart loop.
        new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([0], "input");

    private static async Task<List<int>> DrainAsync(IDataStream<int> stream, CancellationToken cancellationToken)
    {
        List<int> received = [];

        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            received.Add(item);
        }

        return received;
    }

    /// <summary>
    ///     Yields the given items, then cancels the pipeline and observes the token, as a node should.
    /// </summary>
    private static async IAsyncEnumerable<int> ProduceThenCancel(IEnumerable<int> items, CancellationTokenSource cts,
        [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await cts.CancelAsync();
        ct.ThrowIfCancellationRequested();
    }

    private sealed class StubInnerStrategy(Func<CancellationToken, IAsyncEnumerable<int>> produce) : IResumableExecutionStrategy
    {
        public int Attempts { get; private set; }

        public Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(IDataStream<TIn> input, ITransformNode<TIn, TOut> node, PipelineContext context,
            string nodeId, CancellationToken cancellationToken)
        {
            Attempts++;
            var stream = new DataStream<int>(produce(cancellationToken), "stub");
            return Task.FromResult((IDataStream<TOut>)(object)stream);
        }

        public Task<IDataStream<TOut>> ExecuteFromAsync<TIn, TOut>(IDataStream<TIn> input, long offset, RestartCheckpoint checkpoint,
            ITransformNode<TIn, TOut> node, PipelineContext context, string nodeId, CancellationToken cancellationToken) =>
            ExecuteAsync(input, node, context, nodeId, cancellationToken);
    }

    private sealed class PassthroughNode : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item);
    }

    private sealed class RecordingPolicy : IResiliencePolicy
    {
        public int PipelineFailureDecisions { get; private set; }

        public bool CancelDuringRetryDelay { get; init; }

        public CancellationTokenSource? CancellationSource { get; set; }

        public ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);

        public ValueTask<ResilienceDecision> DecideRestartAsync(StreamFailure failure, CancellationToken cancellationToken)
        {
            PipelineFailureDecisions++;

            // A shutdown arriving just as the restart backoff begins.
            if (CancelDuringRetryDelay)
                CancellationSource?.Cancel();

            return ValueTask.FromResult(failure.CanRestart
                ? ResilienceDecision.RestartNode
                : ResilienceDecision.Fail);
        }

        public ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ResilienceDecision.Fail);
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    private static async IAsyncEnumerable<int> Produce(IEnumerable<int> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
    }

    private static async IAsyncEnumerable<int> Fail(Exception exception)
    {
        if (exception is not null)
            throw exception;

        yield break;
    }
#pragma warning restore CS1998
}
