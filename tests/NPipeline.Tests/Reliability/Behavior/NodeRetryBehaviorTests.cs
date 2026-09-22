using AwesomeAssertions;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Node retry (L3) and the retry events every layer must raise.
/// </summary>
public sealed class NodeRetryBehaviorTests
{
    [Fact]
    public async Task CancellationDuringTheNodeRetryDelay_StopsInsteadOfRunningTheNodeAgain()
    {
        using var cts = new CancellationTokenSource();
        var source = new FailsToOpenOnceSource([1]);
        var sink = new CollectingSink<int>();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            WireSource(b, source, sink);
            _ = b.AddResiliencePolicy(new RetryNodePolicy(cancelDuringDelay: cts));
        }, cancellationToken: cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        source.Opens.Should().Be(1, "a cancelled delay must not be followed by another attempt");
        sink.Items.Should().BeEmpty();
    }

    [Fact]
    public async Task NodeRetry_RaisesOnRetry()
    {
        var observer = new RecordingObserver();
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            WireSource(b, new FailsToOpenOnceSource([1]), sink);
            _ = b.AddResiliencePolicy(new RetryNodePolicy(cancelDuringDelay: null));
        }, observer: observer);

        sink.Items.Should().Equal([1]);
        observer.Retries.Should().ContainSingle()
            .Which.Should().Match<NodeRetryEvent>(e => e.NodeId == "source" && e.Attempt == 1 && e.LastException is TimeoutException);
    }

    [Fact]
    public async Task SequentialItemRetry_RaisesOnRetry()
    {
        var observer = new RecordingObserver();
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var t = b.AddTransform<FlakyTransform, int, int>("transform");
            var k = b.AddSink<CollectingSink<int>, int>("sink");

            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of([1]))
                .AddPreconfiguredNodeInstance(t.Id, new FlakyTransform(failuresPerItem: 2))
                .AddPreconfiguredNodeInstance(k.Id, sink)
                .Connect(s, t)
                .Connect(t, k)
                .AddResiliencePolicy(new FixedDecisionPolicy(ResilienceDecision.Retry))
                .WithRetryOptions(o => o with { MaxItemRetries = 3 });
        }, observer: observer);

        sink.Items.Should().Equal([1]);
        observer.Retries.Should().OnlyContain(e => e.Kind == RetryKind.ItemRetry && e.NodeId == "transform");
        observer.Retries.Select(e => e.Attempt).Should().Equal([1, 2]);
    }

    private static void WireSource(PipelineBuilder builder, FailsToOpenOnceSource source, CollectingSink<int> sink)
    {
        var s = builder.AddSource<FailsToOpenOnceSource, int>("source");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");

        _ = builder.AddPreconfiguredNodeInstance(s.Id, source)
            .AddPreconfiguredNodeInstance(k.Id, sink)
            .Connect(s, k);
    }

    /// <summary>
    ///     Retries any node failure. When given a token source, cancels it while the retry delay is being computed and
    ///     asks for a long delay, as a shutdown arriving mid-backoff would.
    /// </summary>
    private sealed class RetryNodePolicy(CancellationTokenSource? cancelDuringDelay) : ResiliencePolicyBase
    {
        public override Task<ResilienceDecision> DecideNodeFailureAsync(NodeDefinition nodeDefinition, INode node, Exception exception,
            PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(ResilienceDecision.Retry);
        }

        public override async ValueTask<TimeSpan> GetRetryDelayAsync(PipelineContext context, RetryKind retryKind, int attemptNumber,
            CancellationToken cancellationToken)
        {
            if (cancelDuringDelay is null)
                return TimeSpan.Zero;

            await cancelDuringDelay.CancelAsync();
            return TimeSpan.FromSeconds(30);
        }
    }
}
