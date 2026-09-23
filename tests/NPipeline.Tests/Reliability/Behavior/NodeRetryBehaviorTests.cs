using AwesomeAssertions;
using NPipeline.Execution;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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

        // A shutdown arriving mid-backoff: the backoff cancels the run and asks for a long delay.
        var backoff = RetryBackoff.Custom(_ =>
        {
            cts.Cancel();
            return TimeSpan.FromSeconds(30);
        });

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            WireSource(b, source, sink);
            _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 1, Backoff = backoff } });
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
            _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 1, Backoff = RetryBackoff.None } });
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
                .WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 3 } });
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

    [Fact]
    public async Task NodeRetry_IsNotAttemptedForAPermanentFailure()
    {
        var source = new FailsToOpenSource(() => new InvalidOperationException("misconfigured"));

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<FailsToOpenSource, int>("source");
            var k = b.AddSink<CollectingSink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(s.Id, source).AddPreconfiguredNodeInstance(k.Id, new CollectingSink<int>()).Connect(s, k);
            _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 3, Backoff = RetryBackoff.None } });
        });

        _ = await act.Should().ThrowAsync<Exception>();
        source.Opens.Should().Be(1);
    }

    [Fact]
    public async Task ExhaustedNodeRetries_ThrowRetryExhausted()
    {
        var source = new FailsToOpenSource(() => new TimeoutException("still down"));

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<FailsToOpenSource, int>("source");
            var k = b.AddSink<CollectingSink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(s.Id, source).AddPreconfiguredNodeInstance(k.Id, new CollectingSink<int>()).Connect(s, k);
            _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 2, Backoff = RetryBackoff.None } });
        });

        var thrown = await act.Should().ThrowAsync<Exception>();

        var exhausted = thrown.Which;
        while (exhausted is not null and not RetryExhaustedException)
            exhausted = exhausted.InnerException;

        exhausted.Should().BeOfType<RetryExhaustedException>().Which.NodeId.Should().Be("source");
        source.Opens.Should().Be(3);
    }

    private sealed class FailsToOpenSource(Func<Exception> failure) : SourceNode<int>
    {
        private int _opens;

        public int Opens => _opens;

        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _opens);
            throw failure();
        }
    }
}
