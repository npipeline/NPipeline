using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
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
        }, observer);

        sink.Items.Should().Equal(1);

        observer.Retries.Should().ContainSingle()
            .Which.Should().Match<NodeRetryEvent>(e =>
                e.NodeId == "source" && e.Kind == RetryKind.NodeRetry && e.Attempt == 1 && e.LastException is TimeoutException);

        observer.Exhaustions.Should().BeEmpty("the retry succeeded");
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
                .AddPreconfiguredNodeInstance(t.Id, new FlakyTransform(2))
                .AddPreconfiguredNodeInstance(k.Id, sink)
                .Connect(s, t)
                .Connect(t, k)
                .WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 3 } });
        }, observer);

        sink.Items.Should().Equal(1);
        observer.Retries.Should().OnlyContain(e => e.Kind == RetryKind.ItemRetry && e.NodeId == "transform");
        observer.Retries.Select(e => e.Attempt).Should().Equal(1, 2);
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
        var observer = new RecordingObserver();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<FailsToOpenSource, int>("source");
            var k = b.AddSink<CollectingSink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(s.Id, source).AddPreconfiguredNodeInstance(k.Id, new CollectingSink<int>()).Connect(s, k);
            _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 2, Backoff = RetryBackoff.None } });
        }, observer);

        var thrown = await act.Should().ThrowAsync<Exception>();

        var exhausted = thrown.Which;

        while (exhausted is not null and not RetryExhaustedException)
        {
            exhausted = exhausted.InnerException;
        }

        exhausted.Should().BeOfType<RetryExhaustedException>().Which.NodeId.Should().Be("source");
        source.Opens.Should().Be(3);
        observer.Retries.Should().HaveCount(2).And.OnlyContain(e => e.Kind == RetryKind.NodeRetry);

        observer.Exhaustions.Should().ContainSingle()
            .Which.Should().Match<RetryExhaustedEvent>(e =>
                e.NodeId == "source" && e.Kind == RetryKind.NodeRetry && e.Attempts == 3 && e.LastException is TimeoutException);
    }

    [Fact]
    public async Task NodeRetry_DoesNotReExecuteASinkThatHasConsumedInput()
    {
        // C15: a sink that fails mid-stream cannot be executed again. Its forward-only input has already been partly
        // read, so a second execution would lose the items consumed so far or read them twice.
        var sink = new FailsAfterItemsSink(2);
        var observer = new RecordingObserver();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var k = b.AddSink<FailsAfterItemsSink, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of([1, 2, 3, 4, 5])).AddPreconfiguredNodeInstance(k.Id, sink).Connect(s, k);
            _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 3, Backoff = RetryBackoff.None } });
        }, observer);

        var thrown = await act.Should().ThrowAsync<Exception>();

        thrown.Which.Should().NotBeOfType<RetryExhaustedException>();
        sink.Executions.Should().Be(1, "node retry covers setup only; a sink that has read input is not run again");
        sink.Items.Should().Equal(1, 2);
        observer.Retries.Should().BeEmpty();
    }

    [Fact]
    public async Task NodeRetry_ReExecutesASinkThatFailedBeforeConsumingInput()
    {
        var sink = new FailsToStartOnceSink();

        await BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var k = b.AddSink<FailsToStartOnceSink, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of([1, 2, 3])).AddPreconfiguredNodeInstance(k.Id, sink).Connect(s, k);
            _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 1, Backoff = RetryBackoff.None } });
        });

        sink.Executions.Should().Be(2);
        sink.Items.Should().Equal(1, 2, 3);
    }

    /// <summary>
    ///     Records items, and fails with a transient exception once it has received <c>failAfter</c> of them.
    /// </summary>
    private sealed class FailsAfterItemsSink(int failAfter) : SinkNode<int>
    {
        private readonly List<int> _items = [];
        private int _executions;

        public int Executions => _executions;

        public IReadOnlyList<int> Items => _items;

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _executions);

            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _items.Add(item);

                if (_items.Count == failAfter)
                    throw new TimeoutException("transient write failure");
            }
        }
    }

    /// <summary>
    ///     Fails its first execution before reading any input, as a sink that cannot open its connection would.
    /// </summary>
    private sealed class FailsToStartOnceSink : SinkNode<int>
    {
        private readonly List<int> _items = [];
        private int _executions;

        public int Executions => _executions;

        public IReadOnlyList<int> Items => _items;

        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _executions) == 1)
                throw new TimeoutException("transient failure opening the connection");

            await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _items.Add(item);
            }
        }
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
