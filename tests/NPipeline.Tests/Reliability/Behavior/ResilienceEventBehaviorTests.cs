using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Parallelism;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using ParallelOptions = NPipeline.Extensions.Parallelism.ParallelOptions;

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     The observer events each layer raises, and item handling that must be identical for the sequential and parallel
///     strategies now that both use the core item executor.
/// </summary>
public sealed class ResilienceEventBehaviorTests
{
    public static TheoryData<string> Strategies => ["sequential", "parallel-blocking", "parallel-unordered", "parallel-drop-oldest", "parallel-drop-newest"];

    [Theory]
    [MemberData(nameof(Strategies))]
    public async Task ExhaustedItemRetries_RaiseOnRetryForEachRetry_ThenOnRetryExhausted(string strategy)
    {
        var observer = new RecordingObserver();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, new FlakyTransform(failuresPerItem: 100), new CollectingSink<int>(), [7], strategy);
            _ = b.WithResilience(t, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 2 } });
        }, observer: observer);

        _ = await act.Should().ThrowAsync<Exception>();

        observer.Retries.Should().OnlyContain(e => e.Kind == RetryKind.ItemRetry && e.NodeId == "transform");
        observer.Retries.Select(e => e.Attempt).Should().Equal([1, 2]);
        observer.Exhaustions.Should().ContainSingle()
            .Which.Should().Match<RetryExhaustedEvent>(e =>
                e.NodeId == "transform" && e.Kind == RetryKind.ItemRetry && e.Attempts == 3 && e.LastException is TimeoutException);
    }

    [Theory]
    [MemberData(nameof(Strategies))]
    public async Task AFailureThatIsNotRetried_RaisesNoExhaustionEvent(string strategy)
    {
        var observer = new RecordingObserver();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            // A permanent failure: the default classifier declines to retry it, so nothing was exhausted.
            var t = Wire(b, new FailOn(2, () => new InvalidOperationException("bad record")), new CollectingSink<int>(), [2], strategy);
            _ = b.WithResilience(t, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 2 } });
        }, observer: observer);

        _ = await act.Should().ThrowAsync<Exception>();

        observer.Retries.Should().BeEmpty();
        observer.Exhaustions.Should().BeEmpty();
    }

    [Theory]
    [MemberData(nameof(Strategies))]
    public async Task SkippedItem_OfAValueTypeOutput_IsNotEmittedAsDefault(string strategy)
    {
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            _ = Wire(b, new FailOn(2, () => new InvalidOperationException("bad record")), sink, [1, 2, 3], strategy);
            _ = b.AddResiliencePolicy(new FixedDecisionPolicy(ResilienceDecision.Skip));
        });

        sink.Items.Should().BeEquivalentTo([10, 30], "a skipped item produces no output, not default(int)");
    }

    [Theory]
    [MemberData(nameof(Strategies))]
    public async Task NullOutput_FromAReferenceTypeTransform_IsEmitted(string strategy)
    {
        var sink = new CollectingSink<string?>();

        await BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var t = b.AddTransform<NullForEven, int, string?>("transform");
            var k = b.AddSink<CollectingSink<string?>, string?>("sink");

            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of([1, 2, 3]))
                .AddPreconfiguredNodeInstance(t.Id, new NullForEven())
                .AddPreconfiguredNodeInstance(k.Id, sink)
                .Connect(s, t)
                .Connect(t, k);

            ApplyStrategy(b, t, strategy);
        });

        sink.Items.Should().BeEquivalentTo(["1", null, "3"], "null is a legitimate output, not a skipped item");
    }

    [Fact]
    public async Task ParallelItemRetries_FeedTheParallelMetrics()
    {
        var runner = PipelineRunner.Create();
        await using var context = new PipelineContext();

        await runner.RunAsync(new BehaviorPipeline(b =>
        {
            var t = Wire(b, new FlakyTransform(failuresPerItem: 2), new CollectingSink<int>(), [1, 2, 3], "parallel-blocking");
            _ = b.WithResilience(t, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 3 } });
        }), context);

        var metrics = context.GetParallelMetrics("transform");

        metrics.ItemsWithRetry.Should().Be(3);
        metrics.RetryEvents.Should().Be(6);
        metrics.MaxItemRetryAttempts.Should().Be(2);
    }

    [Fact]
    public async Task ExhaustedNodeRestarts_RaiseOnRetryExhaustedForTheRestartLayer()
    {
        var observer = new RecordingObserver();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, new FlakyTransform(failuresPerItem: 100), new CollectingSink<int>(), [1], "sequential");
            _ = b.WithResilience(t, o => o with
            {
                ItemRetry = ItemRetryOptions.None,
                NodeRestart = new NodeRestartOptions { MaxRestarts = 1, MaxReplayWindow = 100, Backoff = RetryBackoff.None },
            });
        }, observer: observer);

        _ = await act.Should().ThrowAsync<Exception>();

        observer.Retries.Should().ContainSingle().Which.Kind.Should().Be(RetryKind.NodeRestart);
        observer.Exhaustions.Should().ContainSingle()
            .Which.Should().Match<RetryExhaustedEvent>(e => e.NodeId == "transform" && e.Kind == RetryKind.NodeRestart && e.Attempts == 2);
    }

    [Fact]
    public async Task TrippedBreaker_RaisesOnCircuitStateChanged()
    {
        var observer = new RecordingObserver();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, new FlakyTransform(failuresPerItem: 100), new CollectingSink<int>(), [1], "sequential");
            _ = b.WithResilience(t, o => o with
            {
                ItemRetry = new ItemRetryOptions { MaxRetries = 5 },
                CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 1 },
            });
        }, observer: observer);

        _ = await act.Should().ThrowAsync<Exception>();

        observer.CircuitChanges.Should().ContainSingle()
            .Which.Should().Match<CircuitStateChangedEvent>(e =>
                e.NodeId == "transform" && e.PreviousState == CircuitState.Closed && e.State == CircuitState.Open);
    }

    private static TransformNodeHandle<int, int> Wire<TTransform>(PipelineBuilder builder, TTransform transform, CollectingSink<int> sink,
        IEnumerable<int> items, string strategy)
        where TTransform : ITransformNode<int, int>
    {
        var s = builder.AddSource<StreamingSource<int>, int>("source");
        var t = builder.AddTransform<TTransform, int, int>("transform");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");

        _ = builder.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of(items))
            .AddPreconfiguredNodeInstance(t.Id, transform)
            .AddPreconfiguredNodeInstance(k.Id, sink)
            .Connect(s, t)
            .Connect(t, k);

        ApplyStrategy(builder, t, strategy);
        return t;
    }

    private static void ApplyStrategy(PipelineBuilder builder, NodeHandle transform, string strategy)
    {
        if (strategy == "sequential")
            return;

        _ = builder.WithExecutionStrategy(transform, new ParallelExecutionStrategy(2));

        var options = strategy switch
        {
            "parallel-unordered" => new ParallelOptions(2, PreserveOrdering: false),
            "parallel-drop-oldest" => new ParallelOptions(2, MaxQueueLength: 1_000, QueuePolicy: BoundedQueuePolicy.DropOldest),
            "parallel-drop-newest" => new ParallelOptions(2, MaxQueueLength: 1_000, QueuePolicy: BoundedQueuePolicy.DropNewest),
            _ => new ParallelOptions(2),
        };

        _ = builder.WithParallelOptions(transform, options);
    }

    /// <summary>
    ///     Multiplies each item by ten, and fails on one item.
    /// </summary>
    private sealed class FailOn(int failingItem, Func<Exception> failure) : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == failingItem)
                throw failure();

            return ValueTask.FromResult(item * 10);
        }
    }

    private sealed class NullForEven : TransformNode<int, string?>
    {
        public override ValueTask<string?> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(item % 2 == 0 ? null : item.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
    }
}
