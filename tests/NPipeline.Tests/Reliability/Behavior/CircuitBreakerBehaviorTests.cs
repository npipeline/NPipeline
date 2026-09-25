using AwesomeAssertions;
using Microsoft.Extensions.Time.Testing;
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
///     What the circuit breaker does to a running pipeline: it guards item attempts (B2), counts only transient
///     failures (B6), trips on a rate (B3), outlives a run (B4, D-2), and pauses only when asked to (D-4).
/// </summary>
public sealed class CircuitBreakerBehaviorTests
{
    public static TheoryData<string> Strategies => ["sequential", "parallel-ordered", "parallel-unordered"];

    [Fact]
    public async Task Breaker_StopsItemRetries_OnceItOpens()
    {
        var transform = new FlakyTransform(100);
        var observer = new RecordingObserver();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, new CollectingSink<int>(), [1], "sequential");

            _ = b.WithResilience(t, o => o with
            {
                ItemRetry = new ItemRetryOptions { MaxRetries = 10 },
                CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 3 },
            });
        }, observer);

        var failure = await act.Should().ThrowAsync<Exception>();

        transform.TotalAttempts.Should().Be(3, "the fourth attempt is refused by the open breaker");
        Chain(failure.Which).Should().Contain(e => e is CircuitBreakerOpenException);

        observer.CircuitChanges.Should().ContainSingle()
            .Which.Should().Match<CircuitStateChangedEvent>(e => e.NodeId == "transform" && e.State == CircuitState.Open);
    }

    [Theory]
    [MemberData(nameof(Strategies))]
    public async Task Breaker_StopsCallsToADeadDependency_ForEveryStrategy(string strategy)
    {
        var transform = new FlakyTransform(100);
        var items = Enumerable.Range(1, 200).ToArray();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, new CollectingSink<int>(), items, strategy);

            _ = b.WithResilience(t, o => o with
            {
                ItemRetry = ItemRetryOptions.None,
                OnItemFailure = ItemFailureAction.Skip,
                CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 5 },
            });
        });

        var failure = await act.Should().ThrowAsync<Exception>();

        Chain(failure.Which).Should().Contain(e => e is CircuitBreakerOpenException,
            "an open breaker fails the node rather than skipping every remaining item");

        transform.TotalAttempts.Should().BeLessThan(items.Length);
    }

    [Fact]
    public async Task FluentSkipRule_DoesNotSkipWhileTheBreakerIsOpen()
    {
        // C15: a fluent policy's rules match CircuitBreakerOpenException too. Without the breaker check, every
        // remaining item is skipped as fast as the CPU allows, which is exactly what the breaker prevents.
        var transform = new FlakyTransform(1000);
        var sink = new CollectingSink<int>();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, sink, Enumerable.Range(1, 200), "sequential");

            _ = b.WithResilience(t, o => o with { CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 3 } });
            _ = b.AddResiliencePolicy(t, ResiliencePolicyBuilder.ForNode<FlakyTransform, int>().OnAny().Skip().Build());
        });

        var failure = await act.Should().ThrowAsync<Exception>();

        Chain(failure.Which).Should().Contain(e => e is CircuitBreakerOpenException);
        transform.TotalAttempts.Should().BeLessThan(10, "once the breaker opens no further calls are made");
        sink.Items.Should().BeEmpty();
    }

    [Fact]
    public async Task FluentDeadLetterRule_DoesNotDeadLetterWhileTheBreakerIsOpen()
    {
        var transform = new FlakyTransform(1000);
        var deadLetters = new CollectingDeadLetterSink();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, new CollectingSink<int>(), Enumerable.Range(1, 200), "sequential");

            _ = b.AddDeadLetterSink(deadLetters);
            _ = b.WithResilience(t, o => o with { CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 3 } });
            _ = b.AddResiliencePolicy(t, ResiliencePolicyBuilder.ForNode<FlakyTransform, int>().OnAny().DeadLetter().Build());
        });

        _ = await act.Should().ThrowAsync<Exception>();

        // Only the failures the breaker itself admitted (its threshold) can be dead-lettered; the refusals fail.
        deadLetters.Envelopes.Should().HaveCountLessThanOrEqualTo(3);
        transform.TotalAttempts.Should().BeLessThan(10);
    }

    [Fact]
    public async Task FluentRetryRule_DoesNotRetryARefusedAttempt()
    {
        var transform = new FlakyTransform(1000);

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, new CollectingSink<int>(), Enumerable.Range(1, 200), "sequential");

            _ = b.WithResilience(t, o => o with { CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 3 } });
            _ = b.AddResiliencePolicy(t, ResiliencePolicyBuilder.ForNode<FlakyTransform, int>().OnAny().Retry(1000).Build());
        });

        var failure = await act.Should().ThrowAsync<Exception>();

        Chain(failure.Which).Should().Contain(e => e is CircuitBreakerOpenException);
        transform.TotalAttempts.Should().BeLessThan(10);
    }

    [Fact]
    public async Task PermanentFailures_DoNotTripTheBreaker()
    {
        var transform = new PermanentFailureTransform();
        var sink = new CollectingSink<int>();
        var observer = new RecordingObserver();

        await BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, sink, Enumerable.Range(1, 20), "sequential");

            _ = b.WithResilience(t, o => o with
            {
                OnItemFailure = ItemFailureAction.Skip,
                CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 2 },
            });
        }, observer);

        transform.Attempts.Should().Be(20, "a malformed record says nothing about the dependency");
        sink.Items.Should().Equal(Enumerable.Range(1, 20).Where(i => i % 2 == 0));
        observer.CircuitChanges.Should().BeEmpty();
    }

    [Fact]
    public async Task FailureRate_TripsOnceMinimumCallsWereSeen()
    {
        var transform = new EveryOtherFailsTransform();
        var observer = new RecordingObserver();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, new CollectingSink<int>(), Enumerable.Range(1, 100), "sequential");

            _ = b.WithResilience(t, o => o with
            {
                ItemRetry = ItemRetryOptions.None,
                OnItemFailure = ItemFailureAction.Skip,
                CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = null, FailureRate = 0.5, MinimumCalls = 10 },
            });
        }, observer);

        _ = await act.Should().ThrowAsync<Exception>();

        transform.Attempts.Should().Be(10, "the tenth attempt brings the window to five failures in ten calls");
        observer.CircuitChanges.Should().ContainSingle().Which.State.Should().Be(CircuitState.Open);
    }

    [Fact]
    public async Task Breaker_OutlivesARun_WhenRunsShareAFactory()
    {
        var runner = PipelineRunner.Create();
        var time = new FakeTimeProvider();

        var breaker = new CircuitBreakerOptions { ConsecutiveFailures = 2, OpenDuration = TimeSpan.FromMinutes(1) };

        // Run 1: the dependency is down, and the breaker opens.
        var firstRun = () => BehaviorPipeline.RunAsync(b => Configure(b, new FlakyTransform(100)), runner: runner);
        _ = await firstRun.Should().ThrowAsync<Exception>();

        // Run 2, straight after: the dependency is back, but the breaker is still open, so no attempt is made.
        var recovered = new FlakyTransform(0);
        var secondRun = () => BehaviorPipeline.RunAsync(b => Configure(b, recovered), runner: runner);
        var refused = await secondRun.Should().ThrowAsync<Exception>();

        Chain(refused.Which).Should().Contain(e => e is CircuitBreakerOpenException);
        recovered.TotalAttempts.Should().Be(0);

        // Run 3, after the open period: a probe succeeds and the run completes.
        time.Advance(TimeSpan.FromMinutes(1));
        var sink = new CollectingSink<int>();
        await BehaviorPipeline.RunAsync(b => Configure(b, recovered, sink), runner: runner);

        sink.Items.Should().Equal(1, 2, 3);

        // A different factory knows nothing of this breaker.
        var fresh = new FlakyTransform(0);
        await BehaviorPipeline.RunAsync(b => Configure(b, fresh));
        fresh.TotalAttempts.Should().Be(3);

        void Configure(PipelineBuilder b, FlakyTransform transform, CollectingSink<int>? collecting = null)
        {
            var t = Wire(b, transform, collecting ?? new CollectingSink<int>(), [1, 2, 3], "sequential");
            _ = b.WithResilience(t, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 5 }, CircuitBreaker = breaker, Time = time });
        }
    }

    [Fact]
    public async Task Pause_WaitsForTheBreaker_ThenCarriesOn()
    {
        var time = new FakeTimeProvider();
        var transform = new FailsFirstTransform(2);
        var sink = new CollectingSink<int>();
        var observer = new RecordingObserver();

        var run = BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, sink, [1, 2, 3], "sequential");

            _ = b.WithResilience(t, o => o with
            {
                ItemRetry = new ItemRetryOptions { MaxRetries = 5 },
                CircuitBreaker = new CircuitBreakerOptions
                {
                    ConsecutiveFailures = 2, OpenDuration = TimeSpan.FromSeconds(30), WhenOpen = BreakerOpenBehavior.Pause,
                },
                Time = time,
            });
        }, observer);

        await DriveClockUntilDone(time, run, TimeSpan.FromSeconds(5));

        sink.Items.Should().Equal(1, 2, 3);
        transform.Attempts.Should().Be(5, "item 1 failed twice, then a probe succeeded, then items 2 and 3");
        observer.CircuitChanges.Select(e => e.State).Should().Equal(CircuitState.Open, CircuitState.HalfOpen, CircuitState.Closed);
    }

    [Fact]
    public async Task Pause_GivesUpAfterMaxPause()
    {
        var time = new FakeTimeProvider();
        var transform = new FlakyTransform(100);

        var run = BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, new CollectingSink<int>(), [1], "sequential");

            _ = b.WithResilience(t, o => o with
            {
                ItemRetry = new ItemRetryOptions { MaxRetries = 5 },
                CircuitBreaker = new CircuitBreakerOptions
                {
                    ConsecutiveFailures = 1,
                    OpenDuration = TimeSpan.FromHours(1),
                    WhenOpen = BreakerOpenBehavior.Pause,
                    MaxPause = TimeSpan.FromMinutes(1),
                },
                Time = time,
            });
        });

        var act = () => DriveClockUntilDone(time, run, TimeSpan.FromSeconds(10));
        var failure = await act.Should().ThrowAsync<Exception>();

        Chain(failure.Which).OfType<CircuitBreakerOpenException>().Should().ContainSingle()
            .Which.Message.Should().Contain("MaxPause");

        transform.TotalAttempts.Should().Be(1);
    }

    [Fact]
    public async Task Pause_EndsWhenThePipelineIsCancelled()
    {
        using var cts = new CancellationTokenSource();
        var transform = new FlakyTransform(100);
        var observer = new RecordingObserver();

        var run = BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, transform, new CollectingSink<int>(), [1], "sequential");

            _ = b.WithResilience(t, o => o with
            {
                ItemRetry = new ItemRetryOptions { MaxRetries = 5, Backoff = RetryBackoff.None },
                CircuitBreaker = new CircuitBreakerOptions
                {
                    ConsecutiveFailures = 1, OpenDuration = TimeSpan.FromHours(1), WhenOpen = BreakerOpenBehavior.Pause,
                },
            });
        }, observer, cancellationToken: cts.Token);

        while (observer.CircuitChanges.Count == 0)
        {
            await Task.Delay(5);
        }

        await cts.CancelAsync();

        var act = () => run.WaitAsync(TimeSpan.FromSeconds(10));
        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    private static async Task DriveClockUntilDone(FakeTimeProvider time, Task run, TimeSpan step)
    {
        for (var i = 0; i < 1_000 && !run.IsCompleted; i++)
        {
            await Task.Delay(2);
            time.Advance(step);
        }

        await run.WaitAsync(TimeSpan.FromSeconds(10));
    }

    private static IEnumerable<Exception> Chain(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            yield return current;
        }
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

        if (strategy != "sequential")
        {
            _ = builder.WithExecutionStrategy(t, new ParallelExecutionStrategy(4));
            _ = builder.WithParallelOptions(t, new ParallelOptions(4, PreserveOrdering: strategy == "parallel-ordered"));
        }

        return t;
    }

    /// <summary>
    ///     Fails every odd item with a permanent error.
    /// </summary>
    private sealed class PermanentFailureTransform : TransformNode<int, int>
    {
        private int _attempts;

        public int Attempts => _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _attempts);

            if (item % 2 == 1)
                throw new FormatException($"malformed record {item}");

            return ValueTask.FromResult(item);
        }
    }

    /// <summary>
    ///     Fails every other attempt transiently.
    /// </summary>
    private sealed class EveryOtherFailsTransform : TransformNode<int, int>
    {
        private int _attempts;

        public int Attempts => _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _attempts) % 2 == 0)
                throw new TimeoutException("transient");

            return ValueTask.FromResult(item);
        }
    }

    /// <summary>
    ///     Fails its first few attempts transiently, whatever the item, then succeeds.
    /// </summary>
    private sealed class FailsFirstTransform(int failures) : TransformNode<int, int>
    {
        private int _attempts;

        public int Attempts => _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _attempts) <= failures)
                throw new TimeoutException("transient");

            return ValueTask.FromResult(item);
        }
    }
}
