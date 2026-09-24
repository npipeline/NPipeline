using AwesomeAssertions;
using Microsoft.Extensions.Time.Testing;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Parallelism;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Item-level retry (L1), asserted by what happens to items rather than by what is configured. C1 went unnoticed
///     because the existing tests checked <c>MaxItemRetries == 3</c> and never that a failing item was retried.
/// </summary>
public sealed class ItemRetryBehaviorTests
{
    [Fact]
    public async Task DefaultProfile_RetriesATransientItemFailure()
    {
        // The docs promise that under the Default profile a failed item is retried automatically, with no explicit
        // configuration (C1).
        var transform = new FlakyTransform(1);
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            _ = b.WithOptimizationProfile(PipelineOptimizationProfile.Default);
            _ = Wire(b, StreamingSource<int>.Of([1]), transform, sink);
        });

        sink.Items.Should().Equal([1], "a transient failure must be retried under the Default profile");
        transform.AttemptsFor(1).Should().Be(2);
    }

    [Fact]
    public async Task DefaultProfile_FailsAPermanentItemFailureAtOnce()
    {
        var transform = new ThrowingTransform(() => new InvalidOperationException("a bug, not an outage"));

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var t = b.AddTransform<ThrowingTransform, int, int>("transform");
            var k = b.AddSink<CollectingSink<int>, int>("sink");

            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of([1]))
                .AddPreconfiguredNodeInstance(t.Id, transform)
                .AddPreconfiguredNodeInstance(k.Id, new CollectingSink<int>())
                .Connect(s, t)
                .Connect(t, k);
        });

        _ = await act.Should().ThrowAsync<Exception>();
        transform.Attempts.Should().Be(1, "the classifier limits the Default profile's retries to transient failures");
    }

    [Fact]
    public async Task HighThroughputProfile_DoesNotRetry()
    {
        var transform = new FlakyTransform(1);

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            _ = b.WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);
            _ = Wire(b, StreamingSource<int>.Of([1]), transform, new CollectingSink<int>());
        });

        _ = await act.Should().ThrowAsync<Exception>();
        transform.AttemptsFor(1).Should().Be(1);
    }

    [Fact]
    public async Task ExhaustedItemRetries_ThrowRetryExhausted()
    {
        // C4: exhaustion used to surface as a bare InvalidOperationException.
        var transform = new FlakyTransform(int.MaxValue);

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            _ = Wire(b, StreamingSource<int>.Of([1]), transform, new CollectingSink<int>());
            _ = b.WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 2 } });
        });

        var thrown = await act.Should().ThrowAsync<Exception>();
        var exhausted = FindInChain<RetryExhaustedException>(thrown.Which);
        exhausted.Should().NotBeNull();
        exhausted!.NodeId.Should().Be("transform");
        exhausted.InnerException.Should().BeOfType<TimeoutException>();
        transform.AttemptsFor(1).Should().Be(3);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeadLetter_WithoutADeadLetterSink_FailsLoudly(bool parallel)
    {
        var sink = new CollectingSink<int>();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, StreamingSource<int>.Of([1, 2]), new FlakyTransform(int.MaxValue), sink);

            if (parallel)
                _ = b.WithExecutionStrategy(t, new ParallelExecutionStrategy(2));

            _ = b.AddResiliencePolicy(new FixedDecisionPolicy(ResilienceDecision.DeadLetter));
        });

        // Silent data loss is worse than a loud failure: with nowhere to dead-letter to, the item must not vanish.
        var thrown = await act.Should().ThrowAsync<Exception>();
        thrown.Which.GetBaseException().Should().BeOfType<TimeoutException>();
        FindInChain<DeadLetterSinkNotConfiguredException>(thrown.Which).Should().NotBeNull();
        sink.Items.Should().BeEmpty();
    }

    [Fact]
    public async Task DeadLetterOption_WithoutADeadLetterSink_FailsBeforeAnyNodeRuns()
    {
        var transform = new FlakyTransform(0);

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            _ = Wire(b, StreamingSource<int>.Of([1]), transform, new CollectingSink<int>());
            _ = b.WithResilience(o => o with { OnItemFailure = ItemFailureAction.DeadLetter });
        });

        (await act.Should().ThrowAsync<DeadLetterSinkNotConfiguredException>()).Which.NodeId.Should().Be("transform");
        transform.TotalAttempts.Should().Be(0);
    }

    [Fact]
    public async Task DeadLetterOption_WithASinkSuppliedOnlyAtRunTime_Runs()
    {
        // Why the check is made at run setup and not by Build(): the builder has no sink here, but the context does.
        var deadLetters = new CollectingDeadLetterSink();
        var sink = new CollectingSink<int>();
        await using var context = new PipelineContext(PipelineContextConfiguration.WithErrorHandling(deadLetters));

        await PipelineRunner.Create().RunAsync(new BehaviorPipeline(b =>
        {
            _ = Wire(b, StreamingSource<int>.Of([1, 2]), new FailsOnTransform(1), sink);
            _ = b.WithResilience(o => o with { OnItemFailure = ItemFailureAction.DeadLetter });
        }), context);

        sink.Items.Should().Equal(2);
        deadLetters.Envelopes.Should().ContainSingle();
    }

    [Theory]
    [InlineData(ItemFailureAction.Skip, false)]
    [InlineData(ItemFailureAction.DeadLetter, true)]
    public async Task OnItemFailure_HandlesAFailureThatIsNotRetried(ItemFailureAction action, bool deadLettered)
    {
        var sink = new CollectingSink<int>();
        var deadLetters = new CollectingDeadLetterSink();

        await BehaviorPipeline.RunAsync(b =>
        {
            _ = Wire(b, StreamingSource<int>.Of([1, 2]), new FailsOnTransform(1), sink);
            _ = b.WithResilience(o => o with { OnItemFailure = action });
            _ = b.AddDeadLetterSink(deadLetters);
        });

        sink.Items.Should().Equal(2);

        deadLetters.Envelopes.Should().HaveCount(deadLettered
            ? 1
            : 0);
    }

    [Fact]
    public async Task PolicyRetryRule_UnderHighThroughput_RetriesItsOwnCountThenDeadLetters()
    {
        var transform = new FlakyTransform(int.MaxValue);
        var deadLetters = new CollectingDeadLetterSink();

        var policy = ResiliencePolicyBuilder.ForNode<FlakyTransform, int>()
            .On<TimeoutException>().Retry(5)
            .Build();

        await BehaviorPipeline.RunAsync(b =>
        {
            // HighThroughput leaves ItemRetry.MaxRetries at 0. That is advice to the policy, not a cap on it (C3).
            _ = b.WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);
            _ = Wire(b, StreamingSource<int>.Of([1]), transform, new CollectingSink<int>());
            _ = b.AddResiliencePolicy(policy);
            _ = b.AddDeadLetterSink(deadLetters);
        });

        transform.AttemptsFor(1).Should().Be(6, "the rule asked for 5 retries after the first attempt");
        deadLetters.Envelopes.Should().ContainSingle("the rule's fallback after exhausting its retries is dead-letter");
    }

    [Fact]
    public async Task PolicyThatAlwaysRetries_HitsTheSafetyCeiling()
    {
        var transform = new FlakyTransform(int.MaxValue);

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            // HighThroughput has no backoff, so the hundred retries take no time.
            _ = b.WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);
            _ = Wire(b, StreamingSource<int>.Of([1]), transform, new CollectingSink<int>());
            _ = b.AddResiliencePolicy(new FixedDecisionPolicy(ResilienceDecision.Retry));
        });

        var thrown = await act.Should().ThrowAsync<Exception>();
        FindInChain<InvalidOperationException>(thrown.Which)!.Message.Should().Contain(nameof(FixedDecisionPolicy));
        transform.AttemptsFor(1).Should().Be(101);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ItemRetries_UseTheBackoffBeforeEachRetry(bool parallel)
    {
        var backoff = new RecordingBackoff();
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, StreamingSource<int>.Of([1]), new FlakyTransform(2), sink);

            if (parallel)
                _ = b.WithExecutionStrategy(t, new ParallelExecutionStrategy(2));

            _ = b.WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 3, Backoff = backoff.Backoff } });
        });

        sink.Items.Should().Equal(1);
        backoff.Requests.Should().Equal([1, 2], "without a backoff the retries spin against a failing dependency");
    }

    [Fact]
    public async Task PerNodeBackoff_IsHonored()
    {
        // C6: the delay strategy used to be built once per run from the pipeline-wide options.
        var pipelineBackoff = new RecordingBackoff();
        var nodeBackoff = new RecordingBackoff();
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, StreamingSource<int>.Of([1]), new FlakyTransform(2), sink);
            _ = b.WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 3, Backoff = pipelineBackoff.Backoff } });
            _ = b.WithResilience(t, o => o with { ItemRetry = o.ItemRetry with { Backoff = nodeBackoff.Backoff } });
        });

        sink.Items.Should().Equal(1);
        nodeBackoff.Requests.Should().Equal(1, 2);
        pipelineBackoff.Requests.Should().BeEmpty();
    }

    [Fact]
    public async Task RetryDelays_WaitOnThePipelinesClock()
    {
        var time = new FakeTimeProvider();
        var sink = new CollectingSink<int>();

        var run = BehaviorPipeline.RunAsync(b =>
        {
            _ = Wire(b, StreamingSource<int>.Of([1]), new FlakyTransform(1), sink);

            _ = b.WithResilience(o => o with
            {
                ItemRetry = new ItemRetryOptions { MaxRetries = 1, Backoff = RetryBackoff.Constant(TimeSpan.FromHours(1)) },
                Time = time,
            });
        });

        // The hour-long delay only passes when the fake clock is advanced.
        await Task.Delay(100);
        run.IsCompleted.Should().BeFalse();

        time.Advance(TimeSpan.FromHours(1));
        await run.WaitAsync(TimeSpan.FromSeconds(10));

        sink.Items.Should().Equal(1);
    }

    [Fact]
    public async Task PerNodePolicy_DecidesForItsNodeOnly()
    {
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, StreamingSource<int>.Of([1, 2]), new FailsOnTransform(1), sink);
            _ = b.AddResiliencePolicy(t, new FixedDecisionPolicy(ResilienceDecision.Skip));
        });

        sink.Items.Should().Equal(2);
    }

    [Fact]
    public void ItemRetry_OnASink_IsABuildError()
    {
        // C11: this used to be accepted and silently ignored.
        var builder = new PipelineBuilder();
        var s = builder.AddSource<StreamingSource<int>, int>("source");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");
        _ = builder.Connect(s, k);
        _ = builder.WithResilience(k, o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = 3 } });

        var act = () => builder.Build();

        act.Should().Throw<PipelineValidationException>().WithMessage("*'sink'*ItemRetry*");
    }

    [Fact]
    public void NodeRetry_OnASink_IsAllowed()
    {
        var builder = new PipelineBuilder();
        var s = builder.AddSource<StreamingSource<int>, int>("source");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");
        _ = builder.Connect(s, k);
        _ = builder.WithResilience(k, o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 1 } });

        var act = () => builder.Build();

        act.Should().NotThrow("a sink inherits the pipeline's ItemRetry, which it does not change");
    }

    private static T? FindInChain<T>(Exception? exception) where T : Exception
    {
        for (; exception is not null; exception = exception.InnerException)
        {
            if (exception is T match)
                return match;
        }

        return null;
    }

    private static TransformNodeHandle<int, int> Wire<TTransform>(PipelineBuilder builder, StreamingSource<int> source, TTransform transform,
        CollectingSink<int> sink) where TTransform : ITransformNode<int, int>
    {
        var s = builder.AddSource<StreamingSource<int>, int>("source");
        var t = builder.AddTransform<TTransform, int, int>("transform");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");

        _ = builder.AddPreconfiguredNodeInstance(s.Id, source)
            .AddPreconfiguredNodeInstance(t.Id, transform)
            .AddPreconfiguredNodeInstance(k.Id, sink)
            .Connect(s, t)
            .Connect(t, k);

        return t;
    }

    private sealed class ThrowingTransform(Func<Exception> failure) : TransformNode<int, int>
    {
        private int _attempts;

        public int Attempts => _attempts;

        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _attempts);
            throw failure();
        }
    }

    /// <summary>
    ///     Fails one item permanently and passes every other item through.
    /// </summary>
    private sealed class FailsOnTransform(int failingItem) : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            item == failingItem
                ? throw new FormatException($"item {item} is malformed")
                : ValueTask.FromResult(item);
    }
}
