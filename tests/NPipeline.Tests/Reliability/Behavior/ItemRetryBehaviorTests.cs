using System.Diagnostics;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Configuration.RetryDelay;
using NPipeline.ErrorHandling;
using NPipeline.Execution.RetryDelay;
using NPipeline.Extensions.Parallelism;
using NPipeline.Graph;
using NPipeline.Pipeline;
using NPipeline.Resilience;

// Tests skipped with a defect ID pin known bugs; the phase that fixes each one removes its skip.
#pragma warning disable xUnit1004

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Item-level retry (L1), asserted by what happens to items rather than by what is configured. C1 went unnoticed
///     because the existing tests checked <c>MaxItemRetries == 3</c> and never that a failing item was retried.
/// </summary>
public sealed class ItemRetryBehaviorTests
{
    [Fact(Skip = Defects.C1)]
    public async Task DefaultProfile_RetriesATransientItemFailure()
    {
        // The docs promise that under the Default profile a failed item is "retried automatically before the
        // pipeline fails. No explicit configuration is required."
        var transform = new FlakyTransform(failuresPerItem: 1);
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            _ = b.WithOptimizationProfile(PipelineOptimizationProfile.Default);
            _ = Wire(b, StreamingSource<int>.Of([1]), transform, sink);
        });

        sink.Items.Should().Equal([1], "a transient failure must be retried under the Default profile");
        transform.AttemptsFor(1).Should().Be(2);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DeadLetter_WithoutADeadLetterSink_FailsLoudly(bool parallel)
    {
        var sink = new CollectingSink<int>();

        var act = () => BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, StreamingSource<int>.Of([1, 2]), new FlakyTransform(failuresPerItem: int.MaxValue), sink);

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

    [Fact(Skip = Defects.C3)]
    public async Task PolicyRetryRule_UnderHighThroughput_RetriesItsOwnCountThenDeadLetters()
    {
        var transform = new FlakyTransform(failuresPerItem: int.MaxValue);
        var deadLetters = new CollectingDeadLetterSink();

        var policy = ResiliencePolicyBuilder.ForNode<FlakyTransform, int>()
            .On<TimeoutException>().Retry(5)
            .Build();

        await BehaviorPipeline.RunAsync(b =>
        {
            // HighThroughput leaves MaxItemRetries at 0, which today overrides the rule and fails on the first retry.
            _ = b.WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);
            _ = Wire(b, StreamingSource<int>.Of([1]), transform, new CollectingSink<int>());
            _ = b.AddResiliencePolicy(policy);
            _ = b.AddDeadLetterSink(deadLetters);
        });

        transform.AttemptsFor(1).Should().Be(6, "the rule asked for 5 retries after the first attempt");
        deadLetters.Envelopes.Should().ContainSingle("the rule's fallback after exhausting its retries is dead-letter");
    }

    [Fact]
    public async Task SequentialItemRetries_AskForABackoffBeforeEachRetry()
    {
        // Control for the parallel test below: the sequential path already backs off.
        var policy = new FixedDecisionPolicy(ResilienceDecision.Retry);
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            _ = Wire(b, StreamingSource<int>.Of([1]), new FlakyTransform(failuresPerItem: 2), sink);
            _ = b.AddResiliencePolicy(policy);
            _ = b.WithRetryOptions(o => o with { MaxItemRetries = 3 });
        });

        sink.Items.Should().Equal([1]);
        policy.DelayRequests.Should().Equal([1, 2]);
    }

    [Fact]
    public async Task ParallelItemRetries_AskForABackoffBeforeEachRetry()
    {
        var policy = new FixedDecisionPolicy(ResilienceDecision.Retry);
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, StreamingSource<int>.Of([1]), new FlakyTransform(failuresPerItem: 2), sink);
            _ = b.WithExecutionStrategy(t, new ParallelExecutionStrategy(2));
            _ = b.AddResiliencePolicy(policy);
            _ = b.WithRetryOptions(o => o with { MaxItemRetries = 3 });
        });

        sink.Items.Should().Equal([1]);
        policy.DelayRequests.Should().Equal([1, 2], "without a backoff the parallel path spins against a failing dependency");
    }

    [Fact(Skip = Defects.C6)]
    public async Task PerNodeBackoff_IsHonored()
    {
        var delay = TimeSpan.FromMilliseconds(150);
        var sink = new CollectingSink<int>();

        var nodeOptions = PipelineRetryOptions.Default with
        {
            MaxItemRetries = 3,
            DelayStrategyConfiguration = new RetryDelayStrategyConfiguration(BackoffStrategies.FixedDelay(delay), JitterStrategies.NoJitter()),
        };

        var stopwatch = Stopwatch.StartNew();

        await BehaviorPipeline.RunAsync(b =>
        {
            var t = Wire(b, StreamingSource<int>.Of([1]), new FlakyTransform(failuresPerItem: 2), sink);
            _ = b.AddResiliencePolicy(new FixedDecisionPolicy(ResilienceDecision.Retry));

            // The pipeline-wide options carry no delay; only the node override does.
            _ = b.WithRetryOptions(o => o with { MaxItemRetries = 3, DelayStrategyConfiguration = null });
            _ = b.WithRetryOptions(t, nodeOptions);
        });

        stopwatch.Stop();

        sink.Items.Should().Equal([1]);

        // Two retries at 150ms each. Asserting a lower bound only keeps this robust on a loaded machine.
        stopwatch.Elapsed.Should().BeGreaterThan(TimeSpan.FromMilliseconds(250), "the node's own backoff must be applied");
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

    private static TransformNodeHandle<int, int> Wire(PipelineBuilder builder, StreamingSource<int> source, FlakyTransform transform,
        CollectingSink<int> sink)
    {
        var s = builder.AddSource<StreamingSource<int>, int>("source");
        var t = builder.AddTransform<FlakyTransform, int, int>("transform");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");

        _ = builder.AddPreconfiguredNodeInstance(s.Id, source)
            .AddPreconfiguredNodeInstance(t.Id, transform)
            .AddPreconfiguredNodeInstance(k.Id, sink)
            .Connect(s, t)
            .Connect(t, k);

        return t;
    }
}
