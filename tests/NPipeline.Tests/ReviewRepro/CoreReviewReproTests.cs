using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.ReviewRepro;

// TEMPORARY: reproductions for the core review. Each test asserts the CORRECT behaviour and is expected to FAIL today.
public sealed class CoreReviewReproTests
{
    private static readonly DateTime T0 = new(2023, 1, 1, 10, 0, 0, DateTimeKind.Utc);

// ---------- Watermark uses wall clock instead of extractor ----------
// R01, R02, R03 moved to Nodes/Aggregate/AggregateWatermarkTests.cs (C01/C02/C11).

// ---------- Window alignment on local ticks ----------
// R04, R05 moved to Nodes/Batching/WindowAssignerTests.cs (C25/C26).

// ---------- Periodic watermark ----------
// R06 moved to DataFlow/Watermarks/WatermarksTests.cs (C24).

// ---------- BatchAsync <=100ms ----------
// R07 moved to Nodes/Batching/BatchingTests.cs (C22).

    // ---------- TapNode per-item ConsumeAsync ----------
    // R08 moved to Nodes/Source/TapNodeTests.cs (C06).

    // ---------- Join inputs read sequentially ----------
    // R09 moved to JoinInputInterleavingTests.Join_ReadsBothInputsConcurrently (C03).

    // ---------- Interleave hides failure ----------
    // R10 moved to MergeStrategyTests.FanIn_FailureSurfacesWhileOtherInputOpen (C04).

    // ---------- Context resilience policy overwritten ----------
    private sealed class RetryOncePolicy : ResiliencePolicyBase
    {
        public int NodeDecisions;

        public override ValueTask<ResilienceDecision> DecideNodeFailureAsync(NodeFailure failure, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref NodeDecisions);
            return ValueTask.FromResult(ResilienceDecision.Retry);
        }
    }

    [Fact]
    public async Task R11_ContextPolicy_IsHonoured()
    {
        var policy = new RetryOncePolicy();
        await using var context = new PipelineContext(PipelineContextConfiguration.WithResilience(policy));
        var sink = new CollectingSink<int>();
        var def = new BehaviorPipeline(b =>
        {
            var s = b.AddSource<FailsToOpenOnceSource, int>("source");
            var k = b.AddSink<CollectingSink<int>, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(s.Id, new FailsToOpenOnceSource([1])).AddPreconfiguredNodeInstance(k.Id, sink).Connect(s, k);
            _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 1, Backoff = RetryBackoff.None } });
        });
        try { await PipelineRunner.Create().RunAsync(def, context); } catch { /* observed below */ }
        policy.NodeDecisions.Should().BeGreaterThan(0);
    }

    // ---------- L3 treats foreign OCE as cancellation ----------
    private sealed class TimesOutOnceSource : SourceNode<int>
    {
        public int Opens;

        public override IDataStream<int> OpenStream(PipelineContext c, CancellationToken ct) =>
            Interlocked.Increment(ref Opens) == 1
                ? throw new TaskCanceledException("HttpClient.Timeout elapsed")
                : StreamingSource<int>.Of([1]).OpenStream(c, ct);
    }

    [Fact]
    public async Task R12_NodeRetry_RetriesClientTimeout()
    {
        var source = new TimesOutOnceSource();
        var sink = new CollectingSink<int>();
        try
        {
            await BehaviorPipeline.RunAsync(b =>
            {
                var s = b.AddSource<TimesOutOnceSource, int>("source");
                var k = b.AddSink<CollectingSink<int>, int>("sink");
                _ = b.AddPreconfiguredNodeInstance(s.Id, source).AddPreconfiguredNodeInstance(k.Id, sink).Connect(s, k);
                _ = b.WithResilience(o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 1, Backoff = RetryBackoff.None } });
            });
        }
        catch { /* observed below */ }

        source.Opens.Should().Be(2);
    }

    // ---------- Batching strategy does not flush on time ----------
    // R13 moved to Execution/Strategies/BatchingExecutionStrategyTests.cs (C07).

    // ---------- Validation: no edges ----------
    // R14, R15 moved to Validation/BuilderRules/PipelineValidationTests.cs (C08/C20).

    // ---------- Fluent policy drains input while breaker open ----------
    [Fact]
    public async Task R16_FluentPolicy_DoesNotSkipWhileBreakerOpen()
    {
        var sink = new CollectingSink<int>();
        var transform = new FlakyTransform(1000);
        Exception? thrown = null;
        try
        {
            await BehaviorPipeline.RunAsync(b =>
            {
                var s = b.AddSource<StreamingSource<int>, int>("source");
                var t = b.AddTransform<FlakyTransform, int, int>("transform");
                var k = b.AddSink<CollectingSink<int>, int>("sink");
                _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of(Enumerable.Range(1, 200)))
                    .AddPreconfiguredNodeInstance(t.Id, transform).AddPreconfiguredNodeInstance(k.Id, sink)
                    .Connect(s, t).Connect(t, k);
                _ = b.WithResilience(t, o => o with { CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 3 } });
                _ = b.AddResiliencePolicy(t, ResiliencePolicyBuilder.ForNode<FlakyTransform, int>().OnAny().Skip().Build());
            });
        }
        catch (Exception ex) { thrown = ex; }

        transform.TotalAttempts.Should().BeLessThan(10, "once the breaker opens no further calls are made");
        thrown.Should().NotBeNull("an open breaker should fail the node rather than skipping the whole input");
    }
}
