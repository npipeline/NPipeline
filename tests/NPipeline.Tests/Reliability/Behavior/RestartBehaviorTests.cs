using AwesomeAssertions;
using NPipeline.Pipeline;

// Tests skipped with a defect ID pin known bugs; the phase that fixes each one removes its skip.
#pragma warning disable xUnit1004

namespace NPipeline.Tests.Reliability.Behavior;

/// <summary>
///     Node restart (L2) over forward-only inputs, which is what every real connector produces.
/// </summary>
public sealed class RestartBehaviorTests
{
    [Fact(Skip = Defects.R1)]
    public async Task ResilientNode_OverAnUnboundedSource_ProducesOutputBeforeItsInputEnds()
    {
        var sink = new CollectingSink<int>();
        using var cts = new CancellationTokenSource();

        var run = BehaviorPipeline.RunAsync(b => WireResilient(b, StreamingSource<int>.Unbounded([1, 2, 3]), sink, maxMaterializedItems: 100),
            cts.Token);

        try
        {
            // The source never completes, so if the node waits for the end of its input nothing ever arrives.
            var arrived = await Task.WhenAny(sink.FirstItemReceived, Task.Delay(TimeSpan.FromSeconds(5))) == sink.FirstItemReceived;

            arrived.Should().BeTrue("a resilient node must stream, not buffer its whole input first");
        }
        finally
        {
            await cts.CancelAsync();

            try
            {
                await run;
            }
            catch (OperationCanceledException)
            {
                // Expected: the test stops the never-ending pipeline.
            }
        }
    }

    [Fact(Skip = Defects.R2)]
    public async Task ResilientNode_WithZeroFailures_SucceedsOnAnInputLongerThanTheReplayCap()
    {
        const int cap = 10_000;
        var sink = new CollectingSink<int>();

        await BehaviorPipeline.RunAsync(b => WireResilient(b, StreamingSource<int>.Of(Enumerable.Range(0, cap + 1)), sink, cap));

        // The cap bounds what can be replayed after a failure. It is not a limit on the input's length.
        sink.Items.Should().HaveCount(cap + 1);
    }

    private static void WireResilient(PipelineBuilder builder, StreamingSource<int> source, CollectingSink<int> sink, int maxMaterializedItems)
    {
        var s = builder.AddSource<StreamingSource<int>, int>("source");
        var t = builder.AddTransform<FlakyTransform, int, int>("transform");
        var k = builder.AddSink<CollectingSink<int>, int>("sink");

        _ = builder.AddPreconfiguredNodeInstance(s.Id, source)
            .AddPreconfiguredNodeInstance(t.Id, new FlakyTransform(failuresPerItem: 0))
            .AddPreconfiguredNodeInstance(k.Id, sink)
            .Connect(s, t)
            .Connect(t, k)
            .AddResiliencePolicy(new RestartOnFailurePolicy())
            .WithRetryOptions(o => o with { MaxNodeRestartAttempts = 1, MaxMaterializedItems = maxMaterializedItems });

        _ = t.WithResilience(builder);
    }
}
