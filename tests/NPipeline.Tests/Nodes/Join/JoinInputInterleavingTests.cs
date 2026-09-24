using AwesomeAssertions;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow.Branching;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.Nodes.Join;

/// <summary>
///     A join reads both of its inputs concurrently. Reading the left input to completion before touching the right
///     one starves the right side whenever the left input is live (Kafka, a change feed), and deadlocks a diamond
///     topology whose branch buffers are bounded.
/// </summary>
public sealed class JoinInputInterleavingTests
{
    private static readonly TimeSpan DeadlockTimeout = TimeSpan.FromSeconds(30);

    public sealed record L(int Id);

    public sealed record R(int Id, string Name);

    [KeySelector(typeof(L), nameof(L.Id))]
    [KeySelector(typeof(R), nameof(R.Id))]
    public sealed class J : KeyedJoinNode<int, L, R, string>
    {
        public override string CreateOutput(L l, R r) => $"{l.Id}:{r.Name}";
    }

    [Fact]
    public async Task Join_ReadsBothInputsConcurrently()
    {
        var sink = new CollectingSink<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var run = BehaviorPipeline.RunAsync(b =>
        {
            var l = b.AddSource<StreamingSource<L>, L>("left");
            var r = b.AddSource<StreamingSource<R>, R>("right");
            var j = b.AddJoin<J, L, R, string>("join");
            var k = b.AddSink<CollectingSink<string>, string>("sink");
            _ = b.AddPreconfiguredNodeInstance(l.Id, StreamingSource<L>.Unbounded([new L(1)]))
                .AddPreconfiguredNodeInstance(r.Id, StreamingSource<R>.Of([new R(1, "a")]))
                .AddPreconfiguredNodeInstance(k.Id, sink);
            _ = b.Connect(l, j).Connect(r, j).Connect(j, k);
        }, cancellationToken: cts.Token);

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (sink.Items.Count == 0 && DateTime.UtcNow < deadline) await Task.Delay(50);
        await cts.CancelAsync();
        try { await run; } catch { /* cancelled */ }
        sink.Items.Should().Equal("1:a");
    }

    [Fact]
    public async Task DiamondTopology_WithBoundedBranchBuffers_CompletesInsteadOfDeadlocking()
    {
        // S -> A, S -> B, A + B -> keyed join -> sink. The join must interleave A's and B's items
        // instead of waiting for A to complete: A's branch buffer is bounded, so the multicast pump
        // stalls once it fills, and A never completes.
        var sink = new CollectingSink<string>();
        const int itemCount = 200;

        var run = BehaviorPipeline.RunAsync(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var a = b.AddTransform<LeftTransform, int, LeftItem>("a");
            var bb = b.AddTransform<RightTransform, int, RightItem>("b");
            var j = b.AddJoin<IntJoin, LeftItem, RightItem, string>("join");
            var k = b.AddSink<CollectingSink<string>, string>("sink");
            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of(Enumerable.Range(1, itemCount)))
                .AddPreconfiguredNodeInstance(a.Id, new LeftTransform())
                .AddPreconfiguredNodeInstance(bb.Id, new RightTransform())
                .AddPreconfiguredNodeInstance(j.Id, new IntJoin())
                .AddPreconfiguredNodeInstance(k.Id, sink);
            _ = b.Connect(s, a).Connect(s, bb).Connect(a, j).Connect(bb, j).Connect(j, k);
            _ = b.WithBranchOptions("source", new BranchOptions(4));
        });

        var finished = await Task.WhenAny(run, Task.Delay(DeadlockTimeout));
        _ = finished.Should().BeSameAs(run, "the join must drain both branches concurrently, not sequentially");
        await run;

        sink.Items.Should().HaveCount(itemCount);
    }

    public sealed record LeftItem(int Id);

    public sealed record RightItem(int Id);

    private sealed class LeftTransform : TransformNode<int, LeftItem>
    {
        public override ValueTask<LeftItem> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new LeftItem(item));
    }

    private sealed class RightTransform : TransformNode<int, RightItem>
    {
        public override ValueTask<RightItem> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new RightItem(item));
    }

    [KeySelector(typeof(LeftItem), nameof(LeftItem.Id))]
    [KeySelector(typeof(RightItem), nameof(RightItem.Id))]
    private sealed class IntJoin : KeyedJoinNode<int, LeftItem, RightItem, string>
    {
        public override string CreateOutput(LeftItem l, RightItem r) => $"{l.Id}:{r.Id}";
    }
}