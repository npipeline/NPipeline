using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataStreams;
using NPipeline.DataFlow.Routing;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Tests.Core.Execution;

namespace NPipeline.Tests.Nodes.Route;

/// <summary>
///     Measures the route pump's per-item allocations. The measurement is process-wide, so the class runs in the
///     non-parallel <see cref="ProcessWideCounterGroup" /> collection.
/// </summary>
[Collection(ProcessWideCounterGroup.Name)]
public sealed class RouteNodeAllocationTests
{
    [Fact]
    public async Task Pump_AllocatesNegligibleMemoryPerItem()
    {
        // P04: the pump used to allocate a Task[], a closure, and (in AllMatches mode) a HashSet<int> per routed
        // item. Precomputing rule -> channel-index arrays and writing synchronously in the common case should make
        // the steady-state per-item cost close to zero, for both unbounded channels (no blocked write, ever).
        const int itemCount = 100_000;

        var edgeEven = new Edge("src", "even", "even");
        var edgeOdd = new Edge("src", "odd", "odd");

        RouteOptions<int> options = new();
        options.When("even", static x => x % 2 == 0);
        options.When("odd", static x => x % 2 != 0);

        StatsCounter counter = new();
        BranchMetrics metrics = new();

        // The pump and its two consumers each run on their own pooled thread, so a per-thread allocation counter
        // would miss most of what we want to measure. GetTotalAllocatedBytes(precise: true) is process-wide, which is
        // why this class opts out of parallel execution. The pump starts in the constructor, so take the baseline
        // before constructing the stream.
        var before = GC.GetTotalAllocatedBytes(precise: true);

        await using var stream = new CountingConditionalMulticastDataStream<int>(
            new DataStream<int>(Produce(itemCount), "Source"),
            counter,
            [edgeEven, edgeOdd],
            null,
            options,
            metrics);

        var evenView = (IDataStream<int>)stream.GetEdgeView(edgeEven);
        var oddView = (IDataStream<int>)stream.GetEdgeView(edgeOdd);

        var evenTask = Task.Run(async () =>
        {
            var n = 0;

            await foreach (var _ in evenView.WithCancellation(CancellationToken.None))
            {
                n++;
            }

            return n;
        });

        var oddTask = Task.Run(async () =>
        {
            var n = 0;

            await foreach (var _ in oddView.WithCancellation(CancellationToken.None))
            {
                n++;
            }

            return n;
        });

        var counts = await Task.WhenAll(evenTask, oddTask);
        var after = GC.GetTotalAllocatedBytes(precise: true);

        counts.Sum().Should().Be(itemCount);

        // Generous bound: unavoidable Channel<T>/ValueTask machinery still allocates something per item, so this
        // asserts "no extra per-item allocation on top of that baseline", not a specific byte count. The old
        // closure/Task[]/HashSet path added well over this on top of the same baseline.
        (after - before).Should().BeLessThan(itemCount * 300, "the pump must not allocate extra objects per routed item anymore");
    }

    private static async IAsyncEnumerable<int> Produce(int count)
    {
        for (var i = 0; i < count; i++)
        {
            yield return i;
        }

        await Task.CompletedTask;
    }
}
