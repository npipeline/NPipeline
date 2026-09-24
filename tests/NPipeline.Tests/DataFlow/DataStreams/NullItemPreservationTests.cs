using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataStreams;
using NPipeline.DataFlow.Routing;
using NPipeline.Graph;

namespace NPipeline.Tests.DataFlow.DataStreams;

/// <summary>
///     <see cref="IDataStream.ToAsyncEnumerable" /> is declared as <c>IAsyncEnumerable&lt;object?&gt;</c>, so every
///     implementation must carry nulls through. Several decorators used to filter them out, which meant the item count
///     of a stream depended on which decorator happened to be wrapping it, and joins and aggregates over a nullable
///     item type silently lost data.
/// </summary>
public sealed class NullItemPreservationTests
{
    private static readonly string?[] Expected = ["a", null, "b", null, "c"];

    [Fact]
    public async Task DataStream_ToAsyncEnumerable_PreservesNulls()
    {
        await using var stream = new DataStream<string?>(SourceWithNulls(), "Source");

        var items = await CollectAsync(stream);

        items.Should().Equal(Expected);
    }

    [Fact]
    public async Task InMemoryDataStream_ToAsyncEnumerable_PreservesNulls()
    {
        await using var stream = new NPipeline.DataFlow.DataStreams.InMemoryDataStream<string?>(Expected, "Source");

        var items = await CollectAsync(stream);

        items.Should().Equal(Expected);
    }

    [Fact]
    public async Task DataStreamBase_ToAsyncEnumerable_PreservesNulls()
    {
        await using var stream = new PassthroughDataStream<string?>(new DataStream<string?>(SourceWithNulls(), "Source"));

        var items = await CollectAsync(stream);

        items.Should().Equal(Expected);
    }

    [Fact]
    public async Task AsyncEnumerableDataStream_ToAsyncEnumerable_PreservesNulls()
    {
        await using var stream = new AsyncEnumerableDataStream<string?>(SourceWithNulls(), "Source");

        var items = await CollectAsync(stream);

        items.Should().Equal(Expected);
    }

    [Fact]
    public async Task CountingPassthroughDataStream_ToAsyncEnumerable_PreservesNullsAndCountsThem()
    {
        StatsCounter counter = new();
        await using var stream = new CountingPassthroughDataStream<string?>(new DataStream<string?>(SourceWithNulls(), "Source"), counter);

        var items = await CollectAsync(stream);

        items.Should().Equal(Expected);
        counter.Total.Should().Be(Expected.Length);
    }

    [Fact]
    public async Task CountingMulticastDataStream_ToAsyncEnumerable_PreservesNulls()
    {
        StatsCounter counter = new();
        BranchMetrics metrics = new();

        await using var stream = new CountingMulticastDataStream<string?>(
            new DataStream<string?>(SourceWithNulls(), "Source"),
            counter,
            1,
            null,
            metrics);

        var items = await CollectAsync(stream);

        items.Should().Equal(Expected);
        counter.Total.Should().Be(Expected.Length);
    }

    [Fact]
    public async Task MulticastDataStream_ToAsyncEnumerable_PreservesNulls()
    {
        BranchMetrics metrics = new();
        await using var stream = MulticastDataStream<string?>.Create(SourceWithNulls(), 1, null, "Source", metrics);

        var items = await CollectAsync(stream);

        items.Should().Equal(Expected);
    }

    [Fact]
    public async Task CountingConditionalMulticastDataStream_ToAsyncEnumerable_PreservesNulls()
    {
        StatsCounter counter = new();
        BranchMetrics metrics = new();
        Edge edge = new("source", "target", "all");
        RouteOptions<string?> options = new();
        _ = options.When("all", static _ => true);

        await using var stream = new CountingConditionalMulticastDataStream<string?>(
            new DataStream<string?>(SourceWithNulls(), "Source"),
            counter,
            [edge],
            null,
            options,
            metrics);

        var items = await CollectAsync(stream);

        items.Should().Equal(Expected);
        counter.Total.Should().Be(Expected.Length);
    }

    [Fact]
    public async Task CountingConditionalMulticastDataStream_EdgeView_PreservesNulls()
    {
        StatsCounter counter = new();
        BranchMetrics metrics = new();
        Edge edge = new("source", "target", "all");
        RouteOptions<string?> options = new();
        _ = options.When("all", static _ => true);

        await using var stream = new CountingConditionalMulticastDataStream<string?>(
            new DataStream<string?>(SourceWithNulls(), "Source"),
            counter,
            [edge],
            null,
            options,
            metrics);

        var items = await CollectAsync(stream.GetEdgeView(edge));

        items.Should().Equal(Expected);
    }

    /// <summary>
    ///     Every wrapper must report the same item count for the same logical stream. This is the invariant that the
    ///     null filters broke: a node's output count changed depending on whether it was wrapped for counting or
    ///     branching.
    /// </summary>
    [Fact]
    public async Task AllStreamImplementations_AgreeOnItemCount()
    {
        StatsCounter counter = new();

        IDataStream[] streams =
        [
            new DataStream<string?>(SourceWithNulls(), "Source"),
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<string?>(Expected, "Source"),
            new PassthroughDataStream<string?>(new DataStream<string?>(SourceWithNulls(), "Source")),
            new AsyncEnumerableDataStream<string?>(SourceWithNulls(), "Source"),
            new CountingPassthroughDataStream<string?>(new DataStream<string?>(SourceWithNulls(), "Source"), counter),
            new CountingMulticastDataStream<string?>(new DataStream<string?>(SourceWithNulls(), "Source"), counter, 1, null, new BranchMetrics()),
            MulticastDataStream<string?>.Create(SourceWithNulls(), 1, null, "Source", new BranchMetrics()),
        ];

        try
        {
            foreach (var stream in streams)
            {
                var items = await CollectAsync(stream);
                items.Should().Equal(Expected, $"stream '{stream.StreamName}' must not drop null items");
            }
        }
        finally
        {
            foreach (var stream in streams)
            {
                await stream.DisposeAsync();
            }
        }
    }

    private static async Task<List<object?>> CollectAsync(IDataStream stream)
    {
        List<object?> items = [];

        await foreach (var item in stream.ToAsyncEnumerable())
        {
            items.Add(item);
        }

        return items;
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    private static async IAsyncEnumerable<string?> SourceWithNulls()
    {
        foreach (var item in Expected)
        {
            yield return item;
        }
    }
#pragma warning restore CS1998

    /// <summary>
    ///     Minimal concrete <see cref="DataStreamBase{T}" /> so the base class's own
    ///     <see cref="DataStreamBase{T}.ToAsyncEnumerable" /> is exercised directly.
    /// </summary>
    private sealed class PassthroughDataStream<T>(IDataStream<T> inner) : DataStreamBase<T>(inner)
    {
        public override IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) => Inner.GetAsyncEnumerator(cancellationToken);
    }
}
