using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Graph;

namespace NPipeline.Tests.DataFlow.DataStreams;

/// <summary>
///     Covers <see cref="CountingMulticastDataStream{T}" /> directly, constructed the way
///     <see cref="NPipeline.Execution.Services.DataStreamWrapperService" /> constructs it. Ported from the deleted
///     <c>MulticastDataStreamTests</c> (which exercised the unused, unfixed <c>MulticastDataStream&lt;T&gt;</c>, see
///     finding C51) plus the dedicated <c>DisposeAsync</c> edge-case regressions from finding C47.
/// </summary>
public sealed class CountingMulticastDataStreamTests
{
    [Fact]
    public void Create_WithValidParameters_CreatesPipeCorrectly()
    {
        var source = new DataStream<int>(GetTestStream(), "Source");
        BranchMetrics metrics = new();

        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 2, 10, metrics);

        _ = pipe.Should().NotBeNull();
        _ = pipe.GetDataType().Should().Be<int>();
        _ = pipe.Metrics.Should().Be(metrics);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithValidSubscriber_EnumeratesAllItems()
    {
        var source = new DataStream<int>(GetTestStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, null, metrics);
        List<int> enumeratedItems = [];

        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        _ = enumeratedItems.Should().BeEquivalentTo([1, 2, 3]);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithMultipleSubscribers_DeliversToAll()
    {
        var source = new DataStream<int>(GetTestStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 2, null, metrics);

        List<int> firstSubscriber = [];
        List<int> secondSubscriber = [];

        var firstTask = Task.Run(async () =>
        {
            await foreach (var item in pipe)
            {
                firstSubscriber.Add(item);
            }
        });

        var secondTask = Task.Run(async () =>
        {
            await foreach (var item in pipe)
            {
                secondSubscriber.Add(item);
            }
        });

        await Task.WhenAll(firstTask, secondTask);

        _ = firstSubscriber.Should().BeEquivalentTo([1, 2, 3]);
        _ = secondSubscriber.Should().BeEquivalentTo([1, 2, 3]);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithTooManySubscribers_ThrowsInvalidOperationException()
    {
        var source = new DataStream<int>(GetTestStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, null, metrics);

        await foreach (var item in pipe)
        {
            break;
        }

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in pipe)
            {
                // This should throw
            }
        });
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithBoundedBuffer_RespectsBufferLimit()
    {
        var source = new DataStream<int>(GetCancellableEndlessStream(), "Source");
        BranchMetrics metrics = new();
        await using var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, 2, metrics);
        List<int> enumeratedItems = [];

        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);

            if (enumeratedItems.Count >= 3)
                break;
        }

        _ = enumeratedItems.Count.Should().BeGreaterThanOrEqualTo(3);
        _ = metrics.PerSubscriberCapacity.Should().Be(2);
    }

    [Fact]
    public void GetDataType_ReturnsCorrectType()
    {
        var source = new DataStream<string>(GetStringTestStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<string>(source, new StatsCounter(), 1, null, metrics);

        _ = pipe.GetDataType().Should().Be<string>();
    }

    [Fact]
    public async Task WithNullItems_HandlesNullReferences()
    {
        var source = new DataStream<string?>(GetNullTestStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<string?>(source, new StatsCounter(), 1, null, metrics);
        List<string?> enumeratedItems = [];

        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        _ = enumeratedItems.Should().BeEquivalentTo("test1", null, "test3");
    }

    [Fact]
    public async Task WithExceptionInSource_PropagatesExceptionToAllSubscribers()
    {
        var source = new DataStream<int>(GetExceptionStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 2, null, metrics);

        Task firstTask = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in pipe)
            {
                // This should throw
            }
        });

        Task secondTask = Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in pipe)
            {
                // This should throw
            }
        });

        await Task.WhenAll(firstTask, secondTask);
    }

    [Fact]
    public void ImplementsIHasBranchMetrics_Interface()
    {
        var source = new DataStream<int>(GetTestStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, null, metrics);

        _ = pipe.Should().BeAssignableTo<IHasBranchMetrics>();
        _ = pipe.Metrics.Should().Be(metrics);
    }

    // --- C47: DisposeAsync edge cases ---

    [Fact]
    public async Task DisposeAsync_MidStream_DoesNotMarkFault()
    {
        // Item 1: cancelling the pump from DisposeAsync must not be reported as a pump fault. The source observes the
        // pump's token, so disposal surfaces as an OperationCanceledException inside the pump.
        var source = new DataStream<int>(GetCancellableEndlessStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, null, metrics);

        await using (var enumerator = pipe.GetAsyncEnumerator())
        {
            (await enumerator.MoveNextAsync()).Should().BeTrue();
        }

        await pipe.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        metrics.Faulted.Should().Be(0);
    }

    [Fact]
    public async Task DisposeAsync_CalledConcurrently_DoesNotThrow()
    {
        // Item 2: racing DisposeAsync calls must all complete without ObjectDisposedException.
        for (var i = 0; i < 200; i++)
        {
            var source = new DataStream<int>(GetCancellableEndlessStream(), "Source");
            var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, 4, new BranchMetrics());
            using var barrier = new Barrier(2);

            var first = Task.Run(async () =>
            {
                barrier.SignalAndWait();
                await pipe.DisposeAsync();
            });

            var second = Task.Run(async () =>
            {
                barrier.SignalAndWait();
                await pipe.DisposeAsync();
            });

            var act = async () => await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));
            await act.Should().NotThrowAsync();
        }
    }

    [Fact]
    public async Task SlowSubscriber_ReportsBacklog()
    {
        var source = new DataStream<int>(GetRangeStream(1_000), "Source");
        BranchMetrics metrics = new();
        await using var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 2, null, metrics);

        var fast = pipe.GetAsyncEnumerator();
        var slow = pipe.GetAsyncEnumerator();

        // The fast subscriber drains everything while the slow one has read nothing, so the pump samples a backlog.
        var fastCount = 0;

        while (await fast.MoveNextAsync())
            fastCount++;

        var slowCount = 0;

        while (await slow.MoveNextAsync())
            slowCount++;

        await fast.DisposeAsync();
        await slow.DisposeAsync();

        fastCount.Should().Be(1_000);
        slowCount.Should().Be(1_000);
        metrics.MaxAggregateBacklog.Should().BeGreaterThan(0, "the slow subscriber let items pile up in its channel");
    }

    [Fact]
    public async Task MoveNextAsync_AfterCompletion_DoesNotCountTheSubscriberTwice()
    {
        var source = new DataStream<int>(GetTestStream(), "Source");
        BranchMetrics metrics = new();
        await using var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, null, metrics);
        await using var enumerator = pipe.GetAsyncEnumerator();

        while (await enumerator.MoveNextAsync())
        {
        }

        (await enumerator.MoveNextAsync()).Should().BeFalse();
        metrics.SubscribersCompleted.Should().Be(1);
    }

    [Fact]
    public async Task EdgeView_ClaimedAgainBeforeAnyRead_DeliversEveryItem()
    {
        // Node retry re-executes a node that read nothing, so its edge must be claimable again.
        var edge = new Edge("source", "sink");
        var other = new Edge("source", "other");
        await using var pipe = new CountingMulticastDataStream<int>(
            new DataStream<int>(GetTestStream(), "Source"), new StatsCounter(), 2, null, new BranchMetrics(), [edge, other]);

        var firstAttempt = ((IDataStream<int>)pipe.GetEdgeView(edge)).GetAsyncEnumerator();
        await firstAttempt.DisposeAsync();

        var items = new List<int>();

        await foreach (var item in (IDataStream<int>)pipe.GetEdgeView(edge))
            items.Add(item);

        items.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task EdgeView_ClaimedAgainAfterARead_Throws()
    {
        var edge = new Edge("source", "sink");
        await using var pipe = new CountingMulticastDataStream<int>(
            new DataStream<int>(GetTestStream(), "Source"), new StatsCounter(), 1, null, new BranchMetrics(), [edge]);

        await using var reading = ((IDataStream<int>)pipe.GetEdgeView(edge)).GetAsyncEnumerator();
        (await reading.MoveNextAsync()).Should().BeTrue();

        var act = () => ((IDataStream<int>)pipe.GetEdgeView(edge)).GetAsyncEnumerator();
        act.Should().Throw<InvalidOperationException>().WithMessage("*already been consumed*");
    }

    [Fact]
    public async Task EdgeView_SupersededEnumerator_CannotStartReading()
    {
        var edge = new Edge("source", "sink");
        await using var pipe = new CountingMulticastDataStream<int>(
            new DataStream<int>(GetTestStream(), "Source"), new StatsCounter(), 1, null, new BranchMetrics(), [edge]);

        var stale = ((IDataStream<int>)pipe.GetEdgeView(edge)).GetAsyncEnumerator();
        await using var current = ((IDataStream<int>)pipe.GetEdgeView(edge)).GetAsyncEnumerator();

        var act = async () => await stale.MoveNextAsync();
        await act.Should().ThrowAsync<InvalidOperationException>();
        (await current.MoveNextAsync()).Should().BeTrue();
    }

    [Fact]
    public async Task ReleaseEdge_UnblocksAPumpStalledOnAnUnreadBoundedEdge()
    {
        var ignored = new Edge("source", "ignored");
        var reading = new Edge("source", "reading");
        await using var pipe = new CountingMulticastDataStream<int>(
            new DataStream<int>(GetRangeStream(100), "Source"), new StatsCounter(), 2, 4, new BranchMetrics(), [ignored, reading]);

        var readTask = Task.Run(async () =>
        {
            var count = 0;

            await foreach (var _ in (IDataStream<int>)pipe.GetEdgeView(reading))
                count++;

            return count;
        });

        // Nothing reads the ignored edge, so the pump stalls once its buffer of 4 fills.
        await Task.Delay(100);
        readTask.IsCompleted.Should().BeFalse();

        pipe.ReleaseEdge(ignored);

        (await readTask.WaitAsync(TimeSpan.FromSeconds(5))).Should().Be(100);

        var act = () => ((IDataStream<int>)pipe.GetEdgeView(ignored)).GetAsyncEnumerator();
        act.Should().Throw<InvalidOperationException>("a released edge cannot be read again");
    }

    private static async IAsyncEnumerable<int> GetCancellableEndlessStream(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var i = 0; ; i++)
        {
            yield return i;

            await Task.Delay(5, cancellationToken);
        }
    }

    private static async IAsyncEnumerable<int> GetRangeStream(int count)
    {
        for (var i = 0; i < count; i++)
            yield return i;

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<int> GetTestStream()
    {
        yield return 1;
        yield return 2;
        yield return 3;

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<string> GetStringTestStream()
    {
        yield return "test1";
        yield return "test2";

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<string?> GetNullTestStream()
    {
        yield return "test1";
        yield return null;
        yield return "test3";

        await Task.CompletedTask;
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    private static async IAsyncEnumerable<int> GetExceptionStream()
    {
        yield return 1;
        yield return 2;

        throw new InvalidOperationException("Test exception");
    }
#pragma warning restore CS1998
}
