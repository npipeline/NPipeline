using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataStreams;

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
        var source = new DataStream<int>(GetLongRunningStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, 2, metrics);
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
        // Item 1: cancelling _cts from DisposeAsync must not be reported as a pump fault.
        var source = new DataStream<int>(GetLongRunningStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, null, metrics);

        var reader = Task.Run(async () =>
        {
            try
            {
                await foreach (var _ in pipe)
                {
                }
            }
            catch (OperationCanceledException)
            {
                // Expected once disposal cancels the pump mid-stream.
            }
        });

        await Task.Delay(20);
        await pipe.DisposeAsync();
        await reader;

        metrics.Faulted.Should().Be(0);
    }

    [Fact]
    public async Task DisposeAsync_CalledConcurrently_DoesNotThrow()
    {
        // Item 2: two concurrent DisposeAsync calls must both complete without ObjectDisposedException.
        var source = new DataStream<int>(GetTestStream(), "Source");
        BranchMetrics metrics = new();
        var pipe = new CountingMulticastDataStream<int>(source, new StatsCounter(), 1, null, metrics);

        var tasks = new Task[1_000];

        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = i % 2 == 0
                ? pipe.DisposeAsync().AsTask()
                : pipe.DisposeAsync().AsTask();
        }

        var act = async () => await Task.WhenAll(tasks);
        await act.Should().NotThrowAsync();
    }

    private static async IAsyncEnumerable<int> GetTestStream()
    {
        yield return 1;
        yield return 2;
        yield return 3;

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<int> GetLongRunningStream()
    {
        for (var i = 1; i <= 200; i++)
        {
            yield return i;

            await Task.Delay(10);
        }
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
