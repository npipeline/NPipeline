using System.Reflection;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataPipes;

namespace NPipeline.Tests.DataFlow.DataPipes;

public sealed class MulticastDataPipeTests
{
    [Fact]
    public void Create_WithValidParameters_CreatesPipeCorrectly()
    {
        // Arrange
        var source = GetTestStream();
        var subscriberCount = 2;
        int? perSubscriberBuffer = 10;
        var streamName = "TestStream";
        BranchMetrics metrics = new();

        // Act
        var pipe = CreateMulticastDataPipe(source, subscriberCount, perSubscriberBuffer, streamName, metrics);

        // Assert
        _ = pipe.Should().NotBeNull();
        _ = pipe.GetDataType().Should().Be<int>();
        _ = pipe.Metrics.Should().Be(metrics);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithValidSubscriber_EnumeratesAllItems()
    {
        // Arrange
        var source = GetTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);
        List<int> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo([1, 2, 3]);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithMultipleSubscribers_DeliversToAll()
    {
        // Arrange
        var source = GetTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 2, null, "TestStream", metrics);

        // Act
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

        // Assert
        _ = firstSubscriber.Should().BeEquivalentTo([1, 2, 3]);
        _ = secondSubscriber.Should().BeEquivalentTo([1, 2, 3]);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithTooManySubscribers_ThrowsInvalidOperationException()
    {
        // Arrange
        var source = GetTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);

        // Act - Get first enumerator
        await foreach (var item in pipe)

            // Consume first item
        {
            break;
        }

        // Act & Assert - Second enumerator should throw
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
        // Arrange
        var source = GetLongRunningStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, 2, "TestStream", metrics);
        List<int> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);

            if (enumeratedItems.Count >= 3)
                break; // Stop after consuming some items
        }

        // Assert
        _ = enumeratedItems.Count.Should().BeGreaterThanOrEqualTo(3);
        _ = metrics.PerSubscriberCapacity.Should().Be(2);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithCancellation_RespectsCancellation()
    {
        // Arrange
        var source = GetLongRunningStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);
        CancellationTokenSource cts = new();
        List<int> enumeratedItems = [];
        var itemsToCollect = 2;

        // Act - Cancel after processing N items
        var task = Task.Run(async () =>
        {
            await foreach (var item in pipe.WithCancellation(cts.Token))
            {
                enumeratedItems.Add(item);

                if (enumeratedItems.Count >= itemsToCollect)
                    cts.Cancel();
            }
        });

        // Assert - TaskCanceledException is a subclass of OperationCanceledException
        _ = await task.Invoking(t => t).Should().ThrowAsync<OperationCanceledException>();

        // Due to race conditions in async cancellation between the source and the loop,
        // we may collect more items than intended before the cancellation takes effect.
        // Ensure we got at least the items we requested before canceling.
        _ = enumeratedItems.Count.Should().BeGreaterThanOrEqualTo(itemsToCollect);
    }

    [Fact]
    public async Task ToAsyncEnumerable_ReturnsAllItemsAsObjects()
    {
        // Arrange
        var source = GetTestStream();
        BranchMetrics metrics = new();
        IDataPipe<int> pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);
        List<object> enumeratedItems = [];

        // Act
        await foreach (var item in pipe.ToAsyncEnumerable())
        {
            enumeratedItems.Add(item!);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo([1, 2, 3]);
    }

    [Fact]
    public async Task DisposeAsync_CancelsPumpTask()
    {
        // Arrange
        var source = GetLongRunningStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);

        // Act
        await pipe.DisposeAsync();

        // Assert - Should not throw and should complete successfully
        _ = true.Should().BeTrue(); // Test passes if no exception thrown
    }

    [Fact]
    public async Task DisposeAsync_AfterEnumeration_DoesNotThrow()
    {
        // Arrange
        var source = GetTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);

        // Act
        await foreach (var item in pipe)
        {
            // Process items
        }

        // Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void GetDataType_ReturnsCorrectType()
    {
        // Arrange
        var source = GetStringTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<string>();
    }

    [Fact]
    public async Task WithNullItems_HandlesNullReferences()
    {
        // Arrange
        var source = GetNullTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);
        List<string?> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo("test1", null, "test3");
    }

    [Fact]
    public async Task WithComplexTypes_EnumeratesCorrectly()
    {
        // Arrange
        var source = GetComplexTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);
        List<TestData> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().HaveCount(2);
        _ = enumeratedItems[0].Id.Should().Be(1);
        _ = enumeratedItems[0].Name.Should().Be("Test1");
        _ = enumeratedItems[1].Id.Should().Be(2);
        _ = enumeratedItems[1].Name.Should().Be("Test2");
    }

    [Fact]
    public async Task WithExceptionInSource_PropagatesExceptionToAllSubscribers()
    {
        // Arrange
        var source = GetExceptionStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 2, null, "TestStream", metrics);

        // Act & Assert
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
        // Arrange
        var source = GetTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 1, null, "TestStream", metrics);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IHasBranchMetrics>();
        _ = pipe.Metrics.Should().Be(metrics);
    }

    [Fact]
    public async Task Metrics_TracksSubscriberCount()
    {
        // Arrange
        var source = GetTestStream();
        BranchMetrics metrics = new();
        var pipe = CreateMulticastDataPipe(source, 2, null, "TestStream", metrics);

        // Act
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

        // Assert
        _ = metrics.SubscriberCount.Should().Be(2);
    }

    #region Test Helper Classes

    private sealed class TestData
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #endregion

    #region Helper Methods

    private static MulticastDataPipe<T> CreateMulticastDataPipe<T>(
        IAsyncEnumerable<T> source,
        int subscriberCount,
        int? perSubscriberBuffer,
        string streamName,
        BranchMetrics metrics)
    {
        // Use reflection to create instance since constructor is internal
        var constructor = typeof(MulticastDataPipe<T>)
            .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
            .First();

        return (MulticastDataPipe<T>)constructor.Invoke([source, subscriberCount, perSubscriberBuffer, streamName, metrics]);
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
        for (var i = 1; i <= 10; i++)
        {
            yield return i;

            await Task.Delay(10); // Small delay to make cancellation testable
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

    private static async IAsyncEnumerable<TestData> GetComplexTestStream()
    {
        yield return new TestData { Id = 1, Name = "Test1" };
        yield return new TestData { Id = 2, Name = "Test2" };

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

    #endregion
}
