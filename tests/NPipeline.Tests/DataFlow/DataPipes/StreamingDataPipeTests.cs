using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;

namespace NPipeline.Tests.DataFlow.DataPipes;

/// <summary>
///     Tests for StreamingDataPipe class.
///     Validates streaming behavior, disposal, and interface implementation.
/// </summary>
public sealed class StreamingDataPipeTests
{
    [Fact]
    public void Constructor_WithNullStream_ThrowsArgumentNullException()
    {
        // Arrange
        IAsyncEnumerable<int> nullStream = null!;

        // Act
        Action act = () => _ = new StreamingDataPipe<int>(nullStream);

        // Assert
        _ = act.Should().Throw<ArgumentNullException>()
            .WithParameterName("stream");
    }

    [Fact]
    public void Constructor_WithValidStream_SetsPropertiesCorrectly()
    {
        // Arrange
        var stream = GetTestStream();
        var streamName = "TestStream";

        // Act
        StreamingDataPipe<int> pipe = new(stream, streamName);

        // Assert
        _ = pipe.StreamName.Should().Be(streamName);
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public void Constructor_WithDefaultStreamName_SetsDefaultStreamName()
    {
        // Arrange
        var stream = GetTestStream();

        // Act
        StreamingDataPipe<int> pipe = new(stream);

        // Assert
        _ = pipe.StreamName.Should().Be("DefaultStream");
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithValidStream_EnumeratesAllItems()
    {
        // Arrange
        var stream = GetTestStream();
        StreamingDataPipe<int> pipe = new(stream);
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
    public async Task GetAsyncEnumerator_WithEmptyStream_EnumeratesNoItems()
    {
        // Arrange
        var stream = GetEmptyStream();
        StreamingDataPipe<int> pipe = new(stream);
        List<int> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEmpty();
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithCancellation_RespectsCancellation()
    {
        // Arrange
        var stream = GetLongRunningStream();
        StreamingDataPipe<int> pipe = new(stream);
        CancellationTokenSource cts = new();
        List<int> enumeratedItems = [];

        // Act - Cancel after processing 2 items
        var task = Task.Run(async () =>
        {
            var count = 0;

            await foreach (var item in pipe.WithCancellation(cts.Token))
            {
                enumeratedItems.Add(item);
                count++;

                if (count >= 2)
                    cts.Cancel();
            }
        });

        // Assert - TaskCanceledException is a subclass of OperationCanceledException
        _ = await task.Invoking(t => t).Should().ThrowAsync<OperationCanceledException>();
        _ = enumeratedItems.Should().HaveCount(2);
        _ = enumeratedItems.Should().ContainInOrder(1, 2);
    }

    [Fact]
    public async Task ToAsyncEnumerable_ReturnsAllItemsAsObjects()
    {
        // Arrange
        var stream = GetTestStream();
        IDataPipe<int> pipe = new StreamingDataPipe<int>(stream);
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
    public async Task MultipleEnumerations_EnumeratesFromBeginningEachTime()
    {
        // Arrange
        var stream = GetTestStream();
        StreamingDataPipe<int> pipe = new(stream);

        // Act
        List<int> firstEnumeration = [];
        List<int> secondEnumeration = [];

        await foreach (var item in pipe)
        {
            firstEnumeration.Add(item);
        }

        await foreach (var item in pipe)
        {
            secondEnumeration.Add(item);
        }

        // Assert
        _ = firstEnumeration.Should().BeEquivalentTo([1, 2, 3]);
        _ = secondEnumeration.Should().BeEquivalentTo([1, 2, 3]);
    }

    [Fact]
    public async Task DisposeAsync_WithDisposableStream_DisposesStream()
    {
        // Arrange
        DisposableAsyncStream stream = new();
        StreamingDataPipe<int> pipe = new(stream);

        // Act
        await pipe.DisposeAsync();

        // Assert
        _ = stream.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task DisposeAsync_WithNonDisposableStream_DoesNotThrow()
    {
        // Arrange
        var stream = GetTestStream();
        StreamingDataPipe<int> pipe = new(stream);

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Enumeration_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var stream = GetTestStream();
        StreamingDataPipe<int> pipe = new(stream);
        await pipe.DisposeAsync();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await foreach (var item in pipe)
            {
                // This should throw
            }
        });
    }

    [Fact]
    public void GetDataType_ReturnsCorrectType()
    {
        // Arrange
        var stream = GetStringTestStream();
        StreamingDataPipe<string> pipe = new(stream);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<string>();
    }

    [Fact]
    public async Task WithNullItems_HandlesNullReferences()
    {
        // Arrange
        var stream = GetNullTestStream();
        StreamingDataPipe<string?> pipe = new(stream);
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
        var stream = GetComplexTestStream();
        StreamingDataPipe<TestData> pipe = new(stream);
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
    public async Task WithExceptionInStream_PropagatesException()
    {
        // Arrange
        var stream = GetExceptionStream();
        StreamingDataPipe<int> pipe = new(stream);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var item in pipe)
            {
                // This should throw
            }
        });
    }

    [Fact]
    public void ImplementsIStreamingDataPipe_Interface()
    {
        // Arrange
        var stream = GetTestStream();
        StreamingDataPipe<int> pipe = new(stream);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IStreamingDataPipe>();
    }

    #region Test Helper Methods

    private static async IAsyncEnumerable<int> GetTestStream()
    {
        yield return 1;
        yield return 2;
        yield return 3;

        await Task.CompletedTask;
    }

    private static async IAsyncEnumerable<int> GetEmptyStream()
    {
        await Task.CompletedTask;
        yield break; // Explicitly break without yielding any items
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

    #region Test Helper Classes

    private sealed class TestData
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private sealed class DisposableAsyncStream : IAsyncEnumerable<int>, IAsyncDisposable
    {
        public bool IsDisposed { get; private set; }

        public async ValueTask DisposeAsync()
        {
            IsDisposed = true;
            await Task.CompletedTask;
        }

        public async IAsyncEnumerator<int> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield return 1;
            yield return 2;
            yield return 3;
        }
    }

    #endregion
}
