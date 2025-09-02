using AwesomeAssertions;
using NPipeline.DataFlow;

namespace NPipeline.Tests.DataFlow.DataPipes;

public sealed class ListDataPipeTests
{
    [Fact]
    public void Constructor_WithNullItems_ThrowsArgumentNullException()
    {
        // Arrange
        IReadOnlyList<int> nullItems = null!;

        // Act
        Action act = () => _ = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>(nullItems);

        // Assert
        _ = act.Should().Throw<ArgumentNullException>()
            .WithParameterName("items");
    }

    [Fact]
    public void Constructor_WithValidItems_SetsPropertiesCorrectly()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        var streamName = "TestStream";

        // Act
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items, streamName);

        // Assert
        _ = pipe.Items.Should().BeSameAs(items);
        _ = pipe.StreamName.Should().Be(streamName);
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public void Constructor_WithDefaultStreamName_SetsEmptyStreamName()
    {
        // Arrange
        List<int> items = [1, 2, 3];

        // Act
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items);

        // Assert
        _ = pipe.StreamName.Should().BeEmpty();
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithValidItems_EnumeratesAllItems()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items);
        List<int> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithEmptyList_EnumeratesNoItems()
    {
        // Arrange
        List<int> items = [];
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items);
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
        List<int> items = [1, 2, 3, 4, 5];
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items);
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

        // Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => task);
        _ = enumeratedItems.Should().HaveCount(2);
        _ = enumeratedItems.Should().ContainInOrder(1, 2);
    }

    [Fact]
    public async Task ToAsyncEnumerable_ReturnsAllItemsAsObjects()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> pipe = new NPipeline.DataFlow.DataPipes.ListDataPipe<int>(items);
        List<object> enumeratedItems = [];

        // Act
        await foreach (var item in pipe.ToAsyncEnumerable())
        {
            enumeratedItems.Add(item!);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items.Cast<object>());
    }

    [Fact]
    public async Task MultipleEnumerations_ReturnSameItems()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items);

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
        _ = firstEnumeration.Should().BeEquivalentTo(items);
        _ = secondEnumeration.Should().BeEquivalentTo(items);
    }

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items);

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task DisposeAsync_AfterEnumeration_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items);

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
    public async Task Enumeration_AfterDispose_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        NPipeline.DataFlow.DataPipes.ListDataPipe<int> pipe = new(items);
        await pipe.DisposeAsync();

        // Act & Assert
        await foreach (var item in pipe)
        {
            // This should not throw since ListDataPipe doesn't track disposal
        }

        // Note: ListDataPipe doesn't track disposal state, so this test documents current behavior
    }

    [Fact]
    public void GetDataType_ReturnsCorrectType()
    {
        // Arrange
        List<string> items = ["test1", "test2"];
        NPipeline.DataFlow.DataPipes.ListDataPipe<string> pipe = new(items);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<string>();
    }

    [Fact]
    public async Task WithNullItems_HandlesNullReferences()
    {
        // Arrange
        List<string?> items = ["test1", null, "test3"];
        NPipeline.DataFlow.DataPipes.ListDataPipe<string?> pipe = new(items);
        List<string?> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items);
    }

    [Fact]
    public async Task WithComplexTypes_EnumeratesCorrectly()
    {
        // Arrange
        List<TestData> items =
        [
            new() { Id = 1, Name = "Test1" },
            new() { Id = 2, Name = "Test2" },
        ];

        NPipeline.DataFlow.DataPipes.ListDataPipe<TestData> pipe = new(items);
        List<TestData> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items);
    }

    private sealed class TestData
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
