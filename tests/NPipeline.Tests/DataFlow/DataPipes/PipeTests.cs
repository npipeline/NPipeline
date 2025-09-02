using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;

namespace NPipeline.Tests.DataFlow.DataPipes;

/// <summary>
///     Tests for common DataPipe interface contracts.
///     Validates that all DataPipe implementations correctly implement IDataPipe and IDataPipe&lt;T&gt; interfaces.
/// </summary>
public sealed class PipeTests
{
    [Fact]
    public void ListDataPipe_ImplementsIDataPipe()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.ListDataPipe<int> pipe = new(items);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe>();
    }

    [Fact]
    public void ListDataPipe_ImplementsIDataPipeGeneric()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.ListDataPipe<int> pipe = new(items);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe<int>>();
    }

    [Fact]
    public void StreamingDataPipe_ImplementsIDataPipe()
    {
        // Arrange
        var stream = GetTestStream();
        StreamingDataPipe<int> pipe = new(stream);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe>();
    }

    [Fact]
    public void StreamingDataPipe_ImplementsIDataPipeGeneric()
    {
        // Arrange
        var stream = GetTestStream();
        StreamingDataPipe<int> pipe = new(stream);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe<int>>();
    }

    [Fact]
    public async Task ListDataPipe_ToAsyncEnumerable_ReturnsCorrectItems()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe pipe = new Common.ListDataPipe<int>(items);
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
    public async Task StreamingDataPipe_ToAsyncEnumerable_ReturnsCorrectItems()
    {
        // Arrange
        var stream = GetTestStream();
        IDataPipe pipe = new StreamingDataPipe<int>(stream);
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
    public void ListDataPipe_GetDataType_ReturnsCorrectType()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe pipe = new Common.ListDataPipe<int>(items);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public void StreamingDataPipe_GetDataType_ReturnsCorrectType()
    {
        // Arrange
        var stream = GetTestStream();
        IDataPipe pipe = new StreamingDataPipe<int>(stream);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public async Task ListDataPipe_AsAsyncEnumerable_EnumeratesCorrectly()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IAsyncEnumerable<int> pipe = new Common.ListDataPipe<int>(items);
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
    public async Task StreamingDataPipe_AsAsyncEnumerable_EnumeratesCorrectly()
    {
        // Arrange
        var stream = GetTestStream();
        IAsyncEnumerable<int> pipe = new StreamingDataPipe<int>(stream);
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
    public async Task ListDataPipe_Dispose_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IAsyncDisposable pipe = new Common.ListDataPipe<int>(items);

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task StreamingDataPipe_Dispose_DoesNotThrow()
    {
        // Arrange
        var stream = GetTestStream();
        IAsyncDisposable pipe = new StreamingDataPipe<int>(stream);

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void ListDataPipe_StreamName_PropertyAccessible()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.ListDataPipe<int> pipe = new(items);

        // Act & Assert
        _ = pipe.StreamName.Should().Be("TestStream");
    }

    [Fact]
    public void StreamingDataPipe_StreamName_PropertyAccessible()
    {
        // Arrange
        var stream = GetTestStream();
        StreamingDataPipe<int> pipe = new(stream, "TestStream");

        // Act & Assert
        _ = pipe.StreamName.Should().Be("TestStream");
    }

    [Fact]
    public void CappedReplayableDataPipe_ImplementsIDataPipe()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.ListDataPipe<int>(items);
        CappedReplayableDataPipe<int> pipe = new(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe>();
    }

    [Fact]
    public void CappedReplayableDataPipe_ImplementsIDataPipeGeneric()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.ListDataPipe<int>(items);
        CappedReplayableDataPipe<int> pipe = new(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe<int>>();
    }

    [Fact]
    public void CappedReplayableDataPipe_GetDataType_ReturnsCorrectType()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.ListDataPipe<int>(items);
        IDataPipe pipe = new CappedReplayableDataPipe<int>(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public async Task CappedReplayableDataPipe_ToAsyncEnumerable_ReturnsCorrectItems()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.ListDataPipe<int>(items);
        IDataPipe pipe = new CappedReplayableDataPipe<int>(sourcePipe, 5, "TestPipe");
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
    public async Task CappedReplayableDataPipe_AsAsyncEnumerable_EnumeratesCorrectly()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.ListDataPipe<int>(items);
        IAsyncEnumerable<int> pipe = new CappedReplayableDataPipe<int>(sourcePipe, 5, "TestPipe");
        List<int> enumeratedItems = [];

        // Act
        await foreach (var item in pipe.WithCancellation(CancellationToken.None))
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items);
    }

    [Fact]
    public async Task CappedReplayableDataPipe_Dispose_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.ListDataPipe<int>(items);
        IAsyncDisposable pipe = new CappedReplayableDataPipe<int>(sourcePipe, 5, "TestPipe");

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void CappedReplayableDataPipe_StreamName_PropertyAccessible()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.ListDataPipe<int>(items);
        CappedReplayableDataPipe<int> pipe = new(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.StreamName.Should().Be("TestPipe");
    }

    #region Test Helper Methods

    private static async IAsyncEnumerable<int> GetTestStream()
    {
        yield return 1;
        yield return 2;
        yield return 3;

        await Task.CompletedTask;
    }

    #endregion
}
