using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Tests.DataFlow.DataStreams;

/// <summary>
///     Tests for common DataStream interface contracts.
///     Validates that all DataStream implementations correctly implement IDataStream and IDataStream&lt;T&gt; interfaces.
/// </summary>
public sealed class PipeTests
{
    [Fact]
    public void InMemoryDataStream_ImplementsIDataStream()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.InMemoryDataStream<int> pipe = new(items);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataStream>();
    }

    [Fact]
    public void InMemoryDataStream_ImplementsIDataStreamGeneric()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.InMemoryDataStream<int> pipe = new(items);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataStream<int>>();
    }

    [Fact]
    public void DataStream_ImplementsIDataStream()
    {
        // Arrange
        var stream = GetTestStream();
        DataStream<int> pipe = new(stream);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataStream>();
    }

    [Fact]
    public void DataStream_ImplementsIDataStreamGeneric()
    {
        // Arrange
        var stream = GetTestStream();
        DataStream<int> pipe = new(stream);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataStream<int>>();
    }

    [Fact]
    public async Task InMemoryDataStream_ToAsyncEnumerable_ReturnsCorrectItems()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream pipe = new Common.InMemoryDataStream<int>(items);
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
    public async Task DataStream_ToAsyncEnumerable_ReturnsCorrectItems()
    {
        // Arrange
        var stream = GetTestStream();
        IDataStream pipe = new DataStream<int>(stream);
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
    public void InMemoryDataStream_GetDataType_ReturnsCorrectType()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream pipe = new Common.InMemoryDataStream<int>(items);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public void DataStream_GetDataType_ReturnsCorrectType()
    {
        // Arrange
        var stream = GetTestStream();
        IDataStream pipe = new DataStream<int>(stream);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public async Task InMemoryDataStream_AsAsyncEnumerable_EnumeratesCorrectly()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IAsyncEnumerable<int> pipe = new Common.InMemoryDataStream<int>(items);
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
    public async Task DataStream_AsAsyncEnumerable_EnumeratesCorrectly()
    {
        // Arrange
        var stream = GetTestStream();
        IAsyncEnumerable<int> pipe = new DataStream<int>(stream);
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
    public async Task InMemoryDataStream_Dispose_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IAsyncDisposable pipe = new Common.InMemoryDataStream<int>(items);

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void InMemoryDataStream_IDisposable_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDisposable pipe = new Common.InMemoryDataStream<int>(items);

        // Act & Assert
        var act = () => pipe.Dispose();
        act.Should().NotThrow();
    }

    [Fact]
    public void InMemoryDataStream_ImplementsBothDisposalInterfaces()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.InMemoryDataStream<int> pipe = new(items);

        // Act & Assert
        pipe.Should().BeAssignableTo<IDisposable>();
        pipe.Should().BeAssignableTo<IAsyncDisposable>();
    }

    [Fact]
    public async Task DataStream_Dispose_DoesNotThrow()
    {
        // Arrange
        var stream = GetTestStream();
        IAsyncDisposable pipe = new DataStream<int>(stream);

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void InMemoryDataStream_StreamName_PropertyAccessible()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.InMemoryDataStream<int> pipe = new(items);

        // Act & Assert
        _ = pipe.StreamName.Should().Be("TestStream");
    }

    [Fact]
    public void DataStream_StreamName_PropertyAccessible()
    {
        // Arrange
        var stream = GetTestStream();
        DataStream<int> pipe = new(stream, "TestStream");

        // Act & Assert
        _ = pipe.StreamName.Should().Be("TestStream");
    }

    [Fact]
    public void CappedReplayableDataStream_ImplementsIDataStream()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream<int> sourcePipe = new Common.InMemoryDataStream<int>(items);
        CappedReplayableDataStream<int> pipe = new(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataStream>();
    }

    [Fact]
    public void CappedReplayableDataStream_ImplementsIDataStreamGeneric()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream<int> sourcePipe = new Common.InMemoryDataStream<int>(items);
        CappedReplayableDataStream<int> pipe = new(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataStream<int>>();
    }

    [Fact]
    public void CappedReplayableDataStream_GetDataType_ReturnsCorrectType()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream<int> sourcePipe = new Common.InMemoryDataStream<int>(items);
        IDataStream pipe = new CappedReplayableDataStream<int>(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public async Task CappedReplayableDataStream_ToAsyncEnumerable_ReturnsCorrectItems()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream<int> sourcePipe = new Common.InMemoryDataStream<int>(items);
        IDataStream pipe = new CappedReplayableDataStream<int>(sourcePipe, 5, "TestPipe");
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
    public async Task CappedReplayableDataStream_AsAsyncEnumerable_EnumeratesCorrectly()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream<int> sourcePipe = new Common.InMemoryDataStream<int>(items);
        IAsyncEnumerable<int> pipe = new CappedReplayableDataStream<int>(sourcePipe, 5, "TestPipe");
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
    public async Task CappedReplayableDataStream_Dispose_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream<int> sourcePipe = new Common.InMemoryDataStream<int>(items);
        IAsyncDisposable pipe = new CappedReplayableDataStream<int>(sourcePipe, 5, "TestPipe");

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void CappedReplayableDataStream_StreamName_PropertyAccessible()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataStream<int> sourcePipe = new Common.InMemoryDataStream<int>(items);
        CappedReplayableDataStream<int> pipe = new(sourcePipe, 5, "TestPipe");

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
