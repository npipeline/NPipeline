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
    public void InMemoryDataPipe_ImplementsIDataPipe()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.InMemoryDataPipe<int> pipe = new(items);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe>();
    }

    [Fact]
    public void InMemoryDataPipe_ImplementsIDataPipeGeneric()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.InMemoryDataPipe<int> pipe = new(items);

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
    public async Task InMemoryDataPipe_ToAsyncEnumerable_ReturnsCorrectItems()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe pipe = new Common.InMemoryDataPipe<int>(items);
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
    public void InMemoryDataPipe_GetDataType_ReturnsCorrectType()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe pipe = new Common.InMemoryDataPipe<int>(items);

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
    public async Task InMemoryDataPipe_AsAsyncEnumerable_EnumeratesCorrectly()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IAsyncEnumerable<int> pipe = new Common.InMemoryDataPipe<int>(items);
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
    public async Task InMemoryDataPipe_Dispose_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IAsyncDisposable pipe = new Common.InMemoryDataPipe<int>(items);

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public void InMemoryDataPipe_IDisposable_DoesNotThrow()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDisposable pipe = new Common.InMemoryDataPipe<int>(items);

        // Act & Assert
        var act = () => pipe.Dispose();
        act.Should().NotThrow();
    }

    [Fact]
    public void InMemoryDataPipe_ImplementsBothDisposalInterfaces()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.InMemoryDataPipe<int> pipe = new(items);

        // Act & Assert
        pipe.Should().BeAssignableTo<IDisposable>();
        pipe.Should().BeAssignableTo<IAsyncDisposable>();
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
    public void InMemoryDataPipe_StreamName_PropertyAccessible()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        Common.InMemoryDataPipe<int> pipe = new(items);

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
        IDataPipe<int> sourcePipe = new Common.InMemoryDataPipe<int>(items);
        CappedReplayableDataPipe<int> pipe = new(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe>();
    }

    [Fact]
    public void CappedReplayableDataPipe_ImplementsIDataPipeGeneric()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.InMemoryDataPipe<int>(items);
        CappedReplayableDataPipe<int> pipe = new(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IDataPipe<int>>();
    }

    [Fact]
    public void CappedReplayableDataPipe_GetDataType_ReturnsCorrectType()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.InMemoryDataPipe<int>(items);
        IDataPipe pipe = new CappedReplayableDataPipe<int>(sourcePipe, 5, "TestPipe");

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public async Task CappedReplayableDataPipe_ToAsyncEnumerable_ReturnsCorrectItems()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> sourcePipe = new Common.InMemoryDataPipe<int>(items);
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
        IDataPipe<int> sourcePipe = new Common.InMemoryDataPipe<int>(items);
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
        IDataPipe<int> sourcePipe = new Common.InMemoryDataPipe<int>(items);
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
        IDataPipe<int> sourcePipe = new Common.InMemoryDataPipe<int>(items);
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
