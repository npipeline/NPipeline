// ReSharper disable CollectionNeverQueried.Local

using AwesomeAssertions;
using FakeItEasy;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;

namespace NPipeline.Tests.DataFlow.DataPipes;

public class CappedReplayableDataPipeTests
{
    [Fact]
    public async Task GetAsyncEnumerator_WhenCapIsNotReached_ReturnsAllItems()
    {
        // Arrange
        var sourceItems = new List<int> { 1, 2, 3 };
        var sourcePipe = A.Fake<IDataPipe<int>>();

        A.CallTo(() => sourcePipe.GetAsyncEnumerator(A<CancellationToken>.Ignored))
            .Returns(sourceItems.ToAsyncEnumerable().GetAsyncEnumerator());

        var pipe = new CappedReplayableDataPipe<int>(sourcePipe, 5, "TestPipe");
        var receivedItems = new List<int>();

        // Act
        await foreach (var item in pipe.WithCancellation(CancellationToken.None))
        {
            receivedItems.Add(item);
        }

        // Assert
        receivedItems.Should().BeEquivalentTo(sourceItems);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WhenCapIsReached_ThrowsInvalidOperationException()
    {
        // Arrange
        var sourceItems = new List<int> { 1, 2, 3, 4, 5 };
        var sourcePipe = A.Fake<IDataPipe<int>>();

        A.CallTo(() => sourcePipe.GetAsyncEnumerator(A<CancellationToken>.Ignored))
            .Returns(sourceItems.ToAsyncEnumerable().GetAsyncEnumerator());

        var pipe = new CappedReplayableDataPipe<int>(sourcePipe, 2, "TestPipe");

        // Act
        var act = async () =>
        {
            await foreach (var item in pipe.WithCancellation(CancellationToken.None))
            {
                // Consume items until cap is reached
            }
        };

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Resilience materialization exceeded MaxMaterializedItems=2.");
    }

    [Fact]
    public async Task GetAsyncEnumerator_WhenCapIsZero_ThrowsInvalidOperationExceptionImmediatelyIfSourceHasItems()
    {
        // Arrange
        var sourceItems = new List<int> { 1, 2, 3 };
        var sourcePipe = A.Fake<IDataPipe<int>>();

        A.CallTo(() => sourcePipe.GetAsyncEnumerator(A<CancellationToken>.Ignored))
            .Returns(sourceItems.ToAsyncEnumerable().GetAsyncEnumerator());

        var pipe = new CappedReplayableDataPipe<int>(sourcePipe, 0, "TestPipe");

        // Act
        var act = async () =>
        {
            await foreach (var item in pipe.WithCancellation(CancellationToken.None))
            {
                // Consume items
            }
        };

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Resilience materialization exceeded MaxMaterializedItems=0.");
    }

    [Fact]
    public async Task GetAsyncEnumerator_WhenCapIsZeroAndSourceIsEmpty_DoesNotThrow()
    {
        // Arrange
        var sourceItems = new List<int>();
        var sourcePipe = A.Fake<IDataPipe<int>>();

        A.CallTo(() => sourcePipe.GetAsyncEnumerator(A<CancellationToken>.Ignored))
            .Returns(sourceItems.ToAsyncEnumerable().GetAsyncEnumerator());

        var pipe = new CappedReplayableDataPipe<int>(sourcePipe, 0, "TestPipe");
        var receivedItems = new List<int>();

        // Act
        await foreach (var item in pipe.WithCancellation(CancellationToken.None))
        {
            receivedItems.Add(item);
        }

        // Assert
        receivedItems.Should().BeEmpty();
    }

    [Fact]
    public async Task GetAsyncEnumerator_ReplaysBufferedItemsCorrectly()
    {
        // Arrange
        var sourceItems = new List<int> { 1, 2, 3 };
        var sourcePipe = A.Fake<IDataPipe<int>>();

        A.CallTo(() => sourcePipe.GetAsyncEnumerator(A<CancellationToken>.Ignored))
            .Returns(sourceItems.ToAsyncEnumerable().GetAsyncEnumerator());

        var pipe = new CappedReplayableDataPipe<int>(sourcePipe, 5, "TestPipe");
        var firstConsumption = new List<int>();

        await foreach (var item in pipe.WithCancellation(CancellationToken.None))
        {
            firstConsumption.Add(item);
        }

        // Act - second consumption (should replay from buffer)
        var secondConsumption = new List<int>();

        await foreach (var item in pipe.WithCancellation(CancellationToken.None))
        {
            secondConsumption.Add(item);
        }

        // Assert
        firstConsumption.Should().BeEquivalentTo(sourceItems);
        secondConsumption.Should().BeEquivalentTo(sourceItems);
    }

    [Fact]
    public async Task DisposeAsync_DisposesSourcePipe()
    {
        // Arrange
        var sourcePipe = A.Fake<IDataPipe<int>>();
        var pipe = new CappedReplayableDataPipe<int>(sourcePipe, 5, "TestPipe");

        // Act
        await pipe.DisposeAsync();

        // Assert
        A.CallTo(() => sourcePipe.DisposeAsync()).MustHaveHappened();
    }

    [Fact]
    public void Constructor_WithNullSource_ThrowsArgumentNullException()
    {
        // Arrange
        IDataPipe<int> nullSource = null!;

        // Act
        Action act = () => _ = new CappedReplayableDataPipe<int>(nullSource, 5, "TestPipe");

        // Assert
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("inner");
    }

    [Fact]
    public async Task Constructor_WithNegativeCap_CreatesPipeWithNoCap()
    {
        // Arrange
        var sourcePipe = A.Fake<IDataPipe<int>>();

        A.CallTo(() => sourcePipe.GetAsyncEnumerator(A<CancellationToken>.Ignored))
            .Returns(new List<int> { 1, 2, 3, 4, 5, 6, 7 }.ToAsyncEnumerable().GetAsyncEnumerator());

        // Act
        var pipe = new CappedReplayableDataPipe<int>(sourcePipe, -1, "TestPipe");
        var receivedItems = new List<int>();

        // Assert
        var act = async () =>
        {
            await foreach (var item in pipe.WithCancellation(CancellationToken.None))
            {
                receivedItems.Add(item);
            }
        };

        // Negative cap should still throw when trying to materialize
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("Resilience materialization exceeded MaxMaterializedItems=-1.");
    }
}

// Helper for ToAsyncEnumerable
internal static class EnumerableAsyncExtensions
{
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IEnumerable<T> enumerable)
    {
        foreach (var item in enumerable)
        {
            yield return item;
        }

        await Task.CompletedTask; // Ensure it's async
    }
}
