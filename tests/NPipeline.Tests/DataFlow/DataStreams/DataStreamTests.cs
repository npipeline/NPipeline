using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Tests.DataFlow.DataStreams;

/// <summary>
///     Tests for DataStream class.
///     Validates streaming behavior, disposal, and interface implementation.
/// </summary>
public sealed class DataStreamTests
{
    [Fact]
    public void Constructor_WithNullStream_ThrowsArgumentNullException()
    {
        // Arrange
        IAsyncEnumerable<int> nullStream = null!;

        // Act
        Action act = () => _ = new DataStream<int>(nullStream);

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
        DataStream<int> pipe = new(stream, streamName);

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
        DataStream<int> pipe = new(stream);

        // Assert
        _ = pipe.StreamName.Should().Be("DefaultStream");
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithValidStream_EnumeratesAllItems()
    {
        // Arrange
        var stream = GetTestStream();
        DataStream<int> pipe = new(stream);
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
        DataStream<int> pipe = new(stream);
        List<int> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEmpty();
    }

    /// <summary>
    ///     <c>DataStream&lt;T&gt;</c> used to declare its own <c>WithCancellation</c> instance method, which beat the BCL
    ///     extension in overload resolution and added a per-item <c>ThrowIfCancellationRequested</c>. That made a source
    ///     ignoring its token look cancellable, but only when the caller's variable was typed as the concrete class -
    ///     through <c>IDataStream&lt;T&gt;</c> the same call got the BCL's struct wrapper and no such check. The method is
    ///     gone, so cancellation now means one thing everywhere: the token reaches the source, and the source honours it.
    /// </summary>
    [Fact]
    public async Task WithCancellation_CancelsWhenTheSourceObservesTheToken()
    {
        // Arrange
        DataStream<int> pipe = new(GetCancellableStream());
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
                    await cts.CancelAsync();
            }
        });

        // Assert - TaskCanceledException is a subclass of OperationCanceledException
        _ = await task.Invoking(t => t).Should().ThrowAsync<OperationCanceledException>();
        _ = enumeratedItems.Should().HaveCount(2);
        _ = enumeratedItems.Should().ContainInOrder(1, 2);
    }

    [Fact]
    public async Task WithCancellation_BindsToTheBclExtension_ThroughEitherStaticType()
    {
        // Arrange
        DataStream<int> concrete = new(GetCancellableStream());
        IDataStream<int> asInterface = concrete;
        CancellationTokenSource cts = new();
        await cts.CancelAsync();

        // Act
        var throughConcrete = async () =>
        {
            await foreach (var _ in concrete.WithCancellation(cts.Token))
            {
                // Should not yield.
            }
        };

        var throughInterface = async () =>
        {
            await foreach (var _ in asInterface.WithCancellation(cts.Token))
            {
                // Should not yield.
            }
        };

        // Assert - both static types now reach the same implementation and behave identically
        _ = await throughConcrete.Should().ThrowAsync<OperationCanceledException>();
        _ = await throughInterface.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ToAsyncEnumerable_ReturnsAllItemsAsObjects()
    {
        // Arrange
        var stream = GetTestStream();
        IDataStream<int> pipe = new DataStream<int>(stream);
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
        DataStream<int> pipe = new(stream);

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
        DataStream<int> pipe = new(stream);

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
        DataStream<int> pipe = new(stream);

        // Act & Assert
        var act = async () => await pipe.DisposeAsync();
        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Enumeration_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        var stream = GetTestStream();
        DataStream<int> pipe = new(stream);
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
        DataStream<string> pipe = new(stream);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<string>();
    }

    [Fact]
    public async Task WithNullItems_HandlesNullReferences()
    {
        // Arrange
        var stream = GetNullTestStream();
        DataStream<string?> pipe = new(stream);
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
        DataStream<TestData> pipe = new(stream);
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
        DataStream<int> pipe = new(stream);

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
    public void ImplementsIForwardOnlyDataStream_Interface()
    {
        // Arrange
        var stream = GetTestStream();
        DataStream<int> pipe = new(stream);

        // Act & Assert
        _ = pipe.Should().BeAssignableTo<IForwardOnlyDataStream>();
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

    private static async IAsyncEnumerable<int> GetCancellableStream([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var i = 1; i <= 10; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return i;

            await Task.Delay(10, cancellationToken); // Small delay to make cancellation testable
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
