using System.Reflection;
using System.Runtime.ExceptionServices;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Tests.DataFlow.DataPipes;

public sealed class CountingDataPipeTests
{
    [Fact]
    public void Constructor_WithNullInnerPipe_ThrowsArgumentNullException()
    {
        // Arrange
        StatsCounter counter = new();

        // Act
        Action act = () => _ = CreateCountingDataPipe<object>(null!, counter);

        // Assert
        _ = act.Should().Throw<ArgumentNullException>()
            .WithParameterName("inner");
    }

    [Fact]
    public void Constructor_WithNullCounter_ThrowsArgumentNullException()
    {
        // Arrange
        var innerPipe = A.Fake<IDataPipe<int>>();

        // Act
        Action act = () => _ = CreateCountingDataPipe(innerPipe, null!);

        // Assert
        _ = act.Should().Throw<ArgumentNullException>()
            .WithParameterName("counter");
    }

    [Fact]
    public void Constructor_WithValidParameters_SetsPropertiesCorrectly()
    {
        // Arrange
        var innerPipe = A.Fake<IDataPipe<int>>();
        StatsCounter counter = new();

        // Act
        var pipe = CreateCountingDataPipe(innerPipe, counter);

        // Assert
        _ = pipe.StreamName.Should().Be($"Counted_{innerPipe.StreamName}");
        _ = pipe.GetDataType().Should().Be<int>();
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithItems_IncrementsCounter()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> innerPipe = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>(items);
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);

        // Act
        List<int> enumeratedItems = [];

        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items);
        _ = counter.Total.Should().Be(3);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithEmptyItems_DoesNotIncrementCounter()
    {
        // Arrange
        List<int> items = [];
        IDataPipe<int> innerPipe = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>(items);
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);

        // Act
        List<int> enumeratedItems = [];

        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEmpty();
        _ = counter.Total.Should().Be(0);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithRetryExhaustedException_StoresInContext()
    {
        // Arrange
        var retryException = new RetryExhaustedException("Test retry exhausted");
        var innerPipe = CreateThrowingPipe(retryException);
        StatsCounter counter = new();
        var context = PipelineContext.Default;
        var pipe = CreateCountingDataPipe(innerPipe, counter, context);

        // Act & Assert
        await Assert.ThrowsAsync<RetryExhaustedException>(async () =>
        {
            await foreach (var item in pipe)
            {
                // This should throw
            }
        });

        _ = context.Items[PipelineContextKeys.LastRetryExhaustedException].Should().Be(retryException);
    }

    [Fact]
    public async Task GetAsyncEnumerator_WithRegularException_PropagatesException()
    {
        // Arrange
        var regularException = new InvalidOperationException("Test exception");
        var innerPipe = CreateThrowingPipe(regularException);
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);

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
    public async Task GetAsyncEnumerator_WithCancellation_RespectsCancellation()
    {
        // Arrange
        List<int> items = [1, 2, 3, 4, 5];
        IDataPipe<int> innerPipe = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>(items);
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);
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
        _ = counter.Total.Should().Be(2);
    }

    [Fact]
    public async Task GetAsyncEnumerator_MultipleEnumerations_CountsEachSeparately()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> innerPipe = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>(items);
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);

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
        _ = counter.Total.Should().Be(6); // 3 items * 2 enumerations
    }

    [Fact]
    public async Task ToAsyncEnumerable_ReturnsAllItemsAsObjects()
    {
        // Arrange
        List<int> items = [1, 2, 3];
        IDataPipe<int> innerPipe = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<int>(items);
        StatsCounter counter = new();
        IDataPipe pipe = CreateCountingDataPipe(innerPipe, counter);
        List<object> enumeratedItems = [];

        // Act
        await foreach (var item in pipe.ToAsyncEnumerable())
        {
            enumeratedItems.Add(item!);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items.Cast<object>());
        _ = counter.Total.Should().Be(3);
    }

    [Fact]
    public async Task DisposeAsync_DisposesInnerPipe()
    {
        // Arrange
        var innerPipe = A.Fake<IDataPipe<int>>();
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);

        // Act
        await pipe.DisposeAsync();

        // Assert
        A.CallTo(() => innerPipe.DisposeAsync()).MustHaveHappened();
    }

    [Fact]
    public void GetDataType_ReturnsCorrectType()
    {
        // Arrange
        var innerPipe = A.Fake<IDataPipe<string>>();
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);

        // Act & Assert
        _ = pipe.GetDataType().Should().Be<string>();
    }

    [Fact]
    public async Task WithNullItems_HandlesNullReferences()
    {
        // Arrange
        List<string?> items = ["test1", null, "test3"];
        IDataPipe<string?> innerPipe = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<string?>(items);
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);
        List<string?> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items);
        _ = counter.Total.Should().Be(3);
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

        IDataPipe<TestData> innerPipe = new NPipeline.DataFlow.DataPipes.InMemoryDataPipe<TestData>(items);
        StatsCounter counter = new();
        var pipe = CreateCountingDataPipe(innerPipe, counter);
        List<TestData> enumeratedItems = [];

        // Act
        await foreach (var item in pipe)
        {
            enumeratedItems.Add(item);
        }

        // Assert
        _ = enumeratedItems.Should().BeEquivalentTo(items);
        _ = counter.Total.Should().Be(2);
    }

    #region Test Helper Classes

    private sealed class TestData
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    #endregion

    #region Helper Methods

    private static CountingDataPipe<T> CreateCountingDataPipe<T>(
        IDataPipe<T> innerPipe,
        StatsCounter counter,
        PipelineContext? context = null)
    {
        // Use reflection to create the instance since the constructor is internal
        var constructor = typeof(CountingDataPipe<T>)
            .GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .First();

        try
        {
            return (CountingDataPipe<T>)constructor.Invoke([innerPipe, counter, context!]);
        }
        catch (TargetInvocationException tie)
        {
            // Unwrap the inner exception to preserve the original exception
            ExceptionDispatchInfo.Capture(tie.InnerException!).Throw();
            throw; // This line will never be reached
        }
    }

    private static IDataPipe<int> CreateThrowingPipe(Exception exception)
    {
        // Create a pipe that throws the specified exception
        var source = GetThrowingStream(exception);
        return new StreamingDataPipe<int>(source);
    }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
    private static async IAsyncEnumerable<int> GetThrowingStream(Exception exception)
    {
        yield return 1;
        yield return 2;

        ExceptionDispatchInfo.Capture(exception).Throw();
    }
#pragma warning restore CS1998

    #endregion
}
