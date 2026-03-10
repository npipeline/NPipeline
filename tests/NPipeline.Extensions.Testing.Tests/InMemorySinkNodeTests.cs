using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

public class InMemorySinkNodeTests
{
    [Fact]
    public void Constructor_ShouldCreateSinkWithEmptyItems()
    {
        // Arrange & Act
        var sink = new InMemorySinkNode<int>();

        // Assert
        sink.Items.Should().BeEmpty();
        sink.Completion.Should().NotBeNull();
    }

    [Fact]
    public async Task ExecuteAsync_ShouldCollectAllItems()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        var dataStream = new InMemoryDataStream<int>(items);

        // Act
        await sink.ConsumeAsync(dataStream, context, CancellationToken.None);

        // Assert
        sink.Items.Should().BeEquivalentTo(items);
        var completionResult = await sink.Completion;
        completionResult.Should().BeEquivalentTo(items);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellation_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();

        // Create a data pipe that will trigger cancellation
        var dataStream = new InMemoryDataStream<int>([1, 2, 3]);

        // Cancel before execution
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => sink.ConsumeAsync(dataStream, context, cts.Token));

        // The completion task should also be canceled
        await Assert.ThrowsAsync<OperationCanceledException>(() => sink.Completion);
    }

    [Fact]
    public async Task ExecuteAsync_WithExceptionDuringProcessing_ShouldPropagateException()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;

        // Create a data pipe that throws an exception during enumeration
        var dataStream = new ThrowingDataStream<int>(new InvalidOperationException("Test exception"));

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sink.ConsumeAsync(dataStream, context, CancellationToken.None));

        exception.Message.Should().Be("Test exception");

        // The completion task should also contain the exception
        var completionException = await Assert.ThrowsAsync<InvalidOperationException>(() => sink.Completion);
        completionException.Message.Should().Be("Test exception");
    }

    [Fact]
    public void RegisterInContext_ShouldRegisterSinkInContext()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;

        // Act
        sink.RegisterInContext(context);

        // Assert
        context.Items.Should().ContainKey(typeof(InMemorySinkNode<int>).FullName!);
        context.Items.Should().ContainKey(typeof(int).FullName!);

        var registeredSink = context.GetSink<InMemorySinkNode<int>>();
        registeredSink.Should().Be(sink);
    }

    [Fact]
    public void RegisterInContext_WithParentContext_ShouldRegisterSinkInBothContexts()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var parentContext = PipelineContext.Default;

        var context = PipelineContext.Default;
        context.Items[PipelineContextKeys.TestingParentContext] = parentContext;

        // Act
        sink.RegisterInContext(context);

        // Assert
        // Check current context
        context.Items.Should().ContainKey(typeof(InMemorySinkNode<int>).FullName!);
        context.Items.Should().ContainKey(typeof(int).FullName!);

        // Check parent context
        parentContext.Items.Should().ContainKey(typeof(InMemorySinkNode<int>).FullName!);
        parentContext.Items.Should().ContainKey(typeof(int).FullName!);

        var registeredSink = context.GetSink<InMemorySinkNode<int>>();
        registeredSink.Should().Be(sink);
    }

    [Fact]
    public async Task ExecuteAsync_ShouldRegisterSinkBeforeProcessingItems()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        var dataStream = new InMemoryDataStream<int>(items);

        // Act
        await sink.ConsumeAsync(dataStream, context, CancellationToken.None);

        // Assert
        context.Items.Should().ContainKey(typeof(InMemorySinkNode<int>).FullName!);
        context.Items.Should().ContainKey(typeof(int).FullName!);

        var registeredSink = context.GetSink<InMemorySinkNode<int>>();
        registeredSink.Should().Be(sink);
    }

    [Fact]
    public async Task ExecuteAsync_WithParentContext_ShouldRegisterSinkInParentContext()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var parentContext = PipelineContext.Default;
        var context = PipelineContext.Default;
        context.Items[PipelineContextKeys.TestingParentContext] = parentContext;
        var items = new[] { 1, 2, 3 };
        var dataStream = new InMemoryDataStream<int>(items);

        // Act
        await sink.ConsumeAsync(dataStream, context, CancellationToken.None);

        // Assert
        // Check parent context
        parentContext.Items.Should().ContainKey(typeof(InMemorySinkNode<int>).FullName!);
        parentContext.Items.Should().ContainKey(typeof(int).FullName!);

        var registeredSink = parentContext.GetSink<InMemorySinkNode<int>>();
        registeredSink.Should().Be(sink);
    }

    [Fact]
    public async Task ConsumeAsync_WithEmptyStream_ShouldCompleteWithEmptyList()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;
        var dataStream = new InMemoryDataStream<int>([]);

        // Act
        await sink.ConsumeAsync(dataStream, context, CancellationToken.None);

        // Assert
        sink.Items.Should().BeEmpty();
        var completionResult = await sink.Completion;
        completionResult.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecuteAsync_CompletionTaskShouldReturnSnapshotOfItems()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        var dataStream = new InMemoryDataStream<int>(items);

        // Act
        var executionTask = sink.ConsumeAsync(dataStream, context, CancellationToken.None);
        var completionResult = await sink.Completion;

        // Wait for execution to complete
        await executionTask;

        // Assert
        completionResult.Should().BeEquivalentTo(items);

        // Verify it's a snapshot by modifying the original items list
        // (This is more of a conceptual test since InMemoryDataStream creates its own copy)
        completionResult.Should().NotBeSameAs(items);
    }

    [Fact]
    public async Task ConsumeAsync_WithNullStream_ShouldThrowArgumentNullException()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => sink.ConsumeAsync(null!, context, CancellationToken.None));
    }

    [Fact]
    public async Task ExecuteAsync_WithNullContext_ShouldThrowArgumentNullException()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var dataStream = new InMemoryDataStream<int>([1, 2, 3]);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => sink.ConsumeAsync(dataStream, null!, CancellationToken.None));
    }

    // Helper class for testing exception scenarios
    private sealed class ThrowingDataStream<T> : IDataStream<T>
    {
        private readonly Exception _exception;

        public ThrowingDataStream(Exception exception)
        {
            _exception = exception;
        }

        public string StreamName => "ThrowingDataStream";

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            throw _exception;
        }

        public IAsyncEnumerable<object?> ToAsyncEnumerable(CancellationToken cancellationToken = default)
        {
            throw _exception;
        }

        public Type GetDataType()
        {
            return typeof(T);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
