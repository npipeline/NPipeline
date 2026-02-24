using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.Connectors.Azure.CosmosDb.Writers;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Writers;

public sealed class CosmosBulkWriterTests
{
    private readonly CosmosConfiguration _configuration;
    private readonly Container _container;

    public CosmosBulkWriterTests()
    {
        _container = A.Fake<Container>();

        _configuration = new CosmosConfiguration
        {
            WriteStrategy = CosmosWriteStrategy.Upsert,
            ContinueOnError = false,
        };
    }

    [Fact]
    public void Constructor_WithNullContainer_ShouldNotThrow()
    {
        // Arrange & Act - Constructor doesn't validate null container
        var writer = new CosmosBulkWriter<TestItem>(
            null!,
            null,
            null,
            _configuration);

        // Assert - Writer is created (validation happens at usage time)
        writer.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithNullConfiguration_ShouldNotThrow()
    {
        // Arrange & Act - Constructor doesn't validate null configuration
        var writer = new CosmosBulkWriter<TestItem>(
            _container,
            null,
            null,
            null!);

        // Assert - Writer is created (validation happens at usage time)
        writer.Should().NotBeNull();
    }

    [Fact]
    public void WriteAsync_WithSingleItem_ShouldReturnImmediately()
    {
        // Arrange
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        var item = new TestItem { Id = "test-1", Name = "Test" };

        // Act - WriteAsync should return immediately (bulk execution)
        var task = writer.WriteAsync(item);

        // Assert - Should complete immediately (not wait for actual write)
        task.IsCompleted.Should().BeTrue();
    }

    [Fact]
    public async Task WriteAsync_WithInsertStrategy_ShouldUseCreateItem()
    {
        // Arrange
        _configuration.WriteStrategy = CosmosWriteStrategy.Insert;
        var writer = CreateWriter();

        A.CallTo(() => _container.CreateItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Act
        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test" });

        // Assert
        A.CallTo(() => _container.CreateItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithUpsertStrategy_ShouldUseUpsertItem()
    {
        // Arrange
        _configuration.WriteStrategy = CosmosWriteStrategy.Upsert;
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Act
        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test" });

        // Assert
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithPartitionKeySelector_ShouldUseCustomPartitionKey()
    {
        // Arrange
        PartitionKey? capturedPartitionKey = null;

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Invokes((TestItem item, PartitionKey? pk, ItemRequestOptions? _, CancellationToken _) => capturedPartitionKey = pk)
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        var writer = new CosmosBulkWriter<TestItem>(
            _container,
            item => item.Id,
            item => new PartitionKey(item.Name),
            _configuration);

        // Act
        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "CustomPartition" });

        // Assert
        capturedPartitionKey.Should().NotBeNull();
        capturedPartitionKey!.ToString().Should().Contain("CustomPartition");
    }

    [Fact]
    public async Task WriteAsync_WithIdSelector_ShouldSetIdProperty()
    {
        // Arrange
        TestItem? capturedItem = null;

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Invokes((TestItem item, PartitionKey? _, ItemRequestOptions? _, CancellationToken _) => capturedItem = item)
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        var writer = new CosmosBulkWriter<TestItem>(
            _container,
            item => $"generated-{item.Name}",
            null,
            _configuration);

        // Act
        await writer.WriteAsync(new TestItem { Id = "original", Name = "test" });

        // Assert
        capturedItem.Should().NotBeNull();
        capturedItem!.Id.Should().StartWith("generated-");
    }

    [Fact]
    public async Task WriteBatchAsync_WithMultipleItems_ShouldWriteAllItems()
    {
        // Arrange
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        var items = new List<TestItem>
        {
            new() { Id = "test-1", Name = "Test 1" },
            new() { Id = "test-2", Name = "Test 2" },
            new() { Id = "test-3", Name = "Test 3" },
        };

        // Act
        await writer.WriteBatchAsync(items);

        // Assert
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappened(3, Times.Exactly);
    }

    [Fact]
    public async Task FlushAsync_WithNoPendingTasks_ShouldReturnImmediately()
    {
        // Arrange
        var writer = CreateWriter();

        // Act
        await writer.FlushAsync();

        // Assert - No exception should be thrown
    }

    [Fact]
    public async Task FlushAsync_WithPendingTasks_ShouldWaitForAllTasks()
    {
        // Arrange
        var writer = CreateWriter();
        var taskCompletionSource = new TaskCompletionSource<ItemResponse<TestItem>>();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(taskCompletionSource.Task);

        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test" });

        // Act - Flush should wait for pending tasks
        var flushTask = writer.FlushAsync();

        // Give the task a chance to start awaiting
        await Task.Delay(50);

        // Flush should not be completed yet since we haven't set the result
        flushTask.IsCompleted.Should().BeFalse();

        // Complete the pending task
        taskCompletionSource.SetResult(A.Fake<ItemResponse<TestItem>>());

        // Now flush should complete
        await flushTask;

        // Assert
        flushTask.IsCompleted.Should().BeTrue();
    }

    [Fact]
    public async Task FlushAsync_WhenTasksFailAndContinueOnErrorFalse_ShouldThrow()
    {
        // Arrange
        _configuration.ContinueOnError = false;
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .ThrowsAsync(new CosmosException("Test error", HttpStatusCode.InternalServerError, 0, "", 0));

        // Act - Write multiple items to trigger AggregateException from Task.WhenAll
        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test" });
        await writer.WriteAsync(new TestItem { Id = "test-2", Name = "Test" });

        // Assert - Task.WhenAll throws CosmosException directly when single task fails
        // When multiple tasks fail, it can throw AggregateException
        // But in practice with FakeItEasy, all tasks fail with same exception, so it throws CosmosException
        await Assert.ThrowsAsync<CosmosException>(async () => await writer.FlushAsync());
    }

    [Fact]
    public async Task FlushAsync_WhenTasksFailAndContinueOnErrorTrue_ShouldContinue()
    {
        // Arrange
        _configuration.ContinueOnError = true;
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .ThrowsAsync(new CosmosException("Test error", HttpStatusCode.InternalServerError, 0, "", 0));

        // Act - Write items (these queue fire-and-forget tasks)
        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test" });
        await writer.WriteAsync(new TestItem { Id = "test-2", Name = "Test" });
        await writer.WriteAsync(new TestItem { Id = "test-3", Name = "Test" });

        // Assert - With ContinueOnError = true, FlushAsync must not throw even when all tasks fail
        var exception = await Record.ExceptionAsync(async () => await writer.FlushAsync());
        exception.Should().BeNull();
    }

    [Fact]
    public async Task WriteAsync_AfterDisposal_ShouldThrowObjectDisposedException()
    {
        // Arrange
        var writer = CreateWriter();
        await writer.DisposeAsync();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await writer.WriteAsync(new TestItem { Id = "test", Name = "Test" }));
    }

    [Fact]
    public async Task FlushAsync_AfterDisposal_ShouldThrowObjectDisposedException()
    {
        // Arrange
        var writer = CreateWriter();
        await writer.DisposeAsync();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await writer.FlushAsync());
    }

    [Fact]
    public async Task DisposeAsync_ShouldFlushPendingTasks()
    {
        // Arrange
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test" });
        await writer.WriteAsync(new TestItem { Id = "test-2", Name = "Test" });

        // Act
        await writer.DisposeAsync();

        // Assert - Items should have been flushed during disposal
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappened(2, Times.Exactly);
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_ShouldNotThrow()
    {
        // Arrange
        var writer = CreateWriter();

        // Act & Assert - Should not throw
        await writer.DisposeAsync();
        await writer.DisposeAsync();
    }

    [Fact]
    public async Task WriteAsync_WithCancellationToken_ShouldPassTokenToContainer()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Act
        await writer.WriteAsync(new TestItem { Id = "test", Name = "Test" }, cts.Token);

        // Assert
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, cts.Token))
            .MustHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithPartitionKeyAttribute_ShouldExtractPartitionKey()
    {
        // Arrange
        PartitionKey? capturedPartitionKey = null;

        A.CallTo(() => _container.UpsertItemAsync(A<TestItemWithPartitionKey>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Invokes((TestItemWithPartitionKey item, PartitionKey? pk, ItemRequestOptions? _, CancellationToken _) => capturedPartitionKey = pk)
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItemWithPartitionKey>>()));

        var writer = new CosmosBulkWriter<TestItemWithPartitionKey>(
            _container,
            null,
            null,
            _configuration);

        // Act
        await writer.WriteAsync(new TestItemWithPartitionKey { Id = "test-1", Category = "Electronics" });

        // Assert
        capturedPartitionKey.Should().NotBeNull();
        capturedPartitionKey!.ToString().Should().Contain("Electronics");
    }

    private CosmosBulkWriter<TestItem> CreateWriter()
    {
        return new CosmosBulkWriter<TestItem>(
            _container,
            item => item.Id,
            null,
            _configuration);
    }

    public class TestItem
    {
        public string Id { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }

    public class TestItemWithPartitionKey
    {
        public string Id { get; set; } = string.Empty;

        [CosmosPartitionKey]
        public string Category { get; set; } = string.Empty;
    }
}
