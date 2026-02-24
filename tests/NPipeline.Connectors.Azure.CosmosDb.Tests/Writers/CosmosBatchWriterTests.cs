using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.Connectors.Azure.CosmosDb.Writers;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Writers;

public sealed class CosmosBatchWriterTests
{
    private readonly CosmosConfiguration _configuration;
    private readonly Container _container;

    public CosmosBatchWriterTests()
    {
        _container = A.Fake<Container>();

        _configuration = new CosmosConfiguration
        {
            WriteBatchSize = 3,
            WriteStrategy = CosmosWriteStrategy.Upsert,
            ContinueOnError = false,
        };
    }

    [Fact]
    public void Constructor_WithNullContainer_ShouldNotThrow()
    {
        // Arrange & Act - Constructor doesn't validate null container
        var writer = new CosmosBatchWriter<TestItem>(
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
        var writer = new CosmosBatchWriter<TestItem>(
            _container,
            null,
            null,
            null!);

        // Assert - Writer is created (validation happens at usage time)
        writer.Should().NotBeNull();
    }

    [Fact]
    public async Task WriteAsync_WithSingleItem_ShouldBufferItem()
    {
        // Arrange
        var writer = CreateWriter();
        var item = new TestItem { Id = "test-1", Name = "Test" };

        // Act
        await writer.WriteAsync(item);

        // Assert - Item should be buffered, not yet written
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WhenBufferReachesBatchSize_ShouldFlushAutomatically()
    {
        // Arrange
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Act - Write items up to batch size
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", Name = $"Test {i}" });
        }

        // Assert - Items should have been flushed
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappened(_configuration.WriteBatchSize, Times.Exactly);
    }

    [Fact]
    public async Task WriteAsync_WithInsertStrategy_ShouldUseCreateItem()
    {
        // Arrange
        _configuration.WriteStrategy = CosmosWriteStrategy.Insert;
        var writer = CreateWriter();

        A.CallTo(() => _container.CreateItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Act - Write items up to batch size to trigger flush
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", Name = $"Test {i}" });
        }

        // Assert
        A.CallTo(() => _container.CreateItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappened(_configuration.WriteBatchSize, Times.Exactly);
    }

    [Fact]
    public async Task WriteAsync_WithUpsertStrategy_ShouldUseUpsertItem()
    {
        // Arrange
        _configuration.WriteStrategy = CosmosWriteStrategy.Upsert;
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Act - Write items up to batch size to trigger flush
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", Name = $"Test {i}" });
        }

        // Assert
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappened(_configuration.WriteBatchSize, Times.Exactly);
    }

    [Fact]
    public async Task FlushAsync_WithEmptyBuffer_ShouldNotCallContainer()
    {
        // Arrange
        var writer = CreateWriter();

        // Act
        await writer.FlushAsync();

        // Assert
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task FlushAsync_WithBufferedItems_ShouldWriteAllItems()
    {
        // Arrange
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Buffer some items (less than batch size)
        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test 1" });
        await writer.WriteAsync(new TestItem { Id = "test-2", Name = "Test 2" });

        // Act
        await writer.FlushAsync();

        // Assert
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappened(2, Times.Exactly);
    }

    [Fact]
    public async Task FlushAsync_AfterFlush_ShouldClearBuffer()
    {
        // Arrange
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test 1" });
        await writer.FlushAsync();

        // Act - Flush again
        await writer.FlushAsync();

        // Assert - No additional calls
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .MustHaveHappened(1, Times.Exactly);
    }

    [Fact]
    public async Task WriteAsync_WithPartitionKeySelector_ShouldUseCustomPartitionKey()
    {
        // Arrange
        PartitionKey? capturedPartitionKey = null;

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Invokes((TestItem item, PartitionKey? pk, ItemRequestOptions? _, CancellationToken _) => capturedPartitionKey = pk)
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        var writer = new CosmosBatchWriter<TestItem>(
            _container,
            item => item.Id,
            item => new PartitionKey(item.Name),
            _configuration);

        // Act
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", Name = "CustomPartition" });
        }

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

        var writer = new CosmosBatchWriter<TestItem>(
            _container,
            item => $"generated-{item.Name}",
            null,
            _configuration);

        // Act
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = "original", Name = $"test-{i}" });
        }

        // Assert
        capturedItem.Should().NotBeNull();
        capturedItem!.Id.Should().StartWith("generated-");
    }

    [Fact]
    public async Task WriteAsync_WhenBatchFailsAndContinueOnErrorFalse_ShouldThrow()
    {
        // Arrange
        _configuration.ContinueOnError = false;
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .ThrowsAsync(new CosmosException("Test error", HttpStatusCode.InternalServerError, 0, "", 0));

        // Act & Assert - Write items up to batch size to trigger automatic flush
        // Task.WhenAll throws the exception directly (not AggregateException) when single task fails
        for (var i = 0; i < _configuration.WriteBatchSize - 1; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", Name = $"Test {i}" });
        }

        // The last write triggers auto-flush which throws CosmosException
        await Assert.ThrowsAsync<CosmosException>(() => writer.WriteAsync(new TestItem { Id = "last", Name = "Test" }));
    }

    [Fact]
    public async Task WriteAsync_WhenBatchFailsAndContinueOnErrorTrue_ShouldContinue()
    {
        // Arrange
        _configuration.ContinueOnError = true;
        _configuration.WriteBatchSize = 3; // Set batch size to 3 so auto-flush triggers
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .ThrowsAsync(new CosmosException("Test error", HttpStatusCode.InternalServerError, 0, "", 0));

        // Act - Write items up to batch size to trigger auto-flush
        for (var i = 0; i < _configuration.WriteBatchSize - 1; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", Name = $"Test {i}" });
        }

        // The last write triggers auto-flush - with ContinueOnError = true, no exception should propagate
        var exception = await Record.ExceptionAsync(() => writer.WriteAsync(new TestItem { Id = "last", Name = "Test" }));
        exception.Should().BeNull();

        // Failed writes should be tracked
        writer.FailedWriteCount.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task WriteAsync_AfterDisposal_ShouldThrowObjectDisposedException()
    {
        // Arrange
        var writer = CreateWriter();
        await writer.DisposeAsync();

        // Act & Assert
        var act = async () => await writer.WriteAsync(new TestItem { Id = "test", Name = "Test" });
        await Assert.ThrowsAsync<ObjectDisposedException>(act);
    }

    [Fact]
    public async Task FlushAsync_AfterDisposal_ShouldThrowObjectDisposedException()
    {
        // Arrange
        var writer = CreateWriter();
        await writer.DisposeAsync();

        // Act & Assert
        var act = async () => await writer.FlushAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(act);
    }

    [Fact]
    public async Task DisposeAsync_ShouldFlushRemainingItems()
    {
        // Arrange
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Buffer items without flushing
        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test 1" });
        await writer.WriteAsync(new TestItem { Id = "test-2", Name = "Test 2" });

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
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", Name = $"Test {i}" }, cts.Token);
        }

        // Assert
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, cts.Token))
            .MustHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithCancelledToken_ShouldPassTokenToContainer()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        cts.Cancel();
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        // Act - Write items up to batch size to trigger flush
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", Name = $"Test {i}" }, cts.Token);
        }

        // Assert - The cancelled token should be passed to the container
        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, cts.Token))
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

        var writer = new CosmosBatchWriter<TestItemWithPartitionKey>(
            _container,
            null,
            null,
            _configuration);

        // Act
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItemWithPartitionKey { Id = $"test-{i}", Category = "Electronics" });
        }

        // Assert
        capturedPartitionKey.Should().NotBeNull();
        capturedPartitionKey!.ToString().Should().Contain("Electronics");
    }

    private CosmosBatchWriter<TestItem> CreateWriter()
    {
        return new CosmosBatchWriter<TestItem>(
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
