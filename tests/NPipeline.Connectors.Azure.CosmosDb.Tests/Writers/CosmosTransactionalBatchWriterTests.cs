using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.Connectors.Azure.CosmosDb.Writers;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Writers;

public sealed class CosmosTransactionalBatchWriterTests
{
    private readonly CosmosConfiguration _configuration;
    private readonly Container _container;

    public CosmosTransactionalBatchWriterTests()
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
        var writer = new CosmosTransactionalBatchWriter<TestItem>(
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
        var writer = new CosmosTransactionalBatchWriter<TestItem>(
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
        var item = new TestItem { Id = "test-1", PartitionKeyValue = "partition1" };

        // Act
        await writer.WriteAsync(item);

        // Assert - Item should be buffered, not yet written
        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WhenBufferReachesBatchSize_ShouldFlushAutomatically()
    {
        // Arrange
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.CreateItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        // Act - Write items up to batch size (all with same partition key)
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", PartitionKeyValue = "partition1" });
        }

        // Assert - Batch should have been created and executed
        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .MustHaveHappenedOnceExactly();

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithDifferentPartitions_ShouldBufferSeparately()
    {
        // Arrange
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        // Act - Write items to different partitions
        await writer.WriteAsync(new TestItem { Id = "test-1", PartitionKeyValue = "partition1" });
        await writer.WriteAsync(new TestItem { Id = "test-2", PartitionKeyValue = "partition2" });

        // Assert - Items should be buffered separately, no flush yet
        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithInsertStrategy_ShouldUseCreateItem()
    {
        // Arrange
        _configuration.WriteStrategy = CosmosWriteStrategy.Insert;
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.CreateItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        // Act
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", PartitionKeyValue = "partition1" });
        }

        // Assert
        A.CallTo(() => mockBatch.CreateItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .MustHaveHappened(_configuration.WriteBatchSize, Times.Exactly);
    }

    [Fact]
    public async Task WriteAsync_WithUpsertStrategy_ShouldUseUpsertItem()
    {
        // Arrange
        _configuration.WriteStrategy = CosmosWriteStrategy.Upsert;
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        // Act
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", PartitionKeyValue = "partition1" });
        }

        // Assert
        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
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
        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .MustNotHaveHappened();
    }

    [Fact]
    public async Task FlushAsync_WithBufferedItems_ShouldWriteAllItems()
    {
        // Arrange
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        // Buffer items in different partitions
        await writer.WriteAsync(new TestItem { Id = "test-1", PartitionKeyValue = "partition1" });
        await writer.WriteAsync(new TestItem { Id = "test-2", PartitionKeyValue = "partition2" });

        // Act
        await writer.FlushAsync();

        // Assert - Should create batch for each partition
        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .MustHaveHappened(2, Times.Exactly);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .MustHaveHappened(2, Times.Exactly);
    }

    [Fact]
    public async Task FlushAsync_AfterFlush_ShouldClearBuffer()
    {
        // Arrange
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        await writer.WriteAsync(new TestItem { Id = "test-1", PartitionKeyValue = "partition1" });
        await writer.FlushAsync();

        // Act - Flush again
        await writer.FlushAsync();

        // Assert - No additional batch executions
        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task WriteAsync_WithPartitionKeySelector_ShouldUseCustomPartitionKey()
    {
        // Arrange
        PartitionKey? capturedPartitionKey = null;
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Invokes((PartitionKey pk) => capturedPartitionKey = pk)
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        var writer = new CosmosTransactionalBatchWriter<TestItem>(
            _container,
            item => item.Id,
            item => new PartitionKey(item.PartitionKeyValue),
            _configuration);

        // Act
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", PartitionKeyValue = "CustomPartition" });
        }

        // Assert
        capturedPartitionKey.Should().NotBeNull();
        capturedPartitionKey!.ToString().Should().Contain("CustomPartition");
    }

    [Fact]
    public async Task WriteAsync_WhenBatchFailsAndContinueOnErrorFalse_ShouldThrow()
    {
        // Arrange
        _configuration.ContinueOnError = false;
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(false);

        A.CallTo(() => mockResponse.StatusCode)
            .Returns(HttpStatusCode.InternalServerError);

        A.CallTo(() => mockResponse.ActivityId)
            .Returns("test-activity");

        A.CallTo(() => mockResponse.RequestCharge)
            .Returns(1.0);

        // Act & Assert - Write items up to batch size, which triggers automatic flush
        // The exception is thrown during WriteAsync when batch size is reached
        for (var i = 0; i < _configuration.WriteBatchSize - 1; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", PartitionKeyValue = "partition1" });
        }

        // The last write triggers the flush which fails
        await Assert.ThrowsAsync<CosmosException>(() => writer.WriteAsync(new TestItem { Id = "last", PartitionKeyValue = "partition1" }));
    }

    [Fact]
    public async Task WriteAsync_WhenBatchFailsAndContinueOnErrorTrue_ShouldContinue()
    {
        // Arrange
        _configuration.ContinueOnError = true;
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(false);

        A.CallTo(() => mockResponse.StatusCode)
            .Returns(HttpStatusCode.InternalServerError);

        // Act - Write items up to batch size
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", PartitionKeyValue = "partition1" });
        }

        // Assert - Should not throw due to ContinueOnError = true
        var exception = await Record.ExceptionAsync(() => writer.FlushAsync());
        exception.Should().BeNull();
    }

    [Fact]
    public async Task WriteAsync_AfterDisposal_ShouldThrowObjectDisposedException()
    {
        // Arrange
        var writer = CreateWriter();
        await writer.DisposeAsync();

        // Act & Assert
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await writer.WriteAsync(new TestItem { Id = "test", PartitionKeyValue = "partition1" }));
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
    public async Task DisposeAsync_ShouldFlushRemainingItems()
    {
        // Arrange
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        // Buffer items without flushing
        await writer.WriteAsync(new TestItem { Id = "test-1", PartitionKeyValue = "partition1" });
        await writer.WriteAsync(new TestItem { Id = "test-2", PartitionKeyValue = "partition1" });

        // Act
        await writer.DisposeAsync();

        // Assert - Items should have been flushed during disposal
        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
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
    public async Task WriteAsync_WithCancellationToken_ShouldPassTokenToBatch()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        var writer = CreateWriter();
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        // Act
        for (var i = 0; i < _configuration.WriteBatchSize; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", PartitionKeyValue = "partition1" }, cts.Token);
        }

        // Assert
        A.CallTo(() => mockBatch.ExecuteAsync(cts.Token))
            .MustHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithPartitionKeyAttribute_ShouldExtractPartitionKey()
    {
        // Arrange
        PartitionKey? capturedPartitionKey = null;
        var mockBatch = A.Fake<TransactionalBatch>();
        var mockResponse = A.Fake<TransactionalBatchResponse>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Invokes((PartitionKey pk) => capturedPartitionKey = pk)
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItemWithPartitionKey>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Returns(mockResponse);

        A.CallTo(() => mockResponse.IsSuccessStatusCode)
            .Returns(true);

        var writer = new CosmosTransactionalBatchWriter<TestItemWithPartitionKey>(
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

    [Fact]
    public async Task WriteAsync_WithMoreThan100Items_ShouldSplitIntoMultipleBatches()
    {
        // Arrange - Transactional batches support max 100 operations per batch
        // Set WriteBatchSize to 101 so all 101 items are flushed together
        _configuration.WriteBatchSize = 101;
        var writer = CreateWriter();
        var mockResponse = A.Fake<TransactionalBatchResponse>();
        A.CallTo(() => mockResponse.IsSuccessStatusCode).Returns(true);

        var executeCallCount = 0;
        var mockBatch = A.Fake<TransactionalBatch>();

        A.CallTo(() => _container.CreateTransactionalBatch(A<PartitionKey>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.UpsertItem(A<TestItem>._, A<TransactionalBatchItemRequestOptions>._))
            .Returns(mockBatch);

        A.CallTo(() => mockBatch.ExecuteAsync(A<CancellationToken>._))
            .Invokes(() => executeCallCount++)
            .Returns(mockResponse);

        // Act - Write 101 items to the same partition (triggers auto-flush at 101)
        for (var i = 0; i < 101; i++)
        {
            await writer.WriteAsync(new TestItem { Id = $"test-{i}", PartitionKeyValue = "partition1" });
        }

        // Assert - Should have executed 2 batches (100 + 1) since transactional batch max is 100
        executeCallCount.Should().Be(2);
    }

    private CosmosTransactionalBatchWriter<TestItem> CreateWriter()
    {
        return new CosmosTransactionalBatchWriter<TestItem>(
            _container,
            item => item.Id,
            item => new PartitionKey(item.PartitionKeyValue),
            _configuration);
    }

    public class TestItem
    {
        public string Id { get; set; } = string.Empty;
        public string PartitionKeyValue { get; set; } = string.Empty;
    }

    public class TestItemWithPartitionKey
    {
        public string Id { get; set; } = string.Empty;

        [CosmosPartitionKey]
        public string Category { get; set; } = string.Empty;
    }
}
