using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;
using NPipeline.Connectors.Azure.CosmosDb.Writers;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Writers;

public sealed class CosmosPerRowWriterTests
{
    private readonly CosmosConfiguration _configuration;
    private readonly Container _container;

    public CosmosPerRowWriterTests()
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
        var writer = new CosmosPerRowWriter<TestItem>(
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
        var writer = new CosmosPerRowWriter<TestItem>(
            _container,
            null,
            null,
            null!);

        // Assert - Writer is created (validation happens at usage time)
        writer.Should().NotBeNull();
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

        var writer = new CosmosPerRowWriter<TestItem>(
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

        var writer = new CosmosPerRowWriter<TestItem>(
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
    public async Task WriteBatchAsync_WithMultipleItems_ShouldWriteEachItemSequentially()
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
    public void FlushAsync_ShouldReturnCompletedTask()
    {
        // Arrange
        var writer = CreateWriter();

        // Act
        var task = writer.FlushAsync();

        // Assert - Per-row writer has no buffer, so flush is a no-op
        task.IsCompleted.Should().BeTrue();
    }

    [Fact]
    public async Task WriteAsync_WhenConflictOccursAndContinueOnErrorTrue_ShouldContinue()
    {
        // Arrange
        _configuration.ContinueOnError = true;
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .ThrowsAsync(new CosmosException("Conflict", HttpStatusCode.Conflict, 0, "", 0));

        // Act & Assert - Should not throw due to ContinueOnError = true
        var exception = await Record.ExceptionAsync(() => writer.WriteAsync(new TestItem { Id = "test", Name = "Test" }));
        exception.Should().BeNull();
    }

    [Fact]
    public async Task WriteAsync_WhenConflictOccursAndContinueOnErrorFalse_ShouldThrow()
    {
        // Arrange
        _configuration.ContinueOnError = false;
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .ThrowsAsync(new CosmosException("Conflict", HttpStatusCode.Conflict, 0, "", 0));

        // Act & Assert
        await Assert.ThrowsAsync<CosmosException>(async () => await writer.WriteAsync(new TestItem { Id = "test", Name = "Test" }));
    }

    [Fact]
    public async Task WriteAsync_WhenNonConflictErrorOccurs_ShouldThrow()
    {
        // Arrange
        var writer = CreateWriter();

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .ThrowsAsync(new CosmosException("Internal error", HttpStatusCode.InternalServerError, 0, "", 0));

        // Act & Assert - Should throw even with ContinueOnError = true (only handles Conflict)
        _configuration.ContinueOnError = true;
        await Assert.ThrowsAsync<CosmosException>(async () => await writer.WriteAsync(new TestItem { Id = "test", Name = "Test" }));
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
    public async Task DisposeAsync_ShouldNotThrow()
    {
        // Arrange
        var writer = CreateWriter();

        // Act & Assert - Should not throw
        await writer.DisposeAsync();
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

        var writer = new CosmosPerRowWriter<TestItemWithPartitionKey>(
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

    [Fact]
    public async Task WriteAsync_WithNoPartitionKey_ShouldUsePartitionKeyNone()
    {
        // Arrange
        PartitionKey? capturedPartitionKey = null;

        A.CallTo(() => _container.UpsertItemAsync(A<TestItem>._, A<PartitionKey?>._, A<ItemRequestOptions>._, A<CancellationToken>._))
            .Invokes((TestItem item, PartitionKey? pk, ItemRequestOptions? _, CancellationToken _) => capturedPartitionKey = pk)
            .Returns(Task.FromResult(A.Fake<ItemResponse<TestItem>>()));

        var writer = new CosmosPerRowWriter<TestItem>(
            _container,
            item => item.Id,
            null, // No partition key selector
            _configuration);

        // Act
        await writer.WriteAsync(new TestItem { Id = "test-1", Name = "Test" });

        // Assert
        capturedPartitionKey.Should().NotBeNull();
        capturedPartitionKey!.ToString().Should().Contain("None");
    }

    private CosmosPerRowWriter<TestItem> CreateWriter()
    {
        return new CosmosPerRowWriter<TestItem>(
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
