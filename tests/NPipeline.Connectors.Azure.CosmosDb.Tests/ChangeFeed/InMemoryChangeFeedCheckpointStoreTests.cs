using AwesomeAssertions;
using NPipeline.Connectors.Azure.CosmosDb.ChangeFeed;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.ChangeFeed;

public sealed class InMemoryChangeFeedCheckpointStoreTests
{
    [Fact]
    public void Constructor_ShouldCreateEmptyStore()
    {
        // Arrange & Act
        var store = new InMemoryChangeFeedCheckpointStore();

        // Assert
        store.GetStoredKeys().Should().BeEmpty();
    }

    [Fact]
    public async Task GetTokenAsync_WithNoStoredToken_ShouldReturnNull()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();

        // Act
        var token = await store.GetTokenAsync("database1", "container1");

        // Assert
        token.Should().BeNull();
    }

    [Fact]
    public async Task SaveTokenAsync_ShouldStoreToken()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        const string expectedToken = "token123";

        // Act
        await store.SaveTokenAsync("database1", "container1", expectedToken);

        // Assert
        var retrievedToken = await store.GetTokenAsync("database1", "container1");
        retrievedToken.Should().Be(expectedToken);
    }

    [Fact]
    public async Task SaveTokenAsync_WithExistingToken_ShouldUpdateToken()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        await store.SaveTokenAsync("database1", "container1", "oldToken");

        // Act
        await store.SaveTokenAsync("database1", "container1", "newToken");

        // Assert
        var retrievedToken = await store.GetTokenAsync("database1", "container1");
        retrievedToken.Should().Be("newToken");
    }

    [Fact]
    public async Task DeleteTokenAsync_WithExistingToken_ShouldRemoveToken()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        await store.SaveTokenAsync("database1", "container1", "token123");

        // Act
        await store.DeleteTokenAsync("database1", "container1");

        // Assert
        var retrievedToken = await store.GetTokenAsync("database1", "container1");
        retrievedToken.Should().BeNull();
    }

    [Fact]
    public async Task DeleteTokenAsync_WithNonExistentToken_ShouldNotThrow()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();

        // Act & Assert - Should not throw
        await store.DeleteTokenAsync("database1", "container1");
    }

    [Fact]
    public async Task GetTokenAsync_WithDifferentContainers_ShouldReturnCorrectTokens()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        await store.SaveTokenAsync("database1", "container1", "token1");
        await store.SaveTokenAsync("database1", "container2", "token2");
        await store.SaveTokenAsync("database2", "container1", "token3");

        // Act & Assert
        (await store.GetTokenAsync("database1", "container1")).Should().Be("token1");
        (await store.GetTokenAsync("database1", "container2")).Should().Be("token2");
        (await store.GetTokenAsync("database2", "container1")).Should().Be("token3");
    }

    [Fact]
    public async Task DeleteTokenAsync_ShouldOnlyDeleteSpecificToken()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        await store.SaveTokenAsync("database1", "container1", "token1");
        await store.SaveTokenAsync("database1", "container2", "token2");

        // Act
        await store.DeleteTokenAsync("database1", "container1");

        // Assert
        (await store.GetTokenAsync("database1", "container1")).Should().BeNull();
        (await store.GetTokenAsync("database1", "container2")).Should().Be("token2");
    }

    [Fact]
    public async Task GetStoredKeys_ShouldReturnAllKeys()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        await store.SaveTokenAsync("database1", "container1", "token1");
        await store.SaveTokenAsync("database1", "container2", "token2");
        await store.SaveTokenAsync("database2", "container1", "token3");

        // Act
        var keys = store.GetStoredKeys();

        // Assert
        keys.Should().HaveCount(3);
        keys.Should().Contain(["database1|container1", "database1|container2", "database2|container1"]);
    }

    [Fact]
    public void GetStoredKeys_WhenEmpty_ShouldReturnEmptyCollection()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();

        // Act
        var keys = store.GetStoredKeys();

        // Assert
        keys.Should().BeEmpty();
    }

    [Fact]
    public async Task Clear_ShouldRemoveAllTokens()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        await store.SaveTokenAsync("database1", "container1", "token1");
        await store.SaveTokenAsync("database1", "container2", "token2");

        // Act
        store.Clear();

        // Assert
        store.GetStoredKeys().Should().BeEmpty();
    }

    [Fact]
    public async Task SaveTokenAsync_WithEmptyToken_ShouldStoreEmptyToken()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();

        // Act
        await store.SaveTokenAsync("database1", "container1", "");

        // Assert
        var retrievedToken = await store.GetTokenAsync("database1", "container1");
        retrievedToken.Should().Be("");
    }

    [Fact]
    public async Task SaveTokenAsync_WithNullToken_ShouldStoreNullToken()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();

        // Act
        await store.SaveTokenAsync("database1", "container1", null!);

        // Assert - Null token is stored but GetTokenAsync returns null
        store.GetStoredKeys().Should().Contain("database1|container1");
    }

    [Fact]
    public async Task ConcurrentAccess_ShouldBeThreadSafe()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        const int concurrentOperations = 100;
        var tasks = new List<Task>();

        // Act - Concurrent writes
        for (var i = 0; i < concurrentOperations; i++)
        {
            var index = i;
            tasks.Add(Task.Run(async () => { await store.SaveTokenAsync($"database{index % 10}", $"container{index}", $"token{index}"); }));
        }

        await Task.WhenAll(tasks);

        // Assert - All tokens should be stored
        store.GetStoredKeys().Should().HaveCount(concurrentOperations);
    }

    [Fact]
    public async Task ConcurrentReadWrite_ShouldBeThreadSafe()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        const int concurrentOperations = 50;
        var writeTasks = new List<Task>();
        var readTasks = new List<Task<string?>>();

        // Act - Concurrent reads and writes
        for (var i = 0; i < concurrentOperations; i++)
        {
            var index = i;
            writeTasks.Add(Task.Run(async () => { await store.SaveTokenAsync("database1", "container1", $"token{index}"); }));

            readTasks.Add(Task.Run(async () => { return await store.GetTokenAsync("database1", "container1"); }));
        }

        await Task.WhenAll(writeTasks.Concat(readTasks));

        // Assert - Final token should be one of the written values
        var finalToken = await store.GetTokenAsync("database1", "container1");
        finalToken.Should().StartWith("token");
    }

    [Theory]
    [InlineData("db", "container")]
    [InlineData("my-database", "my-container")]
    [InlineData("database123", "container456")]
    public async Task SaveAndGetToken_WithVariousNames_ShouldWorkCorrectly(string databaseId, string containerId)
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        const string token = "testToken";

        // Act
        await store.SaveTokenAsync(databaseId, containerId, token);
        var retrievedToken = await store.GetTokenAsync(databaseId, containerId);

        // Assert
        retrievedToken.Should().Be(token);
    }

    [Fact]
    public async Task GetTokenAsync_WithCancellationToken_ShouldComplete()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        await store.SaveTokenAsync("database1", "container1", "token1");
        var cts = new CancellationTokenSource();

        // Act
        var token = await store.GetTokenAsync("database1", "container1", cts.Token);

        // Assert
        token.Should().Be("token1");
    }

    [Fact]
    public async Task SaveTokenAsync_WithCancellationToken_ShouldComplete()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        var cts = new CancellationTokenSource();

        // Act
        await store.SaveTokenAsync("database1", "container1", "token1", cts.Token);

        // Assert
        var token = await store.GetTokenAsync("database1", "container1");
        token.Should().Be("token1");
    }

    [Fact]
    public async Task DeleteTokenAsync_WithCancellationToken_ShouldComplete()
    {
        // Arrange
        var store = new InMemoryChangeFeedCheckpointStore();
        await store.SaveTokenAsync("database1", "container1", "token1");
        var cts = new CancellationTokenSource();

        // Act
        await store.DeleteTokenAsync("database1", "container1", cts.Token);

        // Assert
        var token = await store.GetTokenAsync("database1", "container1");
        token.Should().BeNull();
    }
}
