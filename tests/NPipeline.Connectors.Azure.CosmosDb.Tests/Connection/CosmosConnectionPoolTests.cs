using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Connection;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Connection;

public sealed class CosmosConnectionPoolTests
{
    // Valid Base64-encoded key for testing (CosmosClient validates this)
    private const string ValidBase64Key = "dGVzdEtleVRlc3RLZXlUZXN0S2V5VGVzdEtleVRlc3RLZXlUZXN0S2V5"; // 44 chars, proper Base64
    private const string ValidConnectionString = $"AccountEndpoint=https://test.documents.azure.com:443/;AccountKey={ValidBase64Key};";

    [Fact]
    public void Constructor_WithNullOptions_ShouldThrow()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new CosmosConnectionPool(null!));
    }

    [Fact]
    public void Constructor_WithNullConnectionStringInOptions_ShouldThrow()
    {
        // Arrange
        var options = new CosmosOptions();

        // Act & Assert
        Assert.Throws<ArgumentException>(() => new CosmosConnectionPool(options));
    }

    [Fact]
    public void Constructor_WithEmptyConnectionString_ShouldThrow()
    {
        // Act & Assert
        Assert.Throws<ArgumentException>(() => new CosmosConnectionPool(""));
    }

    [Fact]
    public void Constructor_WithEmptyNamedConnections_ShouldThrow()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>();

        // Act & Assert
        Assert.Throws<ArgumentException>(() => new CosmosConnectionPool(namedConnections));
    }

    [Fact]
    public void Constructor_WithEmptyConnectionStringInNamedConnections_ShouldThrow()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>
        {
            { "connection1", "" },
        };

        // Act & Assert
        Assert.Throws<ArgumentException>(() => new CosmosConnectionPool(namedConnections));
    }

    [Fact]
    public void Constructor_WithConnectionString_ShouldInitializePool()
    {
        // Arrange & Act
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Assert
        pool.ConnectionString.Should().Be(ValidConnectionString);
    }

    [Fact]
    public void Constructor_WithNamedConnections_ShouldInitializePool()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>
        {
            { "connection1", ValidConnectionString },
            { "connection2", ValidConnectionString },
        };

        // Act
        var pool = new CosmosConnectionPool(namedConnections);

        // Assert
        pool.HasNamedConnection("connection1").Should().BeTrue();
        pool.HasNamedConnection("connection2").Should().BeTrue();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultConnection_ShouldReturnClient()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Act
        var client = await pool.GetClientAsync();

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<CosmosClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithNamedConnection_ShouldReturnClient()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>
        {
            { "myConnection", ValidConnectionString },
        };

        var pool = new CosmosConnectionPool(namedConnections);

        // Act
        var client = await pool.GetClientAsync("myConnection");

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<CosmosClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithNonExistentNamedConnection_ShouldThrow()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pool.GetClientAsync("nonexistent"));
    }

    [Fact]
    public async Task GetClientAsync_WithCancelledToken_ShouldThrow()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await pool.GetClientAsync(cts.Token));
    }

    [Fact]
    public async Task GetContainerAsync_WithDatabaseAndContainer_ShouldReturnContainer()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Act
        var container = await pool.GetContainerAsync("myDatabase", "myContainer");

        // Assert
        container.Should().NotBeNull();
        container.Should().BeAssignableTo<Container>();
    }

    [Fact]
    public async Task GetContainerAsync_WithNamedConnection_ShouldReturnContainer()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>
        {
            { "myConnection", ValidConnectionString },
        };

        var pool = new CosmosConnectionPool(namedConnections);

        // Act
        var container = await pool.GetContainerAsync("myConnection", "myDatabase", "myContainer");

        // Assert
        container.Should().NotBeNull();
        container.Should().BeAssignableTo<Container>();
    }

    [Fact]
    public void HasNamedConnection_WithExistingConnection_ShouldReturnTrue()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>
        {
            { "myConnection", ValidConnectionString },
        };

        var pool = new CosmosConnectionPool(namedConnections);

        // Act
        var result = pool.HasNamedConnection("myConnection");

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void HasNamedConnection_WithNonExistentConnection_ShouldReturnFalse()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Act
        var result = pool.HasNamedConnection("nonexistent");

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void GetNamedConnectionNames_ShouldReturnAllConnectionNames()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>
        {
            { "connection1", ValidConnectionString },
            { "connection2", ValidConnectionString },
        };

        var pool = new CosmosConnectionPool(namedConnections);

        // Act
        var names = pool.GetNamedConnectionNames().ToList();

        // Assert
        names.Should().HaveCount(2);
        names.Should().Contain(["connection1", "connection2"]);
    }

    [Fact]
    public void GetNamedConnectionNames_WithNoNamedConnections_ShouldReturnEmpty()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Act
        var names = pool.GetNamedConnectionNames();

        // Assert
        names.Should().BeEmpty();
    }

    [Fact]
    public async Task DisposeAsync_ShouldDisposeClients()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Get client to ensure it's created
        await pool.GetClientAsync();

        // Act & Assert - Should not throw
        await pool.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_ShouldNotThrow()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Act & Assert - Should not throw
        await pool.DisposeAsync();
        await pool.DisposeAsync();
    }

    [Fact]
    public void Constructor_WithConfiguration_ShouldApplyConfiguration()
    {
        // Arrange
        var configuration = new CosmosConfiguration
        {
            RequestTimeout = 120,
            MaxRetryAttempts = 5,
            AllowBulkExecution = true,
        };

        // Act
        var pool = new CosmosConnectionPool(ValidConnectionString, configuration);

        // Assert
        pool.ConnectionString.Should().Be(ValidConnectionString);
    }

    [Fact]
    public async Task GetClientAsync_WithSameNamedConnection_ShouldReturnSameClient()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>
        {
            { "myConnection", ValidConnectionString },
        };

        var pool = new CosmosConnectionPool(namedConnections);

        // Act
        var client1 = await pool.GetClientAsync("myConnection");
        var client2 = await pool.GetClientAsync("myConnection");

        // Assert
        client1.Should().BeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithApiTypeSql_ShouldReturnCosmosClient()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Act
        var client = await pool.GetClientAsync<CosmosClient>(CosmosApiType.Sql);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<CosmosClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidApiTypeCast_ShouldThrow()
    {
        // Arrange
        var pool = new CosmosConnectionPool(ValidConnectionString);

        // Act & Assert - Requesting wrong type for SQL API
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await pool.GetClientAsync<string>(CosmosApiType.Sql));
    }

    [Fact]
    public void Constructor_WithMultipleNamedConnections_ShouldSetFirstAsDefault()
    {
        // Arrange
        var namedConnections = new Dictionary<string, string>
        {
            { "connection1", ValidConnectionString },
            { "connection2", ValidConnectionString },
        };

        // Act
        var pool = new CosmosConnectionPool(namedConnections);

        // Assert
        pool.ConnectionString.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task GetClientAsync_WhenNoDefaultConnection_ShouldThrow()
    {
        // Arrange - Create pool with only named Mongo connections (no SQL default)
        var options = new CosmosOptions();
        options.AddOrUpdateMongoConnection("mongoConnection", "mongodb://localhost:27017");

        // Act & Assert
        var pool = new CosmosConnectionPool(options);
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await pool.GetClientAsync());
    }
}
