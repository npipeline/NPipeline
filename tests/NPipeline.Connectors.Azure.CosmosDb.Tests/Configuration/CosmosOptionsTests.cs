using AwesomeAssertions;
using Azure.Core;
using FakeItEasy;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Configuration;

public sealed class CosmosOptionsTests
{
    [Fact]
    public void DefaultConstructor_ShouldInitializeEmptyCollections()
    {
        // Arrange & Act
        var options = new CosmosOptions();

        // Assert
        options.DefaultConnectionString.Should().BeNull();
        options.DefaultMongoConnectionString.Should().BeNull();
        options.DefaultCassandraConnection.Should().BeNull();
        options.DefaultEndpoint.Should().BeNull();
        options.DefaultCredential.Should().BeNull();
        options.DefaultConfiguration.Should().BeNull();
        options.NamedConnections.Should().BeEmpty();
        options.NamedEndpoints.Should().BeEmpty();
        options.NamedMongoConnections.Should().BeEmpty();
        options.NamedCassandraConnections.Should().BeEmpty();
    }

    [Fact]
    public void DefaultConnectionString_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CosmosOptions();
        const string connectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;";

        // Act
        options.DefaultConnectionString = connectionString;

        // Assert
        options.DefaultConnectionString.Should().Be(connectionString);
        options.AzureConnections.DefaultConnectionString.Should().Be(connectionString);
    }

    [Fact]
    public void DefaultEndpoint_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CosmosOptions();
        var endpoint = new Uri("https://account.documents.azure.com:443/");

        // Act
        options.DefaultEndpoint = endpoint;

        // Assert
        options.DefaultEndpoint.Should().Be(endpoint);
        options.AzureConnections.DefaultEndpoint?.Endpoint.Should().Be(endpoint);
    }

    [Fact]
    public void DefaultEndpoint_WhenSetToNull_ShouldClearAzureEndpoint()
    {
        // Arrange
        var options = new CosmosOptions
        {
            DefaultEndpoint = new Uri("https://account.documents.azure.com:443/"),
        };

        // Act
        options.DefaultEndpoint = null;

        // Assert
        options.DefaultEndpoint.Should().BeNull();
    }

    [Fact]
    public void DefaultCredential_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CosmosOptions();
        var credential = A.Fake<TokenCredential>();

        // Act
        options.DefaultCredential = credential;

        // Assert
        options.DefaultCredential.Should().Be(credential);
        options.AzureConnections.DefaultEndpoint?.Credential.Should().Be(credential);
    }

    [Fact]
    public void DefaultCredential_WhenEndpointDoesNotExist_ShouldCreateEndpoint()
    {
        // Arrange
        var options = new CosmosOptions();
        var credential = A.Fake<TokenCredential>();

        // Act
        options.DefaultCredential = credential;

        // Assert
        options.AzureConnections.DefaultEndpoint.Should().NotBeNull();
        options.DefaultCredential.Should().Be(credential);
    }

    [Fact]
    public void AddOrUpdateConnection_ShouldAddConnectionToBothDictionaries()
    {
        // Arrange
        var options = new CosmosOptions();
        const string name = "MyConnection";
        const string connectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;";

        // Act
        var result = options.AddOrUpdateConnection(name, connectionString);

        // Assert
        result.Should().BeSameAs(options); // Fluent chaining
        options.NamedConnections.Should().ContainKey(name);
        options.NamedConnections[name].Should().Be(connectionString);
        options.AzureConnections.GetConnectionString(name).Should().Be(connectionString);
    }

    [Fact]
    public void AddOrUpdateConnection_WithExistingName_ShouldUpdateConnection()
    {
        // Arrange
        var options = new CosmosOptions();
        const string name = "MyConnection";
        const string originalConnectionString = "Original";
        const string updatedConnectionString = "Updated";

        // Act
        options.AddOrUpdateConnection(name, originalConnectionString);
        options.AddOrUpdateConnection(name, updatedConnectionString);

        // Assert
        options.NamedConnections[name].Should().Be(updatedConnectionString);
    }

    [Fact]
    public void AddOrUpdateMongoConnection_ShouldAddConnection()
    {
        // Arrange
        var options = new CosmosOptions();
        const string name = "MyMongoConnection";
        const string connectionString = "mongodb://account:password@account.mongo.cosmos.azure.com:10255/";

        // Act
        var result = options.AddOrUpdateMongoConnection(name, connectionString);

        // Assert
        result.Should().BeSameAs(options); // Fluent chaining
        options.NamedMongoConnections.Should().ContainKey(name);
        options.NamedMongoConnections[name].Should().Be(connectionString);
    }

    [Fact]
    public void AddOrUpdateCassandraConnection_ShouldAddConnection()
    {
        // Arrange
        var options = new CosmosOptions();
        const string name = "MyCassandraConnection";

        var connection = new CassandraConnectionOptions
        {
            ContactPoint = "account.cassandra.cosmos.azure.com",
            Port = 10350,
            Keyspace = "mykeyspace",
        };

        // Act
        var result = options.AddOrUpdateCassandraConnection(name, connection);

        // Assert
        result.Should().BeSameAs(options); // Fluent chaining
        options.NamedCassandraConnections.Should().ContainKey(name);
        options.NamedCassandraConnections[name].Should().Be(connection);
    }

    [Fact]
    public void AddOrUpdateEndpoint_ShouldAddEndpointToBothDictionaries()
    {
        // Arrange
        var options = new CosmosOptions();
        const string name = "MyEndpoint";
        var endpoint = new Uri("https://account.documents.azure.com:443/");
        var credential = A.Fake<TokenCredential>();

        // Act
        var result = options.AddOrUpdateEndpoint(name, endpoint, credential);

        // Assert
        result.Should().BeSameAs(options); // Fluent chaining
        options.NamedEndpoints.Should().ContainKey(name);
        options.NamedEndpoints[name].Endpoint.Should().Be(endpoint);
        options.NamedEndpoints[name].Credential.Should().Be(credential);
        options.AzureConnections.GetEndpoint(name)?.Endpoint.Should().Be(endpoint);
    }

    [Fact]
    public void Validate_WithNoConnections_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var options = new CosmosOptions();

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => options.Validate());
        exception.Message.Should().Contain("At least one connection must be configured");
    }

    [Fact]
    public void Validate_WithDefaultConnectionString_ShouldNotThrow()
    {
        // Arrange
        var options = new CosmosOptions
        {
            DefaultConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
        };

        // Act & Assert
        var exception = Record.Exception(() => options.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithDefaultMongoConnectionString_ShouldNotThrow()
    {
        // Arrange
        var options = new CosmosOptions
        {
            DefaultMongoConnectionString = "mongodb://account:password@account.mongo.cosmos.azure.com:10255/",
        };

        // Act & Assert
        var exception = Record.Exception(() => options.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithDefaultCassandraConnection_ShouldNotThrow()
    {
        // Arrange
        var options = new CosmosOptions
        {
            DefaultCassandraConnection = new CassandraConnectionOptions
            {
                ContactPoint = "account.cassandra.cosmos.azure.com",
            },
        };

        // Act & Assert
        var exception = Record.Exception(() => options.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithDefaultEndpoint_ShouldNotThrow()
    {
        // Arrange
        var options = new CosmosOptions
        {
            DefaultEndpoint = new Uri("https://account.documents.azure.com:443/"),
        };

        // Act & Assert
        var exception = Record.Exception(() => options.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithNamedConnection_ShouldNotThrow()
    {
        // Arrange
        var options = new CosmosOptions();
        options.AddOrUpdateConnection("MyConnection", "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;");

        // Act & Assert
        var exception = Record.Exception(() => options.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithNamedMongoConnection_ShouldNotThrow()
    {
        // Arrange
        var options = new CosmosOptions();
        options.AddOrUpdateMongoConnection("MyMongoConnection", "mongodb://account:password@account.mongo.cosmos.azure.com:10255/");

        // Act & Assert
        var exception = Record.Exception(() => options.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithNamedCassandraConnection_ShouldNotThrow()
    {
        // Arrange
        var options = new CosmosOptions();

        options.AddOrUpdateCassandraConnection("MyCassandraConnection", new CassandraConnectionOptions
        {
            ContactPoint = "account.cassandra.cosmos.azure.com",
        });

        // Act & Assert
        var exception = Record.Exception(() => options.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithNamedEndpoint_ShouldNotThrow()
    {
        // Arrange
        var options = new CosmosOptions();
        var credential = A.Fake<TokenCredential>();
        options.AddOrUpdateEndpoint("MyEndpoint", new Uri("https://account.documents.azure.com:443/"), credential);

        // Act & Assert
        var exception = Record.Exception(() => options.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void NamedConnections_ShouldBeCaseInsensitive()
    {
        // Arrange
        var options = new CosmosOptions();
        options.AddOrUpdateConnection("MyConnection", "connectionString");

        // Act & Assert
        options.NamedConnections.ContainsKey("myconnection").Should().BeTrue();
        options.NamedConnections.ContainsKey("MYCONNECTION").Should().BeTrue();
        options.NamedConnections.ContainsKey("MyConnection").Should().BeTrue();
    }

    [Fact]
    public void NamedEndpoints_ShouldBeCaseInsensitive()
    {
        // Arrange
        var options = new CosmosOptions();
        var credential = A.Fake<TokenCredential>();
        options.AddOrUpdateEndpoint("MyEndpoint", new Uri("https://account.documents.azure.com:443/"), credential);

        // Act & Assert
        options.NamedEndpoints.ContainsKey("myendpoint").Should().BeTrue();
        options.NamedEndpoints.ContainsKey("MYENDPOINT").Should().BeTrue();
        options.NamedEndpoints.ContainsKey("MyEndpoint").Should().BeTrue();
    }
}

public sealed class CosmosEndpointOptionsTests
{
    [Fact]
    public void DefaultConstructor_ShouldHaveNullProperties()
    {
        // Arrange & Act
        var options = new CosmosEndpointOptions();

        // Assert
        options.Endpoint.Should().BeNull();
        options.Credential.Should().BeNull();
    }

    [Fact]
    public void Endpoint_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CosmosEndpointOptions();
        var endpoint = new Uri("https://account.documents.azure.com:443/");

        // Act
        options.Endpoint = endpoint;

        // Assert
        options.Endpoint.Should().Be(endpoint);
    }

    [Fact]
    public void Credential_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CosmosEndpointOptions();
        var credential = A.Fake<TokenCredential>();

        // Act
        options.Credential = credential;

        // Assert
        options.Credential.Should().Be(credential);
    }
}

public sealed class CassandraConnectionOptionsTests
{
    [Fact]
    public void DefaultConstructor_ShouldHaveCorrectDefaults()
    {
        // Arrange & Act
        var options = new CassandraConnectionOptions();

        // Assert
        options.ContactPoint.Should().BeEmpty();
        options.Port.Should().Be(10350);
        options.Keyspace.Should().BeEmpty();
        options.Username.Should().BeNull();
        options.Password.Should().BeNull();
    }

    [Fact]
    public void ContactPoint_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CassandraConnectionOptions();
        const string contactPoint = "account.cassandra.cosmos.azure.com";

        // Act
        options.ContactPoint = contactPoint;

        // Assert
        options.ContactPoint.Should().Be(contactPoint);
    }

    [Fact]
    public void Port_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CassandraConnectionOptions();

        // Act
        options.Port = 9042;

        // Assert
        options.Port.Should().Be(9042);
    }

    [Fact]
    public void Keyspace_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CassandraConnectionOptions();
        const string keyspace = "mykeyspace";

        // Act
        options.Keyspace = keyspace;

        // Assert
        options.Keyspace.Should().Be(keyspace);
    }

    [Fact]
    public void Username_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CassandraConnectionOptions();
        const string username = "cassandraUser";

        // Act
        options.Username = username;

        // Assert
        options.Username.Should().Be(username);
    }

    [Fact]
    public void Password_ShouldSetAndGetCorrectly()
    {
        // Arrange
        var options = new CassandraConnectionOptions();
        const string password = "cassandraPassword";

        // Act
        options.Password = password;

        // Assert
        options.Password.Should().Be(password);
    }
}
