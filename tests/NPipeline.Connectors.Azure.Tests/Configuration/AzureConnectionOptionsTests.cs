using AwesomeAssertions;
using Azure.Core;
using FakeItEasy;
using NPipeline.Connectors.Azure.Configuration;

namespace NPipeline.Connectors.Azure.Tests.Configuration;

public class AzureConnectionOptionsTests
{
    #region Default Values

    [Fact]
    public void DefaultConnectionString_DefaultShouldBeNull()
    {
        // Arrange
        var options = new AzureConnectionOptions();

        // Act & Assert
        options.DefaultConnectionString.Should().BeNull();
    }

    [Fact]
    public void DefaultEndpoint_DefaultShouldBeNull()
    {
        // Arrange
        var options = new AzureConnectionOptions();

        // Act & Assert
        options.DefaultEndpoint.Should().BeNull();
    }

    #endregion

    #region Connection String Tests

    [Fact]
    public void GetConnectionString_WhenNotExists_ShouldReturnNull()
    {
        // Arrange
        var options = new AzureConnectionOptions();

        // Act
        var result = options.GetConnectionString("nonexistent");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void AddOrUpdateConnection_ShouldStoreConnection()
    {
        // Arrange
        var options = new AzureConnectionOptions();
        const string name = "myConnection";
        const string connectionString = "AccountEndpoint=https://myaccount.documents.azure.com:443/;AccountKey=mykey;";

        // Act
        options.AddOrUpdateConnection(name, connectionString);

        // Assert
        var result = options.GetConnectionString(name);
        result.Should().Be(connectionString);
    }

    [Fact]
    public void AddOrUpdateConnection_WithSameName_ShouldUpdate()
    {
        // Arrange
        var options = new AzureConnectionOptions();
        const string name = "myConnection";
        const string originalConnectionString = "OriginalConnectionString";
        const string updatedConnectionString = "UpdatedConnectionString";

        // Act
        options.AddOrUpdateConnection(name, originalConnectionString);
        options.AddOrUpdateConnection(name, updatedConnectionString);

        // Assert
        var result = options.GetConnectionString(name);
        result.Should().Be(updatedConnectionString);
    }

    [Fact]
    public void GetConnectionString_ShouldBeCaseInsensitive()
    {
        // Arrange
        var options = new AzureConnectionOptions();
        const string connectionString = "MyConnectionString";
        options.AddOrUpdateConnection("MyConnection", connectionString);

        // Act
        var resultLower = options.GetConnectionString("myconnection");
        var resultUpper = options.GetConnectionString("MYCONNECTION");

        // Assert
        resultLower.Should().Be(connectionString);
        resultUpper.Should().Be(connectionString);
    }

    [Fact]
    public void GetAllConnections_ShouldReturnAllConnections()
    {
        // Arrange
        var options = new AzureConnectionOptions();
        options.AddOrUpdateConnection("conn1", "connectionString1");
        options.AddOrUpdateConnection("conn2", "connectionString2");

        // Act
        var allConnections = options.GetAllConnections();

        // Assert
        allConnections.Should().HaveCount(2);
        allConnections["conn1"].Should().Be("connectionString1");
        allConnections["conn2"].Should().Be("connectionString2");
    }

    [Fact]
    public void GetAllConnections_WhenEmpty_ShouldReturnEmptyDictionary()
    {
        // Arrange
        var options = new AzureConnectionOptions();

        // Act
        var allConnections = options.GetAllConnections();

        // Assert
        allConnections.Should().BeEmpty();
    }

    #endregion

    #region Endpoint Tests

    [Fact]
    public void GetEndpoint_WhenNotExists_ShouldReturnNull()
    {
        // Arrange
        var options = new AzureConnectionOptions();

        // Act
        var result = options.GetEndpoint("nonexistent");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void AddOrUpdateEndpoint_ShouldStoreEndpoint()
    {
        // Arrange
        var options = new AzureConnectionOptions();
        const string name = "myEndpoint";

        var endpoint = new AzureEndpointOptions
        {
            Endpoint = new Uri("https://myaccount.blob.core.windows.net/"),
            Credential = A.Fake<TokenCredential>(),
        };

        // Act
        options.AddOrUpdateEndpoint(name, endpoint);

        // Assert
        var result = options.GetEndpoint(name);
        result.Should().NotBeNull();
        result!.Endpoint.Should().Be(endpoint.Endpoint);
        result.Credential.Should().Be(endpoint.Credential);
    }

    [Fact]
    public void AddOrUpdateEndpoint_WithSameName_ShouldUpdate()
    {
        // Arrange
        var options = new AzureConnectionOptions();
        const string name = "myEndpoint";

        var originalEndpoint = new AzureEndpointOptions
        {
            Endpoint = new Uri("https://original.blob.core.windows.net/"),
        };

        var updatedEndpoint = new AzureEndpointOptions
        {
            Endpoint = new Uri("https://updated.blob.core.windows.net/"),
        };

        // Act
        options.AddOrUpdateEndpoint(name, originalEndpoint);
        options.AddOrUpdateEndpoint(name, updatedEndpoint);

        // Assert
        var result = options.GetEndpoint(name);
        result.Should().NotBeNull();
        result!.Endpoint.Should().Be(updatedEndpoint.Endpoint);
    }

    [Fact]
    public void GetEndpoint_ShouldBeCaseInsensitive()
    {
        // Arrange
        var options = new AzureConnectionOptions();

        var endpoint = new AzureEndpointOptions
        {
            Endpoint = new Uri("https://myaccount.blob.core.windows.net/"),
        };

        options.AddOrUpdateEndpoint("MyEndpoint", endpoint);

        // Act
        var resultLower = options.GetEndpoint("myendpoint");
        var resultUpper = options.GetEndpoint("MYENDPOINT");

        // Assert
        resultLower.Should().NotBeNull();
        resultUpper.Should().NotBeNull();
        resultLower!.Endpoint.Should().Be(endpoint.Endpoint);
        resultUpper!.Endpoint.Should().Be(endpoint.Endpoint);
    }

    [Fact]
    public void GetAllEndpoints_ShouldReturnAllEndpoints()
    {
        // Arrange
        var options = new AzureConnectionOptions();
        var endpoint1 = new AzureEndpointOptions { Endpoint = new Uri("https://endpoint1.blob.core.windows.net/") };
        var endpoint2 = new AzureEndpointOptions { Endpoint = new Uri("https://endpoint2.blob.core.windows.net/") };
        options.AddOrUpdateEndpoint("ep1", endpoint1);
        options.AddOrUpdateEndpoint("ep2", endpoint2);

        // Act
        var allEndpoints = options.GetAllEndpoints();

        // Assert
        allEndpoints.Should().HaveCount(2);
        allEndpoints["ep1"].Endpoint.Should().Be(endpoint1.Endpoint);
        allEndpoints["ep2"].Endpoint.Should().Be(endpoint2.Endpoint);
    }

    [Fact]
    public void GetAllEndpoints_WhenEmpty_ShouldReturnEmptyDictionary()
    {
        // Arrange
        var options = new AzureConnectionOptions();

        // Act
        var allEndpoints = options.GetAllEndpoints();

        // Assert
        allEndpoints.Should().BeEmpty();
    }

    #endregion

    #region Default Connection Tests

    [Fact]
    public void DefaultConnectionString_CanBeSet()
    {
        // Arrange
        var options = new AzureConnectionOptions();
        const string connectionString = "DefaultConnectionString";

        // Act
        options.DefaultConnectionString = connectionString;

        // Assert
        options.DefaultConnectionString.Should().Be(connectionString);
    }

    [Fact]
    public void DefaultEndpoint_CanBeSet()
    {
        // Arrange
        var options = new AzureConnectionOptions();

        var endpoint = new AzureEndpointOptions
        {
            Endpoint = new Uri("https://default.blob.core.windows.net/"),
        };

        // Act
        options.DefaultEndpoint = endpoint;

        // Assert
        options.DefaultEndpoint.Should().NotBeNull();
        options.DefaultEndpoint!.Endpoint.Should().Be(endpoint.Endpoint);
    }

    #endregion
}
