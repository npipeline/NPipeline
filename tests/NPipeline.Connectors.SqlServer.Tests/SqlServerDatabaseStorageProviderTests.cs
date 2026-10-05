using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Abstractions;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.SqlServer.Tests.Fixtures;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.SqlServer.Tests;

[Collection("SqlServer")]
public sealed class SqlServerDatabaseStorageProviderTests
{
    private readonly SqlServerTestContainerFixture _fixture;
    private readonly SqlServerDatabaseStorageProvider _provider;

    public SqlServerDatabaseStorageProviderTests(SqlServerTestContainerFixture fixture)
    {
        _provider = new SqlServerDatabaseStorageProvider();
        _fixture = fixture;
    }

    // Unit Tests - CanHandle

    [Fact]
    public void CanHandle_WithMssqlScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://localhost/mydb");

        // Act
        var result = _provider.Schemes.Contains(uri.Scheme);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanHandle_WithSqlServerScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("sqlserver://localhost/mydb");

        // Act
        var result = _provider.Schemes.Contains(uri.Scheme);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanHandle_WithOtherScheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb");

        // Act
        var result = _provider.Schemes.Contains(uri.Scheme);

        // Assert
        Assert.False(result);
    }

    // Unit Tests - GetConnectionString

    [Fact]
    public void GetConnectionString_GeneratesCorrectSqlConnectionString()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://testuser:testpass@localhost:1433/mydb");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new SqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost,1433", builder.DataSource);
        Assert.Equal("mydb", builder.InitialCatalog);
        Assert.Equal("testuser", builder.UserID);
        Assert.Equal("testpass", builder.Password);
    }

    [Fact]
    public void GetConnectionString_WithEncryptParameter_IncludesEncrypt()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://localhost/mydb?Encrypt=True");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new SqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.DataSource);
        Assert.Equal("mydb", builder.InitialCatalog);
        Assert.True(builder.Encrypt);
    }

    [Fact]
    public void GetConnectionString_WithTrustServerCertificateParameter_IncludesTrustServerCertificate()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://localhost/mydb?TrustServerCertificate=True");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new SqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.DataSource);
        Assert.Equal("mydb", builder.InitialCatalog);
        Assert.True(builder.TrustServerCertificate);
    }

    [Fact]
    public void GetConnectionString_WithUrlEncodedPassword_HandlesCorrectly()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://testuser:p%40ss%23w%24rd@localhost/mydb");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new SqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.DataSource);
        Assert.Equal("mydb", builder.InitialCatalog);
        Assert.Equal("testuser", builder.UserID);
        Assert.Equal("p@ss#w$rd", builder.Password);
    }

    [Fact]
    public void GetConnectionString_WithMissingHost_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql:///mydb");

        // Act & Assert
        Assert.Throws<ArgumentException>(() => _provider.GetConnectionString(uri));
    }

    [Fact]
    public void GetConnectionString_WithMissingDatabase_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://localhost/");

        // Act & Assert
        Assert.Throws<ArgumentException>(() => _provider.GetConnectionString(uri));
    }

    // Unit Tests - GetMetadata

    [Fact]
    public void GetMetadata_ReturnsCorrectProviderMetadata()
    {
        // Assert
        Assert.Equal("SQL Server", _provider.Name);
        Assert.Equal(StorageCapabilities.None, _provider.Capabilities);
        Assert.Contains(new StorageScheme("mssql"), _provider.Schemes);
        Assert.Contains(new StorageScheme("sqlserver"), _provider.Schemes);
    }

    // Unit Tests - Unsupported Operations

    [Fact]
    public async Task OpenReadAsync_ThrowsUnsupportedStorageCapability()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://localhost/mydb");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => _provider.OpenReadAsync(uri));
        Assert.Equal(_provider.Name, exception.ProviderName);
    }

    [Fact]
    public async Task OpenWriteAsync_ThrowsUnsupportedStorageCapability()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://localhost/mydb");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => _provider.OpenWriteAsync(uri));
        Assert.Equal(_provider.Name, exception.ProviderName);
    }

    [Fact]
    public async Task ExistsAsync_ThrowsUnsupportedStorageCapability()
    {
        // Arrange
        var uri = StorageUri.Parse("mssql://localhost/mydb");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => _provider.ExistsAsync(uri));
        Assert.Equal(_provider.Name, exception.ProviderName);
    }

    // Integration Tests

    [Fact]
    public async Task GetConnectionAsync_WithValidUri_ConnectsSuccessfully()
    {
        // Arrange - Parse the fixture's connection string using SqlConnectionStringBuilder
        var builder = new SqlConnectionStringBuilder(_fixture.ConnectionString);
        var dataSource = builder.DataSource;
        var database = builder.InitialCatalog;
        var username = builder.UserID;
        var password = builder.Password;

        // Parse host and port from DataSource (format: host,port)
        var hostPortParts = dataSource.Split(',');
        var host = hostPortParts[0];

        var port = hostPortParts.Length > 1
            ? int.Parse(hostPortParts[1])
            : 1433;

        // Build a URI using query parameters for credentials
        // URL-encode the password to handle special characters
        var encodedPassword = Uri.EscapeDataString(password);

        var uri = StorageUri.Parse(
            $"mssql://{host}:{port}/{database}?username={username}&password={encodedPassword}" +
            "&encrypt=true&trustservercertificate=true");

        // Act
        var connection = await _provider.GetConnectionAsync(uri);

        // Assert
        Assert.NotNull(connection);
        Assert.True(connection.IsOpen);

        // Cleanup
        await connection.DisposeAsync();
    }
}
