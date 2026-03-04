using MySqlConnector;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.Tests;

public sealed class MySqlDatabaseStorageProviderTests
{
    private readonly MySqlDatabaseStorageProvider _provider;

    public MySqlDatabaseStorageProviderTests()
    {
        _provider = new MySqlDatabaseStorageProvider();
    }

    // Unit Tests - CanHandle

    [Fact]
    public void CanHandle_WithMysqlScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("mysql://localhost/mydb");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanHandle_WithMariaDbScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("mariadb://localhost/mydb");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanHandle_WithOtherScheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void CanHandle_WithSqlServerScheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("sqlserver://localhost/mydb");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        Assert.False(result);
    }

    // Unit Tests - GetConnectionString

    [Fact]
    public void GetConnectionString_GeneratesCorrectConnectionString()
    {
        // Arrange
        var uri = StorageUri.Parse("mysql://testuser:testpass@localhost:3306/mydb");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new MySqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.Server);
        Assert.Equal("mydb", builder.Database);
        Assert.Equal("testuser", builder.UserID);
        Assert.Equal("testpass", builder.Password);
        Assert.Equal(3306u, builder.Port);
    }

    [Fact]
    public void GetConnectionString_WithMariaDbScheme_GeneratesConnectionString()
    {
        // Arrange
        var uri = StorageUri.Parse("mariadb://testuser:pass@localhost:3306/mydb");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new MySqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.Server);
        Assert.Equal("mydb", builder.Database);
    }

    [Fact]
    public void GetConnectionString_WithNoPort_UsesDefaultPort()
    {
        // Arrange
        var uri = StorageUri.Parse("mysql://localhost/mydb");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new MySqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.Server);
        Assert.Equal("mydb", builder.Database);
    }

    [Fact]
    public void GetConnectionString_WithUrlEncodedPassword_HandlesCorrectly()
    {
        // Arrange
        var uri = StorageUri.Parse("mysql://testuser:p%40ss%23w%24rd@localhost/mydb");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new MySqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.Server);
        Assert.Equal("mydb", builder.Database);
        Assert.Equal("testuser", builder.UserID);
        Assert.Equal("p@ss#w$rd", builder.Password);
    }

    [Fact]
    public void GetConnectionString_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => _provider.GetConnectionString(null!));
    }

    // Unit Tests - Scheme

    [Fact]
    public void Scheme_ReturnsMySqlScheme()
    {
        // Act
        var scheme = _provider.Scheme;

        // Assert
        Assert.Equal(StorageScheme.MySql, scheme);
    }
}
