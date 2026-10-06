using Npgsql;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Postgres.Tests.Fixtures;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Postgres.Tests;

[Collection("PostgresTestCollection")]
public sealed class PostgresDatabaseConnectionProviderTests
{
    private readonly PostgresTestContainerFixture _fixture;
    private readonly PostgresDatabaseConnectionProvider _provider;

    public PostgresDatabaseConnectionProviderTests(PostgresTestContainerFixture fixture)
    {
        _provider = new PostgresDatabaseConnectionProvider();
        _fixture = fixture;
    }

    // Unit Tests - CanHandle

    [Fact]
    public void CanHandle_WithPostgresScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb");

        // Act
        var result = _provider.Schemes.Contains(uri.Scheme);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanHandle_WithPostgresqlScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("postgresql://localhost/mydb");

        // Act
        var result = _provider.Schemes.Contains(uri.Scheme);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanHandle_WithOtherScheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("sqlserver://localhost/mydb");

        // Act
        var result = _provider.Schemes.Contains(uri.Scheme);

        // Assert
        Assert.False(result);
    }

    // Unit Tests - GetConnectionString

    [Fact]
    public void GetConnectionString_GeneratesCorrectNpgsqlConnectionString()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost:5432/mydb?username=testuser&password=testpass");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new NpgsqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.Host);
        Assert.Equal(5432, builder.Port);
        Assert.Equal("mydb", builder.Database);
        Assert.Equal("testuser", builder.Username);
        Assert.Equal("testpass", builder.Password);
    }

    [Fact]
    public void GetConnectionString_WithSslModeParameter_IncludesSslMode()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb?sslmode=require");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new NpgsqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.Host);
        Assert.Equal("mydb", builder.Database);
        Assert.Equal(SslMode.Require, builder.SslMode);
    }

    [Fact]
    public void GetConnectionString_WithTimeoutParameter_IncludesTimeout()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb?Timeout=30");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new NpgsqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.Host);
        Assert.Equal("mydb", builder.Database);
        Assert.Equal(30, builder.Timeout);
    }

    [Fact]
    public void GetConnectionString_WithUrlEncodedPassword_HandlesCorrectly()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://testuser:p%40ss%23w%24rd@localhost/mydb");

        // Act
        var connectionString = _provider.GetConnectionString(uri);
        var builder = new NpgsqlConnectionStringBuilder(connectionString);

        // Assert
        Assert.Equal("localhost", builder.Host);
        Assert.Equal("mydb", builder.Database);
        Assert.Equal("testuser", builder.Username);
        Assert.Equal("p@ss#w$rd", builder.Password);
    }

    [Fact]
    public void GetConnectionString_WithMissingHost_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres:///mydb");

        // Act & Assert
        Assert.Throws<ArgumentException>(() => _provider.GetConnectionString(uri));
    }

    [Fact]
    public void GetConnectionString_WithMissingDatabase_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/");

        // Act & Assert
        Assert.Throws<ArgumentException>(() => _provider.GetConnectionString(uri));
    }

    // Integration Tests

    [Fact]
    public async Task OpenConnectionAsync_WithValidUri_ConnectsSuccessfully()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var encodedPassword = Uri.EscapeDataString(builder.Password ?? string.Empty);
        var uri = StorageUri.Parse($"postgres://{builder.Username}:{encodedPassword}@{builder.Host}:{builder.Port}/{builder.Database}");

        // Act
        var connection = await _provider.OpenConnectionAsync(uri);

        // Assert
        Assert.NotNull(connection);
        Assert.True(connection.IsOpen);

        // Cleanup
        await connection.DisposeAsync();
    }

    [Fact]
    public async Task OpenConnectionAsync_WithInvalidUri_ThrowsDatabaseConnectionException()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://invalidhost:9999/invaliddb?username=invaliduser&password=invalidpass");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<DatabaseConnectionException>(() => _provider.OpenConnectionAsync(uri));
        Assert.Contains("Failed to establish PostgreSQL connection", exception.Message);
    }
}
