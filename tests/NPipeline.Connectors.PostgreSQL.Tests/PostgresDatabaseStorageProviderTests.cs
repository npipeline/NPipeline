using Npgsql;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.PostgreSQL.Tests.Fixtures;

namespace NPipeline.Connectors.PostgreSQL.Tests;

[Collection("PostgresTestCollection")]
public sealed class PostgresDatabaseStorageProviderTests
{
    private readonly PostgresTestContainerFixture _fixture;
    private readonly PostgresDatabaseStorageProvider _provider;

    public PostgresDatabaseStorageProviderTests(PostgresTestContainerFixture fixture)
    {
        _provider = new PostgresDatabaseStorageProvider();
        _fixture = fixture;
    }

    // Unit Tests - CanHandle

    [Fact]
    public void CanHandle_WithPostgresScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanHandle_WithPostgresqlScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("postgresql://localhost/mydb");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void CanHandle_WithOtherScheme_ReturnsFalse()
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

    // Unit Tests - GetMetadata

    [Fact]
    public void GetMetadata_ReturnsCorrectProviderMetadata()
    {
        // Act
        var metadata = _provider.GetMetadata();

        // Assert
        Assert.Equal("PostgreSQL", metadata.Name);
        Assert.Contains("postgres", metadata.SupportedSchemes);
        Assert.Contains("postgresql", metadata.SupportedSchemes);
        Assert.False(metadata.SupportsRead);
        Assert.False(metadata.SupportsWrite);
        Assert.False(metadata.SupportsDelete);
        Assert.False(metadata.SupportsListing);
        Assert.False(metadata.SupportsMetadata);
        Assert.False(metadata.SupportsHierarchy);
    }

    // Unit Tests - Unsupported Operations

    [Fact]
    public async Task OpenReadAsync_ThrowsNotSupportedException()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NotSupportedException>(() => _provider.OpenReadAsync(uri));
        Assert.Contains("OpenReadAsync is not supported", exception.Message);
        Assert.Contains(nameof(PostgresDatabaseStorageProvider), exception.Message);
    }

    [Fact]
    public async Task OpenWriteAsync_ThrowsNotSupportedException()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NotSupportedException>(() => _provider.OpenWriteAsync(uri));
        Assert.Contains("OpenWriteAsync is not supported", exception.Message);
        Assert.Contains(nameof(PostgresDatabaseStorageProvider), exception.Message);
    }

    [Fact]
    public async Task ExistsAsync_ThrowsNotSupportedException()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://localhost/mydb");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NotSupportedException>(() => _provider.ExistsAsync(uri));
        Assert.Contains("ExistsAsync is not supported", exception.Message);
        Assert.Contains(nameof(PostgresDatabaseStorageProvider), exception.Message);
    }

    // Integration Tests

    [Fact]
    public async Task GetConnectionAsync_WithValidUri_ConnectsSuccessfully()
    {
        // Arrange
        var connectionString = _fixture.ConnectionString;
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        var encodedPassword = Uri.EscapeDataString(builder.Password ?? string.Empty);
        var uri = StorageUri.Parse($"postgres://{builder.Username}:{encodedPassword}@{builder.Host}:{builder.Port}/{builder.Database}");

        // Act
        var connection = await _provider.GetConnectionAsync(uri);

        // Assert
        Assert.NotNull(connection);
        Assert.True(connection.IsOpen);

        // Cleanup
        await connection.DisposeAsync();
    }

    [Fact]
    public async Task GetConnectionAsync_WithInvalidUri_ThrowsDatabaseConnectionException()
    {
        // Arrange
        var uri = StorageUri.Parse("postgres://invalidhost:9999/invaliddb?username=invaliduser&password=invalidpass");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<DatabaseConnectionException>(() => _provider.GetConnectionAsync(uri));
        Assert.Contains("Failed to establish PostgreSQL connection", exception.Message);
    }
}
