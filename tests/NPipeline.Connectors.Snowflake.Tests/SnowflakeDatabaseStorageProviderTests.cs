using AwesomeAssertions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Snowflake.Tests;

public sealed class SnowflakeDatabaseStorageProviderTests
{
    [Fact]
    public void CanHandle_WithSnowflakeScheme_ShouldReturnTrue()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();
        var uri = StorageUri.Parse("snowflake://myaccount/mydb");

        // Act
        var result = provider.CanHandle(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void CanHandle_WithNonSnowflakeScheme_ShouldReturnFalse()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();
        var uri = StorageUri.Parse("mssql://localhost/mydb");

        // Act
        var result = provider.CanHandle(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WithNullUri_ShouldThrow()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => provider.CanHandle(null!));
    }

    [Fact]
    public void GetConnectionString_WithNullUri_ShouldThrow()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => provider.GetConnectionString(null!));
    }

    [Fact]
    public async Task OpenReadAsync_ShouldThrowNotSupportedException()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();
        var uri = StorageUri.Parse("snowflake://myaccount/mydb");

        // Act & Assert
        await Assert.ThrowsAsync<NotSupportedException>(() => provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenWriteAsync_ShouldThrowNotSupportedException()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();
        var uri = StorageUri.Parse("snowflake://myaccount/mydb");

        // Act & Assert
        await Assert.ThrowsAsync<NotSupportedException>(() => provider.OpenWriteAsync(uri));
    }

    [Fact]
    public async Task ExistsAsync_ShouldThrowNotSupportedException()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();
        var uri = StorageUri.Parse("snowflake://myaccount/mydb");

        // Act & Assert
        await Assert.ThrowsAsync<NotSupportedException>(() => provider.ExistsAsync(uri));
    }

    [Fact]
    public void GetMetadata_ShouldReturnValidMetadata()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();

        // Act
        var metadata = provider.GetMetadata();

        // Assert
        metadata.Should().NotBeNull();
        metadata.Name.Should().Be("Snowflake");
        metadata.SupportedSchemes.Should().Contain("snowflake");
        metadata.SupportsRead.Should().BeFalse();
        metadata.SupportsWrite.Should().BeFalse();
    }

    [Fact]
    public async Task GetConnectionAsync_WithNullUri_ShouldThrow()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => provider.GetConnectionAsync(null!));
    }
}
