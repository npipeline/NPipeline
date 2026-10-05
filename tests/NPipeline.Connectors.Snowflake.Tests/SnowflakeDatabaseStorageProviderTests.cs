using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Abstractions;
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
        var result = provider.Schemes.Contains(uri.Scheme);

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
        var result = provider.Schemes.Contains(uri.Scheme);

        // Assert
        result.Should().BeFalse();
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
    public async Task OpenReadAsync_ThrowsUnsupportedStorageCapability()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();
        var uri = StorageUri.Parse("snowflake://myaccount/mydb");

        // Act & Assert
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenWriteAsync_ThrowsUnsupportedStorageCapability()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();
        var uri = StorageUri.Parse("snowflake://myaccount/mydb");

        // Act & Assert
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.OpenWriteAsync(uri));
    }

    [Fact]
    public async Task ExistsAsync_ThrowsUnsupportedStorageCapability()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();
        var uri = StorageUri.Parse("snowflake://myaccount/mydb");

        // Act & Assert
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.ExistsAsync(uri));
    }

    [Fact]
    public void Provider_DeclaresNameSchemesAndNoFileCapabilities()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();

        // Assert
        Assert.Equal("Snowflake", provider.Name);
        Assert.Equal(StorageCapabilities.None, provider.Capabilities);
        Assert.Contains(new StorageScheme("snowflake"), provider.Schemes);
    }

    [Fact]
    public async Task GetConnectionAsync_WithNullUri_ShouldThrow()
    {
        // Arrange
        var provider = new SnowflakeDatabaseStorageProvider();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => provider.GetConnectionAsync(null!));
    }

    [Fact]
    public void GetConnectionString_PasswordWithSemicolon_IsEscaped()
    {
        // A raw "password=p;host=evil" would let the password set the host.
        var provider = new SnowflakeDatabaseStorageProvider();
        var password = "p;host=evil\"x";
        var uri = StorageUri.Parse($"snowflake://alice:{Uri.EscapeDataString(password)}@myaccount/mydb");

        var connectionString = provider.GetConnectionString(uri);

        var parsed = new System.Data.Common.DbConnectionStringBuilder { ConnectionString = connectionString };
        parsed["password"].Should().Be(password);
        parsed.ContainsKey("host").Should().BeFalse();
        parsed["account"].Should().Be("myaccount");
        parsed["db"].Should().Be("mydb");
    }
}

