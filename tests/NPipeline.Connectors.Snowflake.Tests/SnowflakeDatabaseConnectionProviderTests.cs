using AwesomeAssertions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Snowflake.Tests;

public sealed class SnowflakeDatabaseConnectionProviderTests
{
    [Fact]
    public void CanHandle_WithSnowflakeScheme_ShouldReturnTrue()
    {
        // Arrange
        var provider = new SnowflakeDatabaseConnectionProvider();
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
        var provider = new SnowflakeDatabaseConnectionProvider();
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
        var provider = new SnowflakeDatabaseConnectionProvider();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => provider.GetConnectionString(null!));
    }

    [Fact]
    public void Provider_DeclaresSchemes()
    {
        // Arrange
        var provider = new SnowflakeDatabaseConnectionProvider();

        // Assert
        Assert.Contains(new StorageScheme("snowflake"), provider.Schemes);
    }

    [Fact]
    public async Task OpenConnectionAsync_WithNullUri_ShouldThrow()
    {
        // Arrange
        var provider = new SnowflakeDatabaseConnectionProvider();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => provider.OpenConnectionAsync(null!));
    }

    [Fact]
    public void GetConnectionString_PasswordWithSemicolon_IsEscaped()
    {
        // A raw "password=p;host=evil" would let the password set the host.
        var provider = new SnowflakeDatabaseConnectionProvider();
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
