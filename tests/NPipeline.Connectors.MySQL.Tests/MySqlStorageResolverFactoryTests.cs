using AwesomeAssertions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.Tests;

public sealed class MySqlStorageResolverFactoryTests
{
    [Fact]
    public void CreateResolver_ReturnsNonNullResolver()
    {
        // Act
        var resolver = MySqlStorageResolverFactory.CreateResolver();

        // Assert
        _ = resolver.Should().NotBeNull();
    }

    [Fact]
    public void CreateResolver_RegisteredResolver_CanHandleMySqlUri()
    {
        // Arrange
        var resolver = MySqlStorageResolverFactory.CreateResolver();
        var uri = StorageUri.Parse("mysql://localhost/testdb");

        // Act
        var provider = resolver.ResolveProvider(uri);

        // Assert
        _ = provider.Should().NotBeNull();
        _ = provider.Should().BeOfType<MySqlDatabaseStorageProvider>();
    }

    [Fact]
    public void CreateResolver_RegisteredResolver_CanHandleMariaDbUri()
    {
        // Arrange
        var resolver = MySqlStorageResolverFactory.CreateResolver();
        var uri = StorageUri.Parse("mariadb://localhost/testdb");

        // Act
        var provider = resolver.ResolveProvider(uri);

        // Assert
        _ = provider.Should().NotBeNull();
        _ = provider.Should().BeOfType<MySqlDatabaseStorageProvider>();
    }
}
