using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.Tests;

public class RedshiftStorageResolverFactoryTests
{
    [Fact]
    public void CreateResolver_ShouldReturnResolverWithRedshiftProvider()
    {
        // Act
        var resolver = RedshiftStorageResolverFactory.CreateResolver();

        // Assert
        resolver.Should().NotBeNull();
        resolver.GetAvailableProviders().Should().ContainSingle(p => p is RedshiftDatabaseStorageProvider);
    }

    [Fact]
    public void CreateResolver_ShouldResolveRedshiftUri()
    {
        // Arrange
        var resolver = RedshiftStorageResolverFactory.CreateResolver();
        var uri = StorageUri.Parse("redshift://cluster.example.com/mydb");

        // Act
        var provider = resolver.ResolveProvider(uri);

        // Assert
        provider.Should().NotBeNull();
        provider.Should().BeOfType<RedshiftDatabaseStorageProvider>();
    }

    [Fact]
    public void CreateResolver_ShouldNotResolveNonRedshiftUri()
    {
        // Arrange
        var resolver = RedshiftStorageResolverFactory.CreateResolver();
        var uri = StorageUri.Parse("postgres://cluster.example.com/mydb");

        // Act
        var provider = resolver.ResolveProvider(uri);

        // Assert
        provider.Should().BeNull();
    }

    [Fact]
    public void AddRedshiftProvider_ShouldRegisterProviderWithExistingResolver()
    {
        // Arrange
        var resolver = new StorageResolver();

        // Act
        var result = resolver.AddRedshiftProvider();

        // Assert
        result.Should().BeSameAs(resolver);
        resolver.GetAvailableProviders().Should().ContainSingle(p => p is RedshiftDatabaseStorageProvider);
    }

    [Fact]
    public void AddRedshiftProvider_WhenNullResolver_ShouldThrowArgumentNullException()
    {
        // Arrange
        IStorageResolver resolver = null!;

        // Act
        var act = () => resolver.AddRedshiftProvider();

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddRedshiftProvider_ShouldEnableRedshiftUriResolution()
    {
        // Arrange
        var resolver = new StorageResolver();
        resolver.AddRedshiftProvider();
        var uri = StorageUri.Parse("redshift://cluster.example.com/mydb");

        // Act
        var provider = resolver.ResolveProvider(uri);

        // Assert
        provider.Should().NotBeNull();
        provider.Should().BeOfType<RedshiftDatabaseStorageProvider>();
    }

    [Fact]
    public void CreateResolver_CanBeCalledMultipleTimes()
    {
        // Act
        var resolver1 = RedshiftStorageResolverFactory.CreateResolver();
        var resolver2 = RedshiftStorageResolverFactory.CreateResolver();

        // Assert
        resolver1.Should().NotBeSameAs(resolver2);
        resolver1.GetAvailableProviders().Should().HaveCount(1);
        resolver2.GetAvailableProviders().Should().HaveCount(1);
    }
}
