using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.DependencyInjection;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddAdlsGen2StorageProvider_RegistersAllServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAdlsGen2StorageProvider();

        // Assert
        var provider = services.BuildServiceProvider();
        var storageProvider = provider.GetRequiredService<AdlsGen2StorageProvider>();
        storageProvider.Should().NotBeNull();
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_RegistersIStorageProvider()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAdlsGen2StorageProvider();

        // Assert
        var provider = services.BuildServiceProvider();
        var storageProvider = provider.GetRequiredService<IStorageProvider>();
        storageProvider.Should().BeOfType<AdlsGen2StorageProvider>();
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_Twice_RegistersOneProvider()
    {
        var services = new ServiceCollection();
        services.AddAdlsGen2StorageProvider();
        services.AddAdlsGen2StorageProvider(new AdlsGen2StorageProviderOptions());

        var provider = services.BuildServiceProvider();

        provider.GetServices<IStorageProvider>().OfType<AdlsGen2StorageProvider>().Should().ContainSingle();
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_ProviderIsResolvableThroughIStorageResolver()
    {
        var services = new ServiceCollection();
        services.AddStorageResolver(false);
        services.AddAdlsGen2StorageProvider();

        var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IStorageResolver>().Resolve(StorageUri.Parse("adls://filesystem/path"))
            .Should().BeOfType<AdlsGen2StorageProvider>();
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_WithConfiguration_AppliesOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        var customThreshold = 128 * 1024 * 1024; // 128 MB

        // Act
        services.AddAdlsGen2StorageProvider(options => { options.PartSizeBytes = customThreshold; });

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<AdlsGen2StorageProviderOptions>();
        options.PartSizeBytes.Should().Be(customThreshold);
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_WithPreBuiltOptions_UsesOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        var customThreshold = 256 * 1024 * 1024; // 256 MB

        // Act
        services.AddAdlsGen2StorageProvider(new AdlsGen2StorageProviderOptions { PartSizeBytes = customThreshold });

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<AdlsGen2StorageProviderOptions>();
        options.PartSizeBytes.Should().Be(customThreshold);
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_ReturnsServiceCollection()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var result = services.AddAdlsGen2StorageProvider();

        // Assert
        result.Should().BeSameAs(services);
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_SingletonLifetime()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddAdlsGen2StorageProvider();
        var provider = services.BuildServiceProvider();

        // Act
        var instance1 = provider.GetRequiredService<AdlsGen2StorageProvider>();
        var instance2 = provider.GetRequiredService<AdlsGen2StorageProvider>();

        // Assert
        instance1.Should().BeSameAs(instance2);
    }
}
