using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;
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
    public void AddAdlsGen2StorageProvider_RegistersIDeletableStorageProvider()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAdlsGen2StorageProvider();

        // Assert
        var provider = services.BuildServiceProvider();
        var deletableProvider = provider.GetRequiredService<IDeletableStorageProvider>();
        deletableProvider.Should().BeOfType<AdlsGen2StorageProvider>();
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_RegistersIMoveableStorageProvider()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAdlsGen2StorageProvider();

        // Assert
        var provider = services.BuildServiceProvider();
        var moveableProvider = provider.GetRequiredService<IMoveableStorageProvider>();
        moveableProvider.Should().BeOfType<AdlsGen2StorageProvider>();
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_RegistersIStorageProviderMetadataProvider()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAdlsGen2StorageProvider();

        // Assert
        var provider = services.BuildServiceProvider();
        var metadataProvider = provider.GetRequiredService<IStorageProviderMetadataProvider>();
        metadataProvider.Should().BeOfType<AdlsGen2StorageProvider>();
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_WithConfiguration_AppliesOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        var customThreshold = 128 * 1024 * 1024; // 128 MB

        // Act
        services.AddAdlsGen2StorageProvider(options => { options.UploadThresholdBytes = customThreshold; });

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<AdlsGen2StorageProviderOptions>();
        options.UploadThresholdBytes.Should().Be(customThreshold);
    }

    [Fact]
    public void AddAdlsGen2StorageProvider_WithPreBuiltOptions_UsesOptions()
    {
        // Arrange
        var services = new ServiceCollection();
        var customThreshold = 256 * 1024 * 1024; // 256 MB

        // Act
        services.AddAdlsGen2StorageProvider(options => options.UploadThresholdBytes = customThreshold);

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<AdlsGen2StorageProviderOptions>();
        options.UploadThresholdBytes.Should().Be(customThreshold);
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
