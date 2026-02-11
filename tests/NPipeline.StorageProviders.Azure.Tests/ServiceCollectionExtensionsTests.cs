using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddAzureBlobStorageProvider_WithConfigurationAction_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
            options.BlockBlobUploadThresholdBytes = 128 * 1024 * 1024;
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var factory = provider.GetService<AzureBlobClientFactory>();
        factory.Should().NotBeNull();
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithConfigurationAction_RegistersFactoryAsSingleton()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var factory1 = provider.GetRequiredService<AzureBlobClientFactory>();
        var factory2 = provider.GetRequiredService<AzureBlobClientFactory>();
        factory1.Should().BeSameAs(factory2);
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithConfigurationAction_RegistersProviderAsSingleton()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var storageProvider1 = provider.GetRequiredService<AzureBlobStorageProvider>();
        var storageProvider2 = provider.GetRequiredService<AzureBlobStorageProvider>();
        storageProvider1.Should().BeSameAs(storageProvider2);
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithConfigurationAction_RegistersOptionsAsSingleton()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
            options.BlockBlobUploadThresholdBytes = 256 * 1024 * 1024;
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var options1 = provider.GetRequiredService<AzureBlobStorageProviderOptions>();
        var options2 = provider.GetRequiredService<AzureBlobStorageProviderOptions>();
        options1.Should().BeSameAs(options2);
        options1.BlockBlobUploadThresholdBytes.Should().Be(256 * 1024 * 1024);
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithOptionsObject_RegistersServices()
    {
        // Arrange
        var services = new ServiceCollection();

        var options = new AzureBlobStorageProviderOptions
        {
            DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
            BlockBlobUploadThresholdBytes = 128 * 1024 * 1024,
        };

        // Act
        services.AddAzureBlobStorageProvider(options);

        // Assert
        var provider = services.BuildServiceProvider();
        var factory = provider.GetService<AzureBlobClientFactory>();
        factory.Should().NotBeNull();
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithOptionsObject_RegistersFactoryAsSingleton()
    {
        // Arrange
        var services = new ServiceCollection();

        var options = new AzureBlobStorageProviderOptions
        {
            DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
        };

        // Act
        services.AddAzureBlobStorageProvider(options);

        // Assert
        var provider = services.BuildServiceProvider();
        var factory1 = provider.GetRequiredService<AzureBlobClientFactory>();
        var factory2 = provider.GetRequiredService<AzureBlobClientFactory>();
        factory1.Should().BeSameAs(factory2);
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithOptionsObject_RegistersProviderAsSingleton()
    {
        // Arrange
        var services = new ServiceCollection();

        var options = new AzureBlobStorageProviderOptions
        {
            DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
        };

        // Act
        services.AddAzureBlobStorageProvider(options);

        // Assert
        var provider = services.BuildServiceProvider();
        var storageProvider1 = provider.GetRequiredService<AzureBlobStorageProvider>();
        var storageProvider2 = provider.GetRequiredService<AzureBlobStorageProvider>();
        storageProvider1.Should().BeSameAs(storageProvider2);
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithOptionsObject_RegistersOptionsAsSingleton()
    {
        // Arrange
        var services = new ServiceCollection();

        var options = new AzureBlobStorageProviderOptions
        {
            DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net",
            BlockBlobUploadThresholdBytes = 256 * 1024 * 1024,
        };

        // Act
        services.AddAzureBlobStorageProvider(options);

        // Assert
        var provider = services.BuildServiceProvider();
        var options1 = provider.GetRequiredService<AzureBlobStorageProviderOptions>();
        var options2 = provider.GetRequiredService<AzureBlobStorageProviderOptions>();
        options1.Should().BeSameAs(options2);
        options1.BlockBlobUploadThresholdBytes.Should().Be(256 * 1024 * 1024);
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithConfigurationAction_SetsConnectionString()
    {
        // Arrange
        var services = new ServiceCollection();
        var connectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";

        // Act
        services.AddAzureBlobStorageProvider(options => { options.DefaultConnectionString = connectionString; });

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<AzureBlobStorageProviderOptions>();
        options.DefaultConnectionString.Should().Be(connectionString);
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithConfigurationAction_SetsServiceUrl()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options => { options.ServiceUrl = new Uri("https://testaccount.blob.core.windows.net"); });

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<AzureBlobStorageProviderOptions>();
        options.ServiceUrl.Should().Be(new Uri("https://testaccount.blob.core.windows.net"));
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithConfigurationAction_SetsUploadOptions()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.BlockBlobUploadThresholdBytes = 64 * 1024 * 1024;
            options.UploadMaximumConcurrency = 4;
            options.UploadMaximumTransferSizeBytes = 4 * 1024 * 1024;
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<AzureBlobStorageProviderOptions>();
        options.BlockBlobUploadThresholdBytes.Should().Be(64 * 1024 * 1024);
        options.UploadMaximumConcurrency.Should().Be(4);
        options.UploadMaximumTransferSizeBytes.Should().Be(4 * 1024 * 1024);
    }

    [Fact]
    public void AddAzureBlobStorageProvider_WithConfigurationAction_SetsUseDefaultCredentialChain()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options => { options.UseDefaultCredentialChain = false; });

        // Assert
        var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<AzureBlobStorageProviderOptions>();
        options.UseDefaultCredentialChain.Should().BeFalse();
    }

    [Fact]
    public void AddAzureBlobStorageProvider_ProviderCanBeResolved()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var storageProvider = provider.GetRequiredService<AzureBlobStorageProvider>();
        storageProvider.Should().NotBeNull();
    }

    [Fact]
    public void AddAzureBlobStorageProvider_ProviderImplementsIStorageProvider()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var storageProvider = provider.GetRequiredService<IStorageProvider>();
        storageProvider.Should().NotBeNull();
        storageProvider.Should().BeOfType<AzureBlobStorageProvider>();
    }

    [Fact]
    public void AddAzureBlobStorageProvider_ProviderImplementsIStorageProviderMetadataProvider()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var metadataProvider = provider.GetRequiredService<IStorageProviderMetadataProvider>();
        metadataProvider.Should().NotBeNull();
        metadataProvider.Should().BeOfType<AzureBlobStorageProvider>();
    }

    [Fact]
    public void AddAzureBlobStorageProvider_MultipleCalls_IndependentServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test1;AccountKey=test1;EndpointSuffix=core.windows.net";
        });

        services.AddAzureBlobStorageProvider(options =>
        {
            options.DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test2;AccountKey=test2;EndpointSuffix=core.windows.net";
        });

        // Assert
        var provider = services.BuildServiceProvider();
        var factory1 = provider.GetRequiredService<AzureBlobClientFactory>();
        var factory2 = provider.GetRequiredService<AzureBlobClientFactory>();
        factory1.Should().BeSameAs(factory2); // Factory is singleton
    }
}
