using Microsoft.Extensions.DependencyInjection;

namespace NPipeline.StorageProviders.Sftp.Tests;

/// <summary>
///     Unit tests for <see cref="ServiceCollectionExtensions" />.
/// </summary>
public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddSftpStorageProvider_WithDefaultOptions_ShouldRegisterServices()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddSftpStorageProvider();

        // Assert
        var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetService<SftpStorageProviderOptions>();
        var factory = serviceProvider.GetService<SftpClientFactory>();
        var provider = serviceProvider.GetService<SftpStorageProvider>();

        options.Should().NotBeNull();
        factory.Should().NotBeNull();
        provider.Should().NotBeNull();
    }

    [Fact]
    public void AddSftpStorageProvider_WithConfiguration_ShouldConfigureOptions()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddSftpStorageProvider(options =>
        {
            options.DefaultHost = "sftp.example.com";
            options.DefaultUsername = "testuser";
            options.DefaultPassword = "testpass";
            options.MaxPoolSize = 20;
        });

        // Assert
        var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<SftpStorageProviderOptions>();

        options.DefaultHost.Should().Be("sftp.example.com");
        options.DefaultUsername.Should().Be("testuser");
        options.DefaultPassword.Should().Be("testpass");
        options.MaxPoolSize.Should().Be(20);
    }

    [Fact]
    public void AddSftpStorageProvider_WithPreconfiguredOptions_ShouldUseProvidedOptions()
    {
        // Arrange
        var services = new ServiceCollection();

        var preconfiguredOptions = new SftpStorageProviderOptions
        {
            DefaultHost = "preconfigured.example.com",
            DefaultPort = 2222,
            MaxPoolSize = 30,
        };

        // Act
        services.AddSftpStorageProvider(preconfiguredOptions);

        // Assert
        var serviceProvider = services.BuildServiceProvider();
        var options = serviceProvider.GetRequiredService<SftpStorageProviderOptions>();

        options.DefaultHost.Should().Be("preconfigured.example.com");
        options.DefaultPort.Should().Be(2222);
        options.MaxPoolSize.Should().Be(30);
    }

    [Fact]
    public void AddSftpStorageProvider_WithNullOptions_ShouldThrowArgumentNullException()
    {
        // Arrange
        var services = new ServiceCollection();
        SftpStorageProviderOptions? options = null;

        // Act
        var act = () => services.AddSftpStorageProvider(options!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void AddSftpStorageProvider_ShouldReturnSameServiceCollection()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        var result = services.AddSftpStorageProvider();

        // Assert
        result.Should().BeSameAs(services);
    }

    [Fact]
    public void AddSftpStorageProvider_ShouldRegisterSingletonServices()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSftpStorageProvider();
        var serviceProvider = services.BuildServiceProvider();

        // Act
        var options1 = serviceProvider.GetService<SftpStorageProviderOptions>();
        var options2 = serviceProvider.GetService<SftpStorageProviderOptions>();
        var factory1 = serviceProvider.GetService<SftpClientFactory>();
        var factory2 = serviceProvider.GetService<SftpClientFactory>();
        var provider1 = serviceProvider.GetService<SftpStorageProvider>();
        var provider2 = serviceProvider.GetService<SftpStorageProvider>();

        // Assert
        options1.Should().BeSameAs(options2);
        factory1.Should().BeSameAs(factory2);
        provider1.Should().BeSameAs(provider2);
    }
}
