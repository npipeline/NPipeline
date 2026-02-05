using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Connectors.DependencyInjection;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Configuration;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.Connectors.Tests.DependencyInjection;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddStorageProvider_WithNullServices_Throws()
    {
        Action act = () => ServiceCollectionExtensions.AddStorageProvider<TestStorageProvider>(null!);

        act.Should().Throw<ArgumentNullException>()
            .Which.ParamName.Should().Be("services");
    }

    [Fact]
    public void AddStorageProvider_RegistersTransientProvider()
    {
        var services = new ServiceCollection();

        services.AddStorageProvider<TestStorageProvider>();

        services.Should().ContainSingle(sd => sd.ServiceType == typeof(IStorageProvider)
                                              && sd.ImplementationType == typeof(TestStorageProvider)
                                              && sd.Lifetime == ServiceLifetime.Transient);
    }

    [Fact]
    public void AddStorageProvider_Instance_WithNullServices_Throws()
    {
        Action act = () => ServiceCollectionExtensions.AddStorageProvider(null!, new TestStorageProvider());

        act.Should().Throw<ArgumentNullException>()
            .Which.ParamName.Should().Be("services");
    }

    [Fact]
    public void AddStorageProvider_Instance_WithNullInstance_Throws()
    {
        var services = new ServiceCollection();

        Action act = () => services.AddStorageProvider(null!);

        act.Should().Throw<ArgumentNullException>()
            .Which.ParamName.Should().Be("instance");
    }

    [Fact]
    public void AddStorageProvider_WithInstance_RegistersSingletonInstance()
    {
        var services = new ServiceCollection();
        var provider = new TestStorageProvider();

        services.AddStorageProvider(provider);

        services.Should().ContainSingle(sd => sd.ServiceType == typeof(IStorageProvider)
                                              && sd.Lifetime == ServiceLifetime.Singleton
                                              && ReferenceEquals(sd.ImplementationInstance, provider));
    }

    [Fact]
    public void AddDefaultFileStorageProvider_RegistersFileSystemProvider()
    {
        var services = new ServiceCollection();

        services.AddDefaultFileStorageProvider();

        services.Should().ContainSingle(sd => sd.ServiceType == typeof(IStorageProvider)
                                              && sd.ImplementationType == typeof(FileSystemStorageProvider)
                                              && sd.Lifetime == ServiceLifetime.Transient);
    }

    [Fact]
    public void AddStorageResolver_DefaultIncludesFileSystemProvider()
    {
        var services = new ServiceCollection();

        services.AddStorageResolver();

        services.Should().ContainSingle(sd => sd.ServiceType == typeof(IStorageResolver)
                                              && sd.Lifetime == ServiceLifetime.Singleton);

        services.Should().Contain(sd => sd.ImplementationType == typeof(FileSystemStorageProvider));
    }

    [Fact]
    public void AddStorageResolver_WhenIncludeFileSystemFalse_OnlyAddsResolver()
    {
        var services = new ServiceCollection();

        services.AddStorageResolver(false);

        services.Should().ContainSingle(sd => sd.ServiceType == typeof(IStorageResolver));
        services.Should().NotContain(sd => sd.ImplementationType == typeof(FileSystemStorageProvider));
    }

    [Fact]
    public void AddStorageProvidersFromConfiguration_WithNullServices_Throws()
    {
        Action act = () => ServiceCollectionExtensions.AddStorageProvidersFromConfiguration(null!, _ => { });

        act.Should().Throw<ArgumentNullException>()
            .Which.ParamName.Should().Be("services");
    }

    [Fact]
    public void AddStorageProvidersFromConfiguration_WithNullConfigure_Throws()
    {
        var services = new ServiceCollection();

        Action act = () => services.AddStorageProvidersFromConfiguration(null!);

        act.Should().Throw<ArgumentNullException>()
            .Which.ParamName.Should().Be("configure");
    }

    [Fact]
    public void AddStorageProvidersFromConfiguration_SkipsInvalidProviderTypes()
    {
        var services = new ServiceCollection();

        services.AddStorageProvidersFromConfiguration(config =>
        {
            config.Providers.Add("InvalidProvider", new StorageProviderConfig
            {
                ProviderType = "NonExistent.Type.Name",
            });
        });

        services.Should().NotContain(sd => sd.ServiceType == typeof(IStorageProvider));
    }

    [Fact]
    public void AddStorageProvidersFromConfiguration_RegistersValidProviders()
    {
        var services = new ServiceCollection();

        services.AddStorageProvidersFromConfiguration(config =>
        {
            config.Providers.Add("TestProvider", new StorageProviderConfig
            {
                ProviderType = typeof(TestStorageProvider).AssemblyQualifiedName!,
            });
        });

        services.Should().Contain(sd => sd.ServiceType == typeof(IStorageProvider)
                                        && sd.ImplementationType == typeof(TestStorageProvider));
    }

    [Fact]
    public void AddConnectorsFromConfiguration_WithNullServices_Throws()
    {
        Action act = () => ServiceCollectionExtensions.AddConnectorsFromConfiguration(null!, _ => { });

        act.Should().Throw<ArgumentNullException>()
            .Which.ParamName.Should().Be("services");
    }

    [Fact]
    public void AddConnectorsFromConfiguration_WithNullConfigure_Throws()
    {
        var services = new ServiceCollection();

        Action act = () => services.AddConnectorsFromConfiguration(null!);

        act.Should().Throw<ArgumentNullException>()
            .Which.ParamName.Should().Be("configure");
    }

    [Fact]
    public void AddConnectorsFromConfiguration_RegistersResolverAndProviders()
    {
        var services = new ServiceCollection();

        services.AddConnectorsFromConfiguration(config =>
        {
            config.Providers.Add("TestProvider", new StorageProviderConfig
            {
                ProviderType = typeof(TestStorageProvider).AssemblyQualifiedName!,
            });
        });

        services.Should().Contain(sd => sd.ServiceType == typeof(IStorageProvider)
                                        && sd.ImplementationType == typeof(TestStorageProvider));

        services.Should().ContainSingle(sd => sd.ServiceType == typeof(IStorageResolver));
        services.Should().NotContain(sd => sd.ImplementationType == typeof(FileSystemStorageProvider));
    }

    private sealed class TestStorageProvider : IStorageProvider
    {
        public StorageScheme Scheme => new("test");

        public bool CanHandle(StorageUri uri)
        {
            return uri.Scheme.ToString() == "test";
        }

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return Task.FromResult<Stream>(Stream.Null);
        }

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return Task.FromResult<Stream>(Stream.Null);
        }

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(false);
        }

        public Task DeleteAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public IAsyncEnumerable<StorageItem> ListAsync(StorageUri prefix, bool recursive = false, CancellationToken cancellationToken = default)
        {
            return AsyncEnumerable.Empty<StorageItem>();
        }

        public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return Task.FromResult<StorageMetadata?>(null);
        }
    }
}
