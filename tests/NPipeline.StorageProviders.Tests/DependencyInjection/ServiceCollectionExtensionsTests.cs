using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.DependencyInjection;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Tests.DependencyInjection;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddStorageResolver_ResolverSeesRegisteredProviders()
    {
        var services = new ServiceCollection();
        var provider = new TestStorageProvider("test", StorageCapabilities.None, "test");

        services.AddStorageResolver(false);
        services.AddStorageProvider(provider); // registered after the resolver: still visible

        using var sp = services.BuildServiceProvider();

        sp.GetRequiredService<IStorageResolver>().Resolve(StorageUri.Parse("test://host/path")).Should().BeSameAs(provider);
    }

    [Fact]
    public void AddStorageResolver_Default_ResolvesFileUris()
    {
        var services = new ServiceCollection();
        services.AddStorageResolver();

        using var sp = services.BuildServiceProvider();

        sp.GetRequiredService<IStorageResolver>().Resolve(StorageUri.FromFilePath(Path.GetTempPath())).Should().BeOfType<FileSystemStorageProvider>();
    }

    [Fact]
    public void AddStorageResolver_WithoutFileSystem_DoesNotServeFiles()
    {
        var services = new ServiceCollection();
        services.AddStorageResolver(false);

        using var sp = services.BuildServiceProvider();

        sp.GetRequiredService<IStorageResolver>().TryResolve(StorageUri.FromFilePath(Path.GetTempPath()), out _).Should().BeFalse();
    }

    [Fact]
    public void AddStorageResolver_CalledTwice_RegistersOneResolver()
    {
        var services = new ServiceCollection();

        services.AddStorageResolver().AddStorageResolver();

        services.Count(d => d.ServiceType == typeof(IStorageResolver)).Should().Be(1);
        services.Count(d => d.ServiceType == typeof(IStorageProvider)).Should().Be(1);
    }

    [Fact]
    public void AddStorageResolver_TwoProvidersForOneScheme_FailsWhenResolved()
    {
        var services = new ServiceCollection();
        services.AddStorageResolver(false);
        services.AddStorageProvider(new TestStorageProvider("First", StorageCapabilities.None, "s3"));
        services.AddStorageProvider(new TestStorageProvider("Second", StorageCapabilities.None, "s3"));

        using var sp = services.BuildServiceProvider();

        var act = () => sp.GetRequiredService<IStorageResolver>();

        act.Should().Throw<ArgumentException>().Which.Message.Should().Contain("First").And.Contain("Second");
    }

    [Fact]
    public void AddStorageProvider_Generic_RegistersASingletonOnce()
    {
        var services = new ServiceCollection();

        services.AddStorageProvider<FileSystemStorageProvider>().AddStorageProvider<FileSystemStorageProvider>();

        services.Should().ContainSingle(d => d.ServiceType == typeof(IStorageProvider)
                                             && d.ImplementationType == typeof(FileSystemStorageProvider)
                                             && d.Lifetime == ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddFileSystemStorageProvider_RegistersTheFileSystemProvider()
    {
        var services = new ServiceCollection();

        services.AddFileSystemStorageProvider();

        using var sp = services.BuildServiceProvider();

        sp.GetServices<IStorageProvider>().Should().ContainSingle().Which.Should().BeOfType<FileSystemStorageProvider>();
    }

    [Fact]
    public void NullArguments_Throw()
    {
        var services = new ServiceCollection();

        Action nullServices = () => ServiceCollectionExtensions.AddStorageProvider<FileSystemStorageProvider>(null!);
        Action nullInstance = () => services.AddStorageProvider(null!);
        Action nullResolverServices = () => ServiceCollectionExtensions.AddStorageResolver(null!);

        nullServices.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("services");
        nullInstance.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("instance");
        nullResolverServices.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("services");
    }
}
