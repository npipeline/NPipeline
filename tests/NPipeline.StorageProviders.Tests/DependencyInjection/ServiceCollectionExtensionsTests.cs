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
        var provider = new TestSchemeProvider();

        services.AddStorageResolver(false);
        services.AddStorageProvider(provider); // registered after the resolver: still visible

        using var sp = services.BuildServiceProvider();
        var resolver = sp.GetRequiredService<IStorageResolver>();

        resolver.ResolveProvider(StorageUri.Parse("test://host/path")).Should().BeSameAs(provider);
    }

    [Fact]
    public void AddStorageResolver_Default_ResolvesFileUris()
    {
        var services = new ServiceCollection();
        services.AddStorageResolver();

        using var sp = services.BuildServiceProvider();
        var resolver = sp.GetRequiredService<IStorageResolver>();

        resolver.ResolveProvider(StorageUri.FromFilePath(Path.GetTempPath())).Should().BeOfType<FileSystemStorageProvider>();
    }

    private sealed class TestSchemeProvider : IStorageProvider
    {
        public StorageScheme Scheme => new("test");

        public bool CanHandle(StorageUri uri) => uri.Scheme == Scheme;

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
