using AwesomeAssertions;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Tests;

public sealed class StorageResolverTests
{
    [Fact]
    public void ResolveProvider_WhenNoCanHandle_MatchesByScheme()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new FooProvider());

        var uri = StorageUri.Parse("foo://bucket/path");

        var provider = resolver.ResolveProvider(uri);
        provider.Should().NotBeNull();
        provider!.Scheme.ToString().Should().Be("foo");
    }

    [Fact]
    public void RegisterProvider_SameTypeRegisteredTwice_AllowsMultipleInstances()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new FooProvider());
        resolver.RegisterProvider(new FooProvider()); // same type

        var all = resolver.GetAvailableProviders().ToArray();

        all.Count(p => p is FooProvider).Should().Be(2);
        all.All(p => p.Scheme.ToString() == "foo").Should().BeTrue();
    }

    private sealed class FooProvider : IStorageProvider
    {
        public StorageScheme Scheme => new("foo");

        public bool CanHandle(StorageUri uri)
        {
            return false;

            // force fallback path
        }

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException();
        }

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(false);
        }
    }
}
