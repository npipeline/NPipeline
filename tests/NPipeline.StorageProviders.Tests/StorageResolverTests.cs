using AwesomeAssertions;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Tests;

public sealed class StorageResolverTests
{
    [Fact]
    public void ResolveProvider_ProviderRefusesUri_ReturnsNull()
    {
        // The provider claims the "foo" scheme but its CanHandle refuses the URI: the resolver must not return it.
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new FooProvider());

        resolver.ResolveProvider(StorageUri.Parse("foo://bucket/path")).Should().BeNull();
    }

    [Fact]
    public void ResolveProvider_CanHandleThrows_Propagates()
    {
        var resolver = new StorageResolver();
        resolver.RegisterProvider(new ThrowingProvider());

        var act = () => resolver.ResolveProvider(StorageUri.Parse("foo://bucket/path"));

        act.Should().Throw<InvalidOperationException>().WithMessage("misconfigured");
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

    [Fact]
    public void GetProviderOrThrow_UnknownScheme_ThrowsPublicStorageProviderNotFoundException()
    {
        var act = () => StorageProviderFactory.GetProviderOrThrow(new StorageResolver(), StorageUri.Parse("nope://bucket/key"));

        act.Should().Throw<NPipeline.StorageProviders.Exceptions.StorageProviderNotFoundException>()
            .Which.Scheme.Should().Be("nope");

        typeof(NPipeline.StorageProviders.Exceptions.StorageProviderNotFoundException).IsPublic.Should().BeTrue();
    }

    private sealed class FooProvider : IStorageProvider
    {
        public StorageScheme Scheme => new("foo");

        public bool CanHandle(StorageUri uri) => false;

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default) => Task.FromResult(false);
    }

    private sealed class ThrowingProvider : IStorageProvider
    {
        public StorageScheme Scheme => new("foo");

        public bool CanHandle(StorageUri uri) => throw new InvalidOperationException("misconfigured");

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default) => throw new NotSupportedException();

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default) => Task.FromResult(false);
    }
}
