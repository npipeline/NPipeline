using AwesomeAssertions;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Tests;

public sealed class StorageResolverTests
{
    [Fact]
    public void Resolve_SchemeServedByProvider_ReturnsThatProvider()
    {
        var multi = new TestStorageProvider("multi", StorageCapabilities.None, "foo", "bar");
        var other = new TestStorageProvider("other", StorageCapabilities.None, "baz");
        var resolver = new StorageResolver([multi, other]);

        resolver.Resolve(StorageUri.Parse("foo://bucket/key")).Should().BeSameAs(multi);
        resolver.Resolve(StorageUri.Parse("bar://bucket/key")).Should().BeSameAs(multi);
        resolver.Resolve(StorageUri.Parse("baz://bucket/key")).Should().BeSameAs(other);
        resolver.Providers.Should().Equal(multi, other);
    }

    [Fact]
    public void Resolve_UnknownScheme_ThrowsStorageProviderNotFoundException()
    {
        var resolver = new StorageResolver([new TestStorageProvider("foo", StorageCapabilities.None, "foo")]);

        var act = () => resolver.Resolve(StorageUri.Parse("nope://bucket/key"));

        act.Should().Throw<StorageProviderNotFoundException>().Which.Scheme.Should().Be("nope");
        typeof(StorageProviderNotFoundException).IsPublic.Should().BeTrue();
    }

    [Fact]
    public void TryResolve_UnknownScheme_ReturnsFalse()
    {
        var resolver = new StorageResolver([]);

        resolver.TryResolve(StorageUri.Parse("nope://bucket/key"), out var provider).Should().BeFalse();
        provider.Should().BeNull();
    }

    [Fact]
    public void Constructor_DuplicateScheme_ThrowsNamingBothProviders()
    {
        var first = new TestStorageProvider("First", StorageCapabilities.None, "s3");
        var second = new TestStorageProvider("Second", StorageCapabilities.None, "s3");

        var act = () => new StorageResolver([first, second]);

        act.Should().Throw<ArgumentException>().Which.Message.Should().Contain("First").And.Contain("Second").And.Contain("s3");
    }

    [Fact]
    public void Constructor_SameInstanceListedTwice_IsAccepted()
    {
        var provider = new TestStorageProvider("foo", StorageCapabilities.None, "foo");

        var resolver = new StorageResolver([provider, provider]);

        resolver.Providers.Should().ContainSingle();
    }

    [Fact]
    public void Default_ServesTheFileSystemOnly()
    {
        StorageResolver.Default.Providers.Should().ContainSingle().Which.Should().BeOfType<FileSystemStorageProvider>();
        StorageResolver.Default.Resolve(StorageUri.FromFilePath(Path.GetTempPath())).Should().BeOfType<FileSystemStorageProvider>();
        StorageResolver.Default.Should().BeSameAs(StorageResolver.Default);
    }
}
