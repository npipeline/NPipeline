using AwesomeAssertions;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Tests;

public sealed class StorageProviderBaseTests
{
    private static readonly StorageUri Uri = StorageUri.Parse("test://host/dir/present");

    [Fact]
    public async Task UndeclaredCapabilities_ThrowUnsupportedStorageCapability()
    {
        var provider = new TestStorageProvider("none", StorageCapabilities.None, "test");

        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.OpenReadAsync(Uri));
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.OpenWriteAsync(Uri));
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.GetMetadataAsync(Uri));
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.ExistsAsync(Uri));
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.DeleteAsync(Uri));
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => provider.MoveAsync(Uri, Uri));
        Assert.Throws<UnsupportedStorageCapabilityException>(() => provider.ListAsync(Uri));
        provider.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task DeclaredCapabilities_ReachTheCoreMethods()
    {
        var provider = new TestStorageProvider("all", StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move, "test");

        await (await provider.OpenReadAsync(Uri)).DisposeAsync();
        await (await provider.OpenWriteAsync(Uri)).DisposeAsync();
        _ = await provider.GetMetadataAsync(Uri);
        await provider.DeleteAsync(Uri);
        await provider.MoveAsync(Uri, Uri);
        await foreach (var _ in provider.ListAsync(Uri))
        {
        }

        provider.Calls.Should().Equal("read", "write", "metadata", "delete", "move", "list");
    }

    [Fact]
    public async Task ExistsAsync_DefaultsToMetadataBeingPresent()
    {
        var provider = new TestStorageProvider("read", StorageCapabilities.Read, "test");

        (await provider.ExistsAsync(StorageUri.Parse("test://host/present"))).Should().BeTrue();
        (await provider.ExistsAsync(StorageUri.Parse("test://host/absent"))).Should().BeFalse();
    }

    [Fact]
    public async Task ListAsync_DirectoryWithoutTrailingSlash_IsListedAsADirectory()
    {
        var provider = new TestStorageProvider("list", StorageCapabilities.List, "test");

        await foreach (var _ in provider.ListAsync(StorageUri.Parse("test://host/logs")))
        {
        }

        provider.LastListed!.Path.Should().Be("/logs/");
    }

    [Fact]
    public async Task ConditionalWrite_WithoutTheCapability_ThrowsUnsupported()
    {
        var plain = new TestStorageProvider("plain", StorageCapabilities.Write, "test");
        var conditional = new TestStorageProvider("conditional", StorageCapabilities.Write | StorageCapabilities.ConditionalWrite, "test");

        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => plain.OpenWriteAsync(Uri, new StorageWriteOptions { Overwrite = false }));
        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => plain.OpenWriteAsync(Uri, new StorageWriteOptions { IfMatch = "etag" }));
        await (await plain.OpenWriteAsync(Uri, new StorageWriteOptions { ContentType = "text/csv" })).DisposeAsync();
        await (await conditional.OpenWriteAsync(Uri, new StorageWriteOptions { Overwrite = false })).DisposeAsync();
    }

    [Fact]
    public async Task CancelledToken_ThrowsBeforeReachingTheCore()
    {
        var provider = new TestStorageProvider("all", StorageCapabilities.Read | StorageCapabilities.Delete, "test");
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.OpenReadAsync(Uri, cancelled.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provider.DeleteAsync(Uri, cancelled.Token));
        provider.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task NullUri_ThrowsArgumentNull()
    {
        var provider = new TestStorageProvider("all", StorageCapabilities.Read | StorageCapabilities.Move, "test");

        await Assert.ThrowsAsync<ArgumentNullException>(() => provider.OpenReadAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => provider.MoveAsync(Uri, null!));
    }
}
