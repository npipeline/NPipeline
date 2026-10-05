using System.Security.Cryptography;
using AwesomeAssertions;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

/// <summary>
///     Integration tests, against Azurite, for streaming writes (multi-block uploads and abandoned writes) and for
///     container creation and listing of containers that do not exist.
/// </summary>
public sealed class AzureBlobStorageProviderStreamingIntegrationTests(AzuriteFixture fixture) : IClassFixture<AzuriteFixture>
{
    private static string UniqueContainer() => $"stream-{Guid.NewGuid():N}";

    private static StorageUri Uri(string container, string blob) =>
        StorageUri.Parse($"azure://{container}/{blob}?accountName={AzuriteFixture.AccountName}");

    private AzureBlobStorageProvider NewProvider(bool createContainer, int partSizeBytes = 1024 * 1024)
    {
        var options = new AzureBlobStorageProviderOptions
        {
            ServiceUrl = fixture.Options.ServiceUrl,
            ServiceVersion = fixture.Options.ServiceVersion,
            AccountName = AzuriteFixture.AccountName,
            DefaultAccountKey = AzuriteFixture.AccountKey,
            UseDefaultCredentialChain = false,
            CreateContainerIfMissing = createContainer,
            PartSizeBytes = partSizeBytes,
            MaxConcurrency = 4,
        };

        return new AzureBlobStorageProvider(new AzureBlobClientFactory(options), options);
    }

    private static async Task<byte[]> ReadAllAsync(IStorageProvider provider, StorageUri uri)
    {
        await using var read = await provider.OpenReadAsync(uri);
        using var copy = new MemoryStream();
        await read.CopyToAsync(copy);

        return copy.ToArray();
    }

    [Fact]
    public async Task OpenWriteAsync_TwentyFiveMegabytesInSmallWrites_RoundTripsThroughBlocks()
    {
        var provider = NewProvider(true);
        var container = UniqueContainer();
        var uri = Uri(container, "big/stream.bin");
        var content = new byte[25 * 1024 * 1024 + 123];
        RandomNumberGenerator.Fill(content);

        await using (var write = await provider.OpenWriteAsync(uri, new StorageWriteOptions { ContentType = "application/x-test" }))
        {
            // Write in odd-sized chunks so block boundaries fall inside a write.
            for (var offset = 0; offset < content.Length; offset += 300_007)
            {
                await write.WriteAsync(content.AsMemory(offset, Math.Min(300_007, content.Length - offset)));
            }

            await write.CommitAsync();
        }

        var actual = await ReadAllAsync(provider, uri);
        actual.Length.Should().Be(content.Length);
        SHA256.HashData(actual).Should().Equal(SHA256.HashData(content));

        // 25 MiB in 1 MiB blocks cannot have been a single request, and the committed blob must have the content type.
        var blob = fixture.BlobServiceClient.GetBlobContainerClient(container).GetBlockBlobClient("big/stream.bin");
        var properties = (await blob.GetPropertiesAsync()).Value;
        properties.ContentType.Should().Be("application/x-test");
        properties.BlobType.Should().Be(BlobType.Block);

        var blockList = (await blob.GetBlockListAsync(BlockListTypes.Committed)).Value;
        blockList.CommittedBlocks.Count().Should().BeGreaterThan(20);
    }

    [Fact]
    public async Task OpenWriteAsync_SmallObject_IsASingleUpload()
    {
        var provider = NewProvider(true);
        var container = UniqueContainer();
        var uri = Uri(container, "small.txt");

        await using (var write = await provider.OpenWriteAsync(uri))
        {
            await write.WriteAsync("hello"u8.ToArray());
            await write.CommitAsync();
        }

        (await ReadAllAsync(provider, uri)).Should().Equal("hello"u8.ToArray());

        var blob = fixture.BlobServiceClient.GetBlobContainerClient(container).GetBlockBlobClient("small.txt");
        var blockList = (await blob.GetBlockListAsync(BlockListTypes.Committed)).Value;
        blockList.CommittedBlocks.Count().Should().BeLessThanOrEqualTo(1);
    }

    [Fact]
    public async Task OpenWriteAsync_AbandonedLargeWrite_LeavesNoBlob()
    {
        var provider = NewProvider(true);
        var container = UniqueContainer();
        var uri = Uri(container, "abandoned.bin");

        await using (var write = await provider.OpenWriteAsync(uri))
        {
            // More than three blocks, so blocks are staged before the write is abandoned.
            await write.WriteAsync(new byte[4 * 1024 * 1024 + 5]);

            // Disposed without CommitAsync.
        }

        (await provider.ExistsAsync(uri)).Should().BeFalse();
        (await provider.ListAsync(Uri(container, string.Empty), true).ToListAsync()).Should().BeEmpty();
    }

    [Fact]
    public async Task OpenWriteAsync_AbandonedSmallWrite_LeavesNoBlob()
    {
        var provider = NewProvider(true);
        var container = UniqueContainer();
        var uri = Uri(container, "abandoned-small.bin");

        await using (var write = await provider.OpenWriteAsync(uri))
        {
            await write.WriteAsync(new byte[10]);
        }

        (await provider.ExistsAsync(uri)).Should().BeFalse();
    }

    [Fact]
    public async Task OpenWriteAsync_AbandonedOverwrite_KeepsTheOriginalContent()
    {
        var provider = NewProvider(true);
        var container = UniqueContainer();
        var uri = Uri(container, "keep.bin");

        await using (var first = await provider.OpenWriteAsync(uri))
        {
            await first.WriteAsync("original"u8.ToArray());
            await first.CommitAsync();
        }

        await using (var second = await provider.OpenWriteAsync(uri))
        {
            await second.WriteAsync(new byte[3 * 1024 * 1024]);
        }

        (await ReadAllAsync(provider, uri)).Should().Equal("original"u8.ToArray());
    }

    [Fact]
    public async Task OpenWriteAsync_LargeObjectWithOverwriteFalse_FailsWhenTheBlobExists()
    {
        var provider = NewProvider(true);
        var container = UniqueContainer();
        var uri = Uri(container, "exists.bin");

        await using (var first = await provider.OpenWriteAsync(uri))
        {
            await first.WriteAsync("original"u8.ToArray());
            await first.CommitAsync();
        }

        await using var second = await provider.OpenWriteAsync(uri, new StorageWriteOptions { Overwrite = false });
        await second.WriteAsync(new byte[3 * 1024 * 1024]);

        var act = async () => await second.CommitAsync();

        await act.Should().ThrowAsync<NPipeline.StorageProviders.Exceptions.StoragePreconditionFailedException>();
        (await ReadAllAsync(provider, uri)).Should().Equal("original"u8.ToArray());
    }

    [Fact]
    public async Task OpenWrite_DefaultOptions_DoesNotCreateContainer()
    {
        var provider = NewProvider(false);
        var container = UniqueContainer();
        var uri = Uri(container, "a.txt");

        await using var write = await provider.OpenWriteAsync(uri);
        await write.WriteAsync("x"u8.ToArray());

        var act = async () => await write.CommitAsync();

        await act.Should().ThrowAsync<FileNotFoundException>();
        (await fixture.BlobServiceClient.GetBlobContainerClient(container).ExistsAsync()).Value.Should().BeFalse();
    }

    [Fact]
    public async Task OpenWrite_CreateContainerIfMissing_CreatesTheContainerAndWrites()
    {
        var provider = NewProvider(true);
        var container = UniqueContainer();

        for (var i = 0; i < 3; i++)
        {
            await using var write = await provider.OpenWriteAsync(Uri(container, $"file-{i}.txt"));
            await write.WriteAsync("x"u8.ToArray());
            await write.CommitAsync();
        }

        (await fixture.BlobServiceClient.GetBlobContainerClient(container).ExistsAsync()).Value.Should().BeTrue();
        (await provider.ListAsync(Uri(container, string.Empty), true).ToListAsync()).Should().HaveCount(3);
    }

    [Fact]
    public async Task ListAsync_MissingContainer_YieldsNothing()
    {
        var provider = NewProvider(false);

        (await provider.ListAsync(Uri(UniqueContainer(), string.Empty), true).ToListAsync()).Should().BeEmpty();
        (await provider.ListAsync(Uri(UniqueContainer(), "dir/"), false).ToListAsync()).Should().BeEmpty();
    }
}
