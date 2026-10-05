using System.Security.Cryptography;
using AwesomeAssertions;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

/// <summary>
///     Integration tests, against Azurite, for streaming writes, container creation and moves across filesystems. Azurite
///     has no hierarchical namespace, so these exercise the blob-listing and copy-and-delete paths.
/// </summary>
public sealed class AdlsGen2StorageProviderStreamingIntegrationTests(AzuriteAdlsFixture fixture) : IClassFixture<AzuriteAdlsFixture>
{
    private static string UniqueFilesystem() => $"fs{Guid.NewGuid():N}"[..20];

    private static StorageUri Uri(string filesystem, string path) =>
        StorageUri.Parse($"adls://{filesystem}/{path}?accountName={AzuriteAdlsFixture.AccountName}");

    private AdlsGen2StorageProvider NewProvider(bool createContainer)
    {
        var options = new AdlsGen2StorageProviderOptions
        {
            DefaultConnectionString = fixture.GetConnectionString(),
            ServiceVersion = fixture.Options.ServiceVersion,
            UseDefaultCredentialChain = false,
            CreateContainerIfMissing = createContainer,
            PartSizeBytes = 1024 * 1024,
            MaxConcurrency = 4,
        };

        return new AdlsGen2StorageProvider(new AdlsGen2ClientFactory(options), options);
    }

    private static async Task<List<StorageItem>> CollectAsync(IAsyncEnumerable<StorageItem> source)
    {
        var items = new List<StorageItem>();

        await foreach (var item in source)
        {
            items.Add(item);
        }

        return items;
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
        var filesystem = UniqueFilesystem();
        var uri = Uri(filesystem, "big/stream.bin");
        var content = new byte[25 * 1024 * 1024 + 123];
        RandomNumberGenerator.Fill(content);

        await using (var write = await provider.OpenWriteAsync(uri, new StorageWriteOptions { ContentType = "application/x-test" }))
        {
            for (var offset = 0; offset < content.Length; offset += 300_007)
            {
                await write.WriteAsync(content.AsMemory(offset, Math.Min(300_007, content.Length - offset)));
            }

            await write.CommitAsync();
        }

        var actual = await ReadAllAsync(provider, uri);
        actual.Length.Should().Be(content.Length);
        SHA256.HashData(actual).Should().Equal(SHA256.HashData(content));

        var blob = fixture.BlobServiceClient.GetBlobContainerClient(filesystem).GetBlockBlobClient("big/stream.bin");
        (await blob.GetPropertiesAsync()).Value.ContentType.Should().Be("application/x-test");
        (await blob.GetBlockListAsync(BlockListTypes.Committed)).Value.CommittedBlocks.Count().Should().BeGreaterThan(20);
    }

    [Fact]
    public async Task OpenWriteAsync_AbandonedLargeWrite_LeavesNoBlob()
    {
        var provider = NewProvider(true);
        var filesystem = UniqueFilesystem();
        var uri = Uri(filesystem, "abandoned.bin");

        await using (var write = await provider.OpenWriteAsync(uri))
        {
            await write.WriteAsync(new byte[4 * 1024 * 1024 + 5]);
        }

        (await provider.ExistsAsync(uri)).Should().BeFalse();
        (await CollectAsync(provider.ListAsync(Uri(filesystem, string.Empty), true))).Should().BeEmpty();
    }

    [Fact]
    public async Task OpenWriteAsync_AbandonedOverwrite_KeepsTheOriginalContent()
    {
        var provider = NewProvider(true);
        var uri = Uri(UniqueFilesystem(), "keep.bin");

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
    public async Task OpenWriteAsync_LargeObjectWithOverwriteFalse_FailsWhenTheFileExists()
    {
        var provider = NewProvider(true);
        var uri = Uri(UniqueFilesystem(), "exists.bin");

        await using (var first = await provider.OpenWriteAsync(uri))
        {
            await first.WriteAsync("original"u8.ToArray());
            await first.CommitAsync();
        }

        await using var second = await provider.OpenWriteAsync(uri, new StorageWriteOptions { Overwrite = false });
        await second.WriteAsync(new byte[3 * 1024 * 1024]);

        var act = async () => await second.CommitAsync();

        await act.Should().ThrowAsync<StoragePreconditionFailedException>();
        (await ReadAllAsync(provider, uri)).Should().Equal("original"u8.ToArray());
    }

    [Fact]
    public async Task OpenWrite_DefaultOptions_DoesNotCreateContainer()
    {
        var provider = NewProvider(false);
        var filesystem = UniqueFilesystem();

        await using var write = await provider.OpenWriteAsync(Uri(filesystem, "a.txt"));
        await write.WriteAsync("x"u8.ToArray());

        var act = async () => await write.CommitAsync();

        await act.Should().ThrowAsync<FileNotFoundException>();
        (await fixture.BlobServiceClient.GetBlobContainerClient(filesystem).ExistsAsync()).Value.Should().BeFalse();
    }

    [Fact]
    public async Task OpenWrite_CreateContainerIfMissing_CreatesTheFilesystemAndWritesSeveralFiles()
    {
        var provider = NewProvider(true);
        var filesystem = UniqueFilesystem();

        for (var i = 0; i < 3; i++)
        {
            await using var write = await provider.OpenWriteAsync(Uri(filesystem, $"dir/file-{i}.txt"));
            await write.WriteAsync("x"u8.ToArray());
            await write.CommitAsync();
        }

        (await fixture.BlobServiceClient.GetBlobContainerClient(filesystem).ExistsAsync()).Value.Should().BeTrue();
        (await CollectAsync(provider.ListAsync(Uri(filesystem, string.Empty), true))).Should().HaveCount(3);
    }

    [Fact]
    public async Task ListAsync_MissingFilesystem_YieldsNothing()
    {
        var provider = NewProvider(false);

        (await CollectAsync(provider.ListAsync(Uri(UniqueFilesystem(), string.Empty), true))).Should().BeEmpty();
        (await CollectAsync(provider.ListAsync(Uri(UniqueFilesystem(), "dir/"), false))).Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_NonRecursive_CaseDistinctDirectoriesBothAppear()
    {
        var provider = NewProvider(true);
        var filesystem = UniqueFilesystem();

        foreach (var name in new[] { "Data/a.txt", "data/a.txt" })
        {
            await using var write = await provider.OpenWriteAsync(Uri(filesystem, name));
            await write.WriteAsync("x"u8.ToArray());
            await write.CommitAsync();
        }

        var items = await CollectAsync(provider.ListAsync(Uri(filesystem, string.Empty), false));

        items.Where(i => i.IsDirectory).Select(i => i.Uri.Path).Should().BeEquivalentTo("/Data/", "/data/");
        items.Should().OnlyContain(i => i.Uri.Parameters["accountName"] == AzuriteAdlsFixture.AccountName);
    }

    [Fact]
    public async Task MoveAsync_AcrossFilesystems_LandsInDestinationFilesystem()
    {
        var provider = NewProvider(true);
        var sourceFilesystem = UniqueFilesystem();
        var destinationFilesystem = UniqueFilesystem();
        var source = Uri(sourceFilesystem, "dir/file.txt");
        var destination = Uri(destinationFilesystem, "moved/file.txt");

        await using (var write = await provider.OpenWriteAsync(source))
        {
            await write.WriteAsync("moving across filesystems"u8.ToArray());
            await write.CommitAsync();
        }

        // The destination filesystem has to exist, as it does on a real account.
        _ = await fixture.BlobServiceClient.GetBlobContainerClient(destinationFilesystem).CreateIfNotExistsAsync();

        await provider.MoveAsync(source, destination);

        (await provider.ExistsAsync(source)).Should().BeFalse();
        (await ReadAllAsync(provider, destination)).Should().Equal("moving across filesystems"u8.ToArray());
        (await fixture.BlobServiceClient.GetBlobContainerClient(destinationFilesystem).GetBlobClient("moved/file.txt").ExistsAsync()).Value.Should().BeTrue();
        (await fixture.BlobServiceClient.GetBlobContainerClient(sourceFilesystem).GetBlobClient("dir/file.txt").ExistsAsync()).Value.Should().BeFalse();
    }

    [Fact]
    public async Task MoveAsync_NonHierarchicalAccount_CopiesAndDeletes()
    {
        var provider = NewProvider(true);
        var filesystem = UniqueFilesystem();
        var source = Uri(filesystem, "a.txt");
        var destination = Uri(filesystem, "b.txt");

        await using (var write = await provider.OpenWriteAsync(source))
        {
            await write.WriteAsync("copy me"u8.ToArray());
            await write.CommitAsync();
        }

        await provider.MoveAsync(source, destination);

        (await provider.ExistsAsync(source)).Should().BeFalse();
        (await ReadAllAsync(provider, destination)).Should().Equal("copy me"u8.ToArray());
    }

    [Fact]
    public async Task MoveAsync_MissingSource_ThrowsFileNotFound()
    {
        var provider = NewProvider(true);
        var filesystem = UniqueFilesystem();
        _ = await fixture.BlobServiceClient.GetBlobContainerClient(filesystem).CreateIfNotExistsAsync();

        var act = async () => await provider.MoveAsync(Uri(filesystem, "missing.txt"), Uri(filesystem, "dest.txt"));

        await act.Should().ThrowAsync<FileNotFoundException>();
    }
}
