using AwesomeAssertions;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common.Storage;
using Xunit;

namespace NPipeline.StorageProviders.S3.Compatible.Tests;

/// <summary>Runs the shared storage provider contract against a real S3 emulator.</summary>
public sealed class FlociS3ConformanceTests(FlociS3Fixture fixture) : StorageProviderConformanceTests, IClassFixture<FlociS3Fixture>
{
    private string _bucket = null!;

    protected override StorageUri RootUri => StorageUri.Parse($"s3://{_bucket}/root/");

    // S3 has no atomic conditional writes on every compatible service, and key length is bounded at 1,024 bytes.
    protected override async Task<IStorageProvider> CreateProviderAsync()
    {
        _bucket = await fixture.CreateBucketAsync();

        return fixture.Provider;
    }
}

public sealed class FlociS3StreamingUploadTests(FlociS3Fixture fixture) : IClassFixture<FlociS3Fixture>
{
    private const int PartSize = S3CoreOptions.MinPartSizeBytes;

    [Fact]
    public async Task OpenWriteAsync_ObjectOfSeveralParts_UploadsWhileWritingAndRoundTrips()
    {
        var bucket = await fixture.CreateBucketAsync();
        var uri = StorageUri.Parse($"s3://{bucket}/big.bin");
        var content = new byte[(PartSize * 3) + 12345];
        new Random(42).NextBytes(content);

        await using (var stream = await fixture.Provider.OpenWriteAsync(uri))
        {
            // Write in slices that straddle the part boundaries.
            for (var offset = 0; offset < content.Length; offset += 700_000)
                await stream.WriteAsync(content.AsMemory(offset, Math.Min(700_000, content.Length - offset)));

            await stream.CommitAsync();
            stream.ETag.Should().NotBeNullOrEmpty();
        }

        await using var read = await fixture.Provider.OpenReadAsync(uri);
        using var copy = new MemoryStream();
        await read.CopyToAsync(copy);

        copy.ToArray().Should().Equal(content);
    }

    [Fact]
    public async Task OpenWriteAsync_AbandonedAfterPartsStarted_LeavesNoObjectAndNoUpload()
    {
        var bucket = await fixture.CreateBucketAsync();
        var uri = StorageUri.Parse($"s3://{bucket}/abandoned.bin");

        await using (var stream = await fixture.Provider.OpenWriteAsync(uri))
        {
            await stream.WriteAsync(new byte[(PartSize * 2) + 1]);

            // Dispose without CommitAsync: the multipart upload is aborted.
        }

        (await fixture.Provider.ExistsAsync(uri)).Should().BeFalse();

        var uploads = await fixture.Client.ListMultipartUploadsAsync(bucket);
        uploads.MultipartUploads.Should().BeNullOrEmpty();
    }

    [Fact]
    public async Task OpenWriteAsync_FailedWriter_NeverPublishesAPartialObject()
    {
        var bucket = await fixture.CreateBucketAsync();
        var uri = StorageUri.Parse($"s3://{bucket}/failed.bin");

        var act = async () =>
        {
            await using var stream = await fixture.Provider.OpenWriteAsync(uri);
            await stream.WriteAsync(new byte[PartSize + 10]);

            throw new InvalidOperationException("the producer failed halfway");
        };

        await act.Should().ThrowAsync<InvalidOperationException>();
        (await fixture.Provider.ExistsAsync(uri)).Should().BeFalse();
    }

    [Fact]
    public async Task OpenWriteAsync_LengthHint_AllowsAnObjectAboveTheTenThousandPartLimitAtTheDefaultPartSize()
    {
        // Only the part-size choice is observable without writing 80 GB: a hint of 100 GB must not fail at construction.
        var bucket = await fixture.CreateBucketAsync();
        var uri = StorageUri.Parse($"s3://{bucket}/hinted.bin");

        await using var stream = await fixture.Provider.OpenWriteAsync(uri, new StorageWriteOptions { LengthHint = 100L * 1024 * 1024 * 1024 });

        stream.CanWrite.Should().BeTrue();
    }
}
