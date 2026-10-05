using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.S3.Tests;

public class S3CoreStorageProviderTests : IAsyncDisposable
{
    // ── Setup ─────────────────────────────────────────────────────────────

    private readonly IAmazonS3 _fakeS3;
    private readonly TestStorageProvider _provider;

    public S3CoreStorageProviderTests()
    {
        _fakeS3 = A.Fake<IAmazonS3>();
        var factory = new TestClientFactory(_fakeS3);
        _provider = new TestStorageProvider(factory, new S3CoreOptions());
    }

    public async ValueTask DisposeAsync()
    {
        await _provider.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    private static StorageUri Uri(string bucket = "my-bucket", string key = "my-key") => StorageUri.Parse($"s3://{bucket}/{key}");

    // ── Constructor ───────────────────────────────────────────────────────

    [Fact]
    public void Constructor_WithNullClientFactory_ThrowsArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => new TestStorageProvider(null!, new S3CoreOptions()));
        ex.ParamName.Should().Be("clientFactory");
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        var factory = new TestClientFactory(_fakeS3);
        var ex = Assert.Throws<ArgumentNullException>(() => new TestStorageProvider(factory, null!));
        ex.ParamName.Should().Be("options");
    }

    // ── Name / Schemes / Capabilities ─────────────────────────────────────

    [Fact]
    public void Schemes_ContainsS3()
    {
        _provider.Schemes.Should().ContainSingle().Which.Should().Be(StorageScheme.S3);
    }

    [Fact]
    public void Capabilities_DeclaresObjectStoreSet()
    {
        _provider.Capabilities.Should().Be(
            StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move);
    }

    [Fact]
    public void Capabilities_DoesNotDeclareAtomicMoveHierarchyOrConditionalWrite()
    {
        _provider.Capabilities.Should().NotHaveFlag(StorageCapabilities.AtomicMove);
        _provider.Capabilities.Should().NotHaveFlag(StorageCapabilities.Hierarchy);
        _provider.Capabilities.Should().NotHaveFlag(StorageCapabilities.ConditionalWrite);
    }

    // ── ExistsAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task ExistsAsync_WhenObjectFound_ReturnsTrue()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new GetObjectMetadataResponse()));

        var result = await _provider.ExistsAsync(Uri());

        result.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_WhenObjectNotFound_ReturnsFalse()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("Not found") { StatusCode = HttpStatusCode.NotFound });

        var result = await _provider.ExistsAsync(Uri());

        result.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_WithNullUri_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.ExistsAsync(null!));
    }

    [Fact]
    public async Task ExistsAsync_WithAccessDenied_ThrowsUnauthorizedAccessException()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("Access denied") { ErrorCode = "AccessDenied" });

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.ExistsAsync(Uri()));
    }

    // ── GetMetadataAsync ──────────────────────────────────────────────────

    [Fact]
    public async Task GetMetadataAsync_WhenObjectFound_ReturnsMetadata()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new GetObjectMetadataResponse
            {
                ContentLength = 1024,
                LastModified = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                ETag = "\"abc123\"",
            }));

        var result = await _provider.GetMetadataAsync(Uri());

        result.Should().NotBeNull();
        result!.Size.Should().Be(1024);
        result.ETag.Should().Be("\"abc123\"");
    }

    [Fact]
    public async Task GetMetadataAsync_WhenNotFound_ReturnsNull()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("Not found") { StatusCode = HttpStatusCode.NotFound });

        var result = await _provider.GetMetadataAsync(Uri());

        result.Should().BeNull();
    }

    [Fact]
    public async Task GetMetadataAsync_WithNullUri_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.GetMetadataAsync(null!));
    }

    // ── OpenReadAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task OpenReadAsync_ReturnsReadableStream()
    {
        var response = new GetObjectResponse
        {
            ResponseStream = new MemoryStream(new byte[] { 1, 2, 3 }),
        };

        A.CallTo(() => _fakeS3.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(response));

        var stream = await _provider.OpenReadAsync(Uri());

        stream.Should().NotBeNull();
        stream.CanRead.Should().BeTrue();
    }

    [Fact]
    public async Task OpenReadAsync_WithNullUri_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenReadAsync(null!));
    }

    [Fact]
    public async Task OpenReadAsync_WhenNotFound_ThrowsFileNotFoundException()
    {
        A.CallTo(() => _fakeS3.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("Not found") { ErrorCode = "NoSuchBucket" });

        await Assert.ThrowsAsync<FileNotFoundException>(() => _provider.OpenReadAsync(Uri()));
    }

    // ── OpenWriteAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task OpenWriteAsync_ReturnsWritableStream()
    {
        var stream = await _provider.OpenWriteAsync(Uri());

        stream.Should().NotBeNull();
        stream.CanWrite.Should().BeTrue();
    }

    [Fact]
    public async Task OpenWriteAsync_WithNullUri_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenWriteAsync(null!));
    }

    // ── GetBucketAndKey (static, via CanHandle) ───────────────────────────

    [Fact]
    public async Task ExistsAsync_WithUriMissingBucket_ThrowsArgumentException()
    {
        // A URI with an empty host component is the way to get an empty bucket.
        // StorageUri.Parse("s3:///key") gives host="" which triggers the guard.
        var uri = StorageUri.Parse("s3:///some-key");

        await Assert.ThrowsAsync<ArgumentException>(() => _provider.ExistsAsync(uri));
    }

    // ── TranslateS3Exception coverage ────────────────────────────────────

    [Theory]
    [InlineData("AccessDenied", typeof(UnauthorizedAccessException))]
    [InlineData("InvalidAccessKeyId", typeof(UnauthorizedAccessException))]
    [InlineData("SignatureDoesNotMatch", typeof(UnauthorizedAccessException))]
    [InlineData("InvalidBucketName", typeof(ArgumentException))]
    [InlineData("InvalidKey", typeof(ArgumentException))]
    [InlineData("NoSuchBucket", typeof(FileNotFoundException))]
    [InlineData("NotFound", typeof(FileNotFoundException))]
    [InlineData("InternalError", typeof(IOException))]
    public async Task ExistsAsync_MapsS3ErrorCodes_ToExpectedExceptionTypes(string errorCode, Type expectedType)
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("error") { ErrorCode = errorCode });

        var act = async () => await _provider.ExistsAsync(Uri());

        await act.Should().ThrowAsync<Exception>()
            .Where(e => e.GetType() == expectedType || expectedType.IsAssignableFrom(e.GetType()));
    }

    [Theory]
    [InlineData("NoSuchKey", HttpStatusCode.NotFound, typeof(FileNotFoundException))]
    [InlineData("NoSuchBucket", HttpStatusCode.NotFound, typeof(FileNotFoundException))]
    [InlineData("NotFound", HttpStatusCode.NotFound, typeof(FileNotFoundException))]
    [InlineData("AccessDenied", HttpStatusCode.Forbidden, typeof(UnauthorizedAccessException))]
    [InlineData(null, HttpStatusCode.Forbidden, typeof(UnauthorizedAccessException))]
    [InlineData(null, HttpStatusCode.Unauthorized, typeof(UnauthorizedAccessException))]
    [InlineData("InvalidBucketName", HttpStatusCode.BadRequest, typeof(ArgumentException))]
    [InlineData("NoSuchKey", (HttpStatusCode)0, typeof(FileNotFoundException))]
    [InlineData("InternalError", HttpStatusCode.InternalServerError, typeof(IOException))]
    public void Translate_NoSuchKey_IsFileNotFound(string? errorCode, HttpStatusCode status, Type expectedType)
    {
        var ex = new AmazonS3Exception("error") { ErrorCode = errorCode, StatusCode = status };

        S3Errors.Translate(ex, "bucket", "key").Should().BeOfType(expectedType);
    }

    [Fact]
    public void Translate_CopiesExceptionData_AndKeepsInnerException()
    {
        var ex = new AmazonS3Exception("error") { ErrorCode = "InternalError", StatusCode = HttpStatusCode.InternalServerError };
        ex.Data["retried"] = true;

        var translated = S3Errors.Translate(ex, "bucket", "key");

        translated.InnerException.Should().BeSameAs(ex);
        translated.Data["retried"].Should().Be(true);
    }

    [Fact]
    public async Task OpenReadAsync_MissingKey_ThrowsFileNotFound()
    {
        A.CallTo(() => _fakeS3.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("The specified key does not exist.") { ErrorCode = "NoSuchKey", StatusCode = System.Net.HttpStatusCode.NotFound });

        var act = async () => await _provider.OpenReadAsync(Uri());

        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    // ── DeleteAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task DeleteAsync_MissingKey_Succeeds()
    {
        // S3 reports 204 for a missing key.
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new DeleteObjectResponse()));

        await _provider.DeleteAsync(Uri());

        A.CallTo(() => _fakeS3.DeleteObjectAsync(
                A<DeleteObjectRequest>.That.Matches(r => r.BucketName == "my-bucket" && r.Key == "my-key"), A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DeleteAsync_MissingBucket_Succeeds()
    {
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("no bucket") { ErrorCode = "NoSuchBucket", StatusCode = HttpStatusCode.NotFound });

        await _provider.DeleteAsync(Uri());
    }

    [Fact]
    public async Task DeleteAsync_AccessDenied_ThrowsUnauthorizedAccessException()
    {
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("denied") { ErrorCode = "AccessDenied", StatusCode = HttpStatusCode.Forbidden });

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.DeleteAsync(Uri()));
    }

    // ── MoveAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task MoveAsync_CopiesThenDeletes()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new GetObjectMetadataResponse { ContentLength = 10 }));
        A.CallTo(() => _fakeS3.CopyObjectAsync(A<CopyObjectRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new CopyObjectResponse()));
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new DeleteObjectResponse()));

        await _provider.MoveAsync(Uri(key: "from"), Uri("other-bucket", "to"));

        A.CallTo(() => _fakeS3.CopyObjectAsync(
                A<CopyObjectRequest>.That.Matches(r =>
                    r.SourceBucket == "my-bucket" && r.SourceKey == "from" && r.DestinationBucket == "other-bucket" && r.DestinationKey == "to"),
                A<CancellationToken>._))
            .MustHaveHappenedOnceExactly()
            .Then(A.CallTo(() => _fakeS3.DeleteObjectAsync(
                    A<DeleteObjectRequest>.That.Matches(r => r.BucketName == "my-bucket" && r.Key == "from"), A<CancellationToken>._))
                .MustHaveHappenedOnceExactly());
    }

    [Fact]
    public async Task MoveAsync_MissingSource_ThrowsFileNotFoundAndDoesNotCopy()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("missing") { StatusCode = HttpStatusCode.NotFound });

        await Assert.ThrowsAsync<FileNotFoundException>(() => _provider.MoveAsync(Uri(key: "from"), Uri(key: "to")));

        A.CallTo(() => _fakeS3.CopyObjectAsync(A<CopyObjectRequest>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task MoveAsync_SameSourceAndDestination_DoesNothing()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new GetObjectMetadataResponse { ContentLength = 10 }));

        await _provider.MoveAsync(Uri(key: "same"), Uri(key: "same"));

        A.CallTo(() => _fakeS3.CopyObjectAsync(A<CopyObjectRequest>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task MoveAsync_ObjectOverFiveGiB_UsesMultipartCopy()
    {
        const long size = S3CoreStorageProvider.MaxSingleCopyBytes + 1;

        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new GetObjectMetadataResponse { ContentLength = size }));
        A.CallTo(() => _fakeS3.InitiateMultipartUploadAsync(A<InitiateMultipartUploadRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new InitiateMultipartUploadResponse { UploadId = "u1" }));
        A.CallTo(() => _fakeS3.CopyPartAsync(A<CopyPartRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new CopyPartResponse { ETag = "\"e\"" }));
        A.CallTo(() => _fakeS3.CompleteMultipartUploadAsync(A<CompleteMultipartUploadRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new CompleteMultipartUploadResponse()));
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new DeleteObjectResponse()));

        await _provider.MoveAsync(Uri(key: "big"), Uri(key: "bigger"));

        A.CallTo(() => _fakeS3.CopyObjectAsync(A<CopyObjectRequest>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => _fakeS3.CopyPartAsync(A<CopyPartRequest>._, A<CancellationToken>._)).MustHaveHappened(11, Times.Exactly);
        A.CallTo(() => _fakeS3.CompleteMultipartUploadAsync(
                A<CompleteMultipartUploadRequest>.That.Matches(r => r.PartETags.Count == 11), A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task MoveAsync_MultipartCopyFails_AbortsUploadAndKeepsSource()
    {
        A.CallTo(() => _fakeS3.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new GetObjectMetadataResponse { ContentLength = S3CoreStorageProvider.MaxSingleCopyBytes + 1 }));
        A.CallTo(() => _fakeS3.InitiateMultipartUploadAsync(A<InitiateMultipartUploadRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(new InitiateMultipartUploadResponse { UploadId = "u1" }));
        A.CallTo(() => _fakeS3.CopyPartAsync(A<CopyPartRequest>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("boom") { StatusCode = HttpStatusCode.InternalServerError });

        await Assert.ThrowsAsync<IOException>(() => _provider.MoveAsync(Uri(key: "big"), Uri(key: "bigger")));

        A.CallTo(() => _fakeS3.AbortMultipartUploadAsync(A<AbortMultipartUploadRequest>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => _fakeS3.DeleteObjectAsync(A<DeleteObjectRequest>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    // ── ListAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ListAsync_NonRecursive_YieldsFilesAndPrefixEntries_WithNullUnknowns()
    {
        ListObjectsV2Request? captured = null;

        A.CallTo(() => _fakeS3.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .Invokes((ListObjectsV2Request r, CancellationToken _) => captured = r)
            .Returns(Task.FromResult(new ListObjectsV2Response
            {
                S3Objects = [new S3Object { Key = "logs/a.txt", Size = 3, LastModified = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) }],
                CommonPrefixes = ["logs/sub/"],
            }));

        var items = await _provider.ListAsync(StorageUri.Parse("s3://my-bucket/logs?region=eu-west-1")).ToListAsync();

        captured!.Prefix.Should().Be("logs/");
        captured.Delimiter.Should().Be("/");
        items.Should().HaveCount(2);

        var file = items.Single(i => !i.IsDirectory);
        file.Uri.Path.Should().Be("/logs/a.txt");
        file.Uri.Parameters["region"].Should().Be("eu-west-1");
        file.Size.Should().Be(3);
        file.LastModified.Should().NotBeNull();

        var dir = items.Single(i => i.IsDirectory);
        dir.Uri.Path.Should().Be("/logs/sub/");
        dir.Size.Should().BeNull();
        dir.LastModified.Should().BeNull();
    }

    [Fact]
    public async Task ListAsync_Recursive_UsesNullDelimiter_AndYieldsNoDirectories()
    {
        ListObjectsV2Request? captured = null;

        A.CallTo(() => _fakeS3.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .Invokes((ListObjectsV2Request r, CancellationToken _) => captured = r)
            .Returns(Task.FromResult(new ListObjectsV2Response
            {
                S3Objects =
                [
                    new S3Object { Key = "logs/", Size = 0 },
                    new S3Object { Key = "logs/a.txt", Size = 1 },
                    new S3Object { Key = "logs/sub/b.txt" },
                ],
            }));

        var items = await _provider.ListAsync(StorageUri.Parse("s3://my-bucket/logs/"), true).ToListAsync();

        captured!.Delimiter.Should().BeNull();
        items.Select(i => i.Uri.Path).Should().Equal("/logs/a.txt", "/logs/sub/b.txt");
        items.Should().OnlyContain(i => !i.IsDirectory);
        items[1].Size.Should().BeNull();
        items[1].LastModified.Should().BeNull();
    }

    [Fact]
    public async Task ListAsync_DirectoryWithoutTrailingSlash_DoesNotMatchSiblingPrefix()
    {
        ListObjectsV2Request? captured = null;

        A.CallTo(() => _fakeS3.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .Invokes((ListObjectsV2Request r, CancellationToken _) => captured = r)
            .Returns(Task.FromResult(new ListObjectsV2Response()));

        _ = await _provider.ListAsync(StorageUri.Parse("s3://my-bucket/logs")).ToListAsync();

        captured!.Prefix.Should().Be("logs/");
    }

    [Fact]
    public async Task ListAsync_MissingBucket_YieldsNothing()
    {
        A.CallTo(() => _fakeS3.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .Throws(new AmazonS3Exception("no bucket") { ErrorCode = "NoSuchBucket", StatusCode = HttpStatusCode.NotFound });

        var items = await _provider.ListAsync(StorageUri.Parse("s3://missing/")).ToListAsync();

        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_FollowsContinuationTokens()
    {
        A.CallTo(() => _fakeS3.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ReturnsNextFromSequence(
                Task.FromResult(new ListObjectsV2Response
                {
                    S3Objects = [new S3Object { Key = "a", Size = 1 }],
                    IsTruncated = true,
                    NextContinuationToken = "t1",
                }),
                Task.FromResult(new ListObjectsV2Response { S3Objects = [new S3Object { Key = "b", Size = 1 }] }));

        var items = await _provider.ListAsync(StorageUri.Parse("s3://my-bucket/"), true).ToListAsync();

        items.Should().HaveCount(2);
    }

    // ── Read / write details ──────────────────────────────────────────────

    [Fact]
    public async Task OpenReadAsync_ReportsLengthFromContentLength()
    {
        var response = new GetObjectResponse { ResponseStream = new MemoryStream(new byte[] { 1, 2, 3 }), ContentLength = 42 };

        A.CallTo(() => _fakeS3.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .Returns(Task.FromResult(response));

        await using var stream = await _provider.OpenReadAsync(Uri());

        stream.Length.Should().Be(42);
    }

    [Fact]
    public async Task OpenWriteAsync_OptionsContentType_OverridesUriParameter()
    {
        PutObjectRequest? captured = null;

        A.CallTo(() => _fakeS3.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Invokes((PutObjectRequest r, CancellationToken _) => captured = r)
            .Returns(Task.FromResult(new PutObjectResponse()));

        await using (var stream = await _provider.OpenWriteAsync(
                         StorageUri.Parse("s3://my-bucket/k?contentType=text/plain"),
                         new StorageWriteOptions { ContentType = "application/json" }))
        {
            await stream.WriteAsync(new byte[] { 1 });
            await stream.CommitAsync();
        }

        captured.Should().NotBeNull();
        captured!.ContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task OpenWriteAsync_UriContentTypeParameter_IsFallback()
    {
        PutObjectRequest? captured = null;

        A.CallTo(() => _fakeS3.PutObjectAsync(A<PutObjectRequest>._, A<CancellationToken>._))
            .Invokes((PutObjectRequest r, CancellationToken _) => captured = r)
            .Returns(Task.FromResult(new PutObjectResponse()));

        await using (var stream = await _provider.OpenWriteAsync(StorageUri.Parse("s3://my-bucket/k?contentType=text/plain")))
        {
            await stream.WriteAsync(new byte[] { 1 });
            await stream.CommitAsync();
        }

        captured!.ContentType.Should().Be("text/plain");
    }

    [Fact]
    public async Task DisposeAsync_DisposesCachedClients()
    {
        _ = await _provider.ExistsAsync(Uri());

        await _provider.DisposeAsync();

        A.CallTo(() => _fakeS3.Dispose()).MustHaveHappened();
    }

    // ── Test doubles ──────────────────────────────────────────────────────

    private sealed class TestClientFactory : S3ClientFactoryBase
    {
        private readonly IAmazonS3 _client;

        public TestClientFactory(IAmazonS3 client)
        {
            _client = client;
        }

        protected override IAmazonS3 CreateClient(StorageUri uri) => _client;
    }

    private sealed class TestStorageProvider : S3CoreStorageProvider
    {
        public TestStorageProvider(S3ClientFactoryBase factory, S3CoreOptions options)
            : base(factory, options)
        {
        }

        public override string Name => "Test S3";

        public override IReadOnlyList<StorageScheme> Schemes { get; } = [StorageScheme.S3];
    }
}
