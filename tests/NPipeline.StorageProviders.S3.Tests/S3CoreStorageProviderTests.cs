using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.S3.Tests;

public class S3CoreStorageProviderTests
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

    private static StorageUri Uri(string bucket = "my-bucket", string key = "my-key")
    {
        return StorageUri.Parse($"s3://{bucket}/{key}");
    }

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

    // ── Scheme / CanHandle ────────────────────────────────────────────────

    [Fact]
    public void Scheme_ReturnsS3()
    {
        _provider.Scheme.Should().Be(StorageScheme.S3);
    }

    [Fact]
    public void CanHandle_WithS3Uri_ReturnsTrue()
    {
        _provider.CanHandle(Uri()).Should().BeTrue();
    }

    [Fact]
    public void CanHandle_WithFileUri_ReturnsFalse()
    {
        _provider.CanHandle(StorageUri.Parse("file:///tmp/file")).Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WithNullUri_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => _provider.CanHandle(null!));
    }

    // ── GetMetadata (IStorageProviderMetadataProvider) ────────────────────

    [Fact]
    public void GetMetadata_ReturnsNonNullMetadata()
    {
        var meta = _provider.GetMetadata();

        meta.Should().NotBeNull();
    }

    [Fact]
    public void GetMetadata_IncludesS3Scheme()
    {
        var meta = _provider.GetMetadata();

        meta.SupportedSchemes.Should().Contain("s3");
    }

    [Fact]
    public void GetMetadata_ReportsReadWriteAndListingSupport()
    {
        var meta = _provider.GetMetadata();

        meta.SupportsRead.Should().BeTrue();
        meta.SupportsWrite.Should().BeTrue();
        meta.SupportsListing.Should().BeTrue();
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

    // ── Test doubles ──────────────────────────────────────────────────────

    private sealed class TestClientFactory : S3ClientFactoryBase
    {
        private readonly IAmazonS3 _client;

        public TestClientFactory(IAmazonS3 client)
        {
            _client = client;
        }

        protected override IAmazonS3 CreateClient(StorageUri uri)
        {
            return _client;
        }
    }

    private sealed class TestStorageProvider : S3CoreStorageProvider
    {
        public TestStorageProvider(S3ClientFactoryBase factory, S3CoreOptions options)
            : base(factory, options)
        {
        }
    }
}
