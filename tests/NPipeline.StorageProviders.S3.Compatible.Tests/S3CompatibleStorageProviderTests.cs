using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.S3.Compatible.Tests;

public class S3CompatibleStorageProviderTests
{
    private static readonly S3CompatibleStorageProviderOptions DefaultOptions = new()
    {
        ServiceUrl = new Uri("http://localhost:9000"),
        AccessKey = "test-access-key",
        SecretKey = "test-secret-key",
    };

    private readonly S3CompatibleClientFactory _fakeClientFactory;
    private readonly IAmazonS3 _fakeS3Client;
    private readonly S3CompatibleStorageProviderOptions _options;
    private readonly S3CompatibleStorageProvider _provider;

    public S3CompatibleStorageProviderTests()
    {
        _fakeClientFactory = A.Fake<S3CompatibleClientFactory>(c => c
            .WithArgumentsForConstructor(new object[] { DefaultOptions })
            .CallsBaseMethods());

        _fakeS3Client = A.Fake<IAmazonS3>();
        _options = DefaultOptions;
        _provider = new S3CompatibleStorageProvider(_fakeClientFactory, _options);
    }

    // ── Constructor ───────────────────────────────────────────────────────

    [Fact]
    public void Constructor_WithNullClientFactory_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => new S3CompatibleStorageProvider(null!, _options));
        exception.ParamName.Should().Be("clientFactory");
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => new S3CompatibleStorageProvider(_fakeClientFactory, null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesProvider()
    {
        _provider.Should().NotBeNull();
    }

    // ── Scheme / CanHandle ────────────────────────────────────────────────

    [Fact]
    public void Scheme_ReturnsS3()
    {
        _provider.Scheme.Should().Be(StorageScheme.S3);
    }

    [Fact]
    public void CanHandle_WithS3Scheme_ReturnsTrue()
    {
        _provider.CanHandle(StorageUri.Parse("s3://bucket/key")).Should().BeTrue();
    }

    [Fact]
    public void CanHandle_WithFileScheme_ReturnsFalse()
    {
        _provider.CanHandle(StorageUri.Parse("file:///path/to/file")).Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WithAzureScheme_ReturnsFalse()
    {
        _provider.CanHandle(StorageUri.Parse("azure://container/blob")).Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WithNullUri_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => _provider.CanHandle(null!));
    }

    // ── GetMetadata ───────────────────────────────────────────────────────

    [Fact]
    public void GetMetadata_ReturnsCompatibleProviderName()
    {
        var metadata = _provider.GetMetadata();
        metadata.Name.Should().Be("S3-Compatible");
    }

    [Fact]
    public void GetMetadata_SupportsS3Scheme()
    {
        var metadata = _provider.GetMetadata();
        metadata.SupportedSchemes.Should().Contain("s3");
    }

    [Fact]
    public void GetMetadata_SupportsReadWriteListMetadata()
    {
        var metadata = _provider.GetMetadata();
        metadata.SupportsRead.Should().BeTrue();
        metadata.SupportsWrite.Should().BeTrue();
        metadata.SupportsListing.Should().BeTrue();
        metadata.SupportsMetadata.Should().BeTrue();
    }

    [Fact]
    public void GetMetadata_DoesNotSupportHierarchy()
    {
        _provider.GetMetadata().SupportsHierarchy.Should().BeFalse();
    }

    [Fact]
    public void GetMetadata_CapabilitiesContainEndpoint()
    {
        var metadata = _provider.GetMetadata();
        metadata.Capabilities.Should().ContainKey("endpoint");
    }

    [Fact]
    public void GetMetadata_CapabilitiesContainForcePathStyle()
    {
        var metadata = _provider.GetMetadata();
        metadata.Capabilities.Should().ContainKey("forcePathStyle");
    }

    [Fact]
    public void GetMetadata_CapabilitiesContainMultipartThreshold()
    {
        var metadata = _provider.GetMetadata();
        metadata.Capabilities.Should().ContainKey("multipartUploadThresholdBytes");
        metadata.Capabilities["multipartUploadThresholdBytes"].Should().Be(64L * 1024 * 1024);
    }

    [Fact]
    public void GetMetadata_WithCustomThreshold_ReturnsCorrectCapability()
    {
        var customOptions = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "key",
            SecretKey = "secret",
            MultipartUploadThresholdBytes = 128 * 1024 * 1024,
        };

        var customProvider = new S3CompatibleStorageProvider(_fakeClientFactory, customOptions);

        customProvider.GetMetadata().Capabilities["multipartUploadThresholdBytes"]
            .Should().Be(128L * 1024 * 1024);
    }

    // ── OpenReadAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task OpenReadAsync_WithNullUri_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenReadAsync(null!));
    }

    [Fact]
    public async Task OpenReadAsync_WithValidUri_ReturnsStream()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        var responseStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ReturnsLazily(_ => Task.FromResult(new GetObjectResponse { ResponseStream = responseStream }));

        var stream = await _provider.OpenReadAsync(uri);

        stream.Should().NotBeNull().And.BeAssignableTo<Stream>();
    }

    [Fact]
    public async Task OpenReadAsync_WithNotFound_ThrowsFileNotFoundException()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonS3Exception("Not found") { ErrorCode = "NoSuchBucket" });

        await Assert.ThrowsAsync<FileNotFoundException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithAccessDenied_ThrowsUnauthorizedAccessException()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonS3Exception("Access denied") { ErrorCode = "AccessDenied" });

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithSignatureDoesNotMatch_ThrowsUnauthorizedAccessException()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonS3Exception("Signature mismatch") { ErrorCode = "SignatureDoesNotMatch" });

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithInvalidKey_ThrowsArgumentException()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonS3Exception("Invalid key") { ErrorCode = "InvalidKey" });

        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithGenericError_ThrowsIOException()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonS3Exception("Internal error") { ErrorCode = "InternalError" });

        await Assert.ThrowsAsync<IOException>(() => _provider.OpenReadAsync(uri));
    }

    // ── OpenWriteAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task OpenWriteAsync_WithNullUri_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenWriteAsync(null!));
    }

    [Fact]
    public async Task OpenWriteAsync_WithValidUri_ReturnsS3WriteStream()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        var stream = await _provider.OpenWriteAsync(uri);

        stream.Should().NotBeNull().And.BeOfType<S3WriteStream>();
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task OpenWriteAsync_WithContentTypeInUri_ReturnsStream()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key?contentType=application/json");
        ConfigureClientFactory(uri);

        var stream = await _provider.OpenWriteAsync(uri);

        stream.Should().NotBeNull().And.BeOfType<S3WriteStream>();
        await stream.DisposeAsync();
    }

    // ── ExistsAsync ───────────────────────────────────────────────────────

    [Fact]
    public async Task ExistsAsync_WithNullUri_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.ExistsAsync(null!));
    }

    [Fact]
    public async Task ExistsAsync_WhenObjectExists_ReturnsTrue()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ReturnsLazily(_ => Task.FromResult(new GetObjectMetadataResponse()));

        var result = await _provider.ExistsAsync(uri);

        result.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_WhenObjectNotFound_ReturnsFalse()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        var notFound = new AmazonS3Exception("Not found") { StatusCode = HttpStatusCode.NotFound };

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ThrowsAsync(notFound);

        var result = await _provider.ExistsAsync(uri);

        result.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_WithAccessDenied_ThrowsUnauthorizedAccessException()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ThrowsAsync(new AmazonS3Exception("Access denied") { ErrorCode = "AccessDenied" });

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.ExistsAsync(uri));
    }

    // ── GetMetadataAsync ──────────────────────────────────────────────────

    [Fact]
    public async Task GetMetadataAsync_WithNullUri_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.GetMetadataAsync(null!));
    }

    [Fact]
    public async Task GetMetadataAsync_WhenObjectExists_ReturnsMetadata()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        var response = new GetObjectMetadataResponse
        {
            ContentLength = 1024,
            LastModified = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc),
        };

        response.Headers.ContentType = "text/csv";
        response.ETag = "\"abc123\"";

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ReturnsLazily(_ => Task.FromResult(response));

        var metadata = await _provider.GetMetadataAsync(uri);

        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(1024);
        metadata.ContentType.Should().Be("text/csv");
        metadata.ETag.Should().Be("\"abc123\"");
        metadata.IsDirectory.Should().BeFalse();
    }

    [Fact]
    public async Task GetMetadataAsync_WhenObjectNotFound_ReturnsNull()
    {
        var uri = StorageUri.Parse("s3://test-bucket/test-key");
        ConfigureClientFactory(uri);

        var notFound = new AmazonS3Exception("Not found") { StatusCode = HttpStatusCode.NotFound };

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ThrowsAsync(notFound);

        var metadata = await _provider.GetMetadataAsync(uri);

        metadata.Should().BeNull();
    }

    // ── ListAsync ─────────────────────────────────────────────────────────

    [Fact]
    public void ListAsync_WithNullPrefix_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => _provider.ListAsync(null!));
    }

    [Fact]
    public async Task ListAsync_WithEmptyBucket_YieldsNothing()
    {
        var uri = StorageUri.Parse("s3://test-bucket/");
        ConfigureClientFactory(uri);

        var emptyResponse = new ListObjectsV2Response
        {
            S3Objects = [],
            CommonPrefixes = [],
            IsTruncated = false,
        };

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ReturnsLazily(_ => Task.FromResult(emptyResponse));

        var items = new List<StorageItem>();

        await foreach (var item in _provider.ListAsync(uri))
        {
            items.Add(item);
        }

        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_NonRecursive_YieldsObjectsAndDirectories()
    {
        var uri = StorageUri.Parse("s3://test-bucket/folder/");
        ConfigureClientFactory(uri);

        var s3Object = new S3Object
        {
            Key = "folder/file.csv",
            Size = 512,
            LastModified = DateTime.UtcNow,
        };

        var response = new ListObjectsV2Response
        {
            S3Objects = [s3Object],
            CommonPrefixes = ["folder/sub/"],
            IsTruncated = false,
        };

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ReturnsLazily(_ => Task.FromResult(response));

        var items = new List<StorageItem>();

        await foreach (var item in _provider.ListAsync(uri, false))
        {
            items.Add(item);
        }

        items.Should().HaveCount(2);
        items.Should().ContainSingle(i => !i.IsDirectory && i.Size == 512);
        items.Should().ContainSingle(i => i.IsDirectory);
    }

    [Fact]
    public async Task ListAsync_Recursive_DoesNotYieldDirectories()
    {
        var uri = StorageUri.Parse("s3://test-bucket/");
        ConfigureClientFactory(uri);

        var s3Object = new S3Object
        {
            Key = "folder/file.csv",
            Size = 256,
            LastModified = DateTime.UtcNow,
        };

        var response = new ListObjectsV2Response
        {
            S3Objects = [s3Object],
            CommonPrefixes = [],
            IsTruncated = false,
        };

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ReturnsLazily(_ => Task.FromResult(response));

        var items = new List<StorageItem>();

        await foreach (var item in _provider.ListAsync(uri, true))
        {
            items.Add(item);
        }

        items.Should().HaveCount(1);
        items[0].IsDirectory.Should().BeFalse();
    }

    [Fact]
    public async Task ListAsync_WithBucketNotFound_YieldsEmpty()
    {
        var uri = StorageUri.Parse("s3://test-bucket/");
        ConfigureClientFactory(uri);

        var notFound = new AmazonS3Exception("Not found") { StatusCode = HttpStatusCode.NotFound };

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ThrowsAsync(notFound);

        var items = new List<StorageItem>();

        await foreach (var item in _provider.ListAsync(uri))
        {
            items.Add(item);
        }

        items.Should().BeEmpty();
    }

    // ── Missing bucket in URI ─────────────────────────────────────────────

    [Fact]
    public async Task OpenReadAsync_WithMissingBucket_ThrowsArgumentException()
    {
        var uri = StorageUri.Parse("s3:///some-key");
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private void ConfigureClientFactory(StorageUri uri)
    {
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(_ => Task.FromResult(_fakeS3Client));
    }
}
