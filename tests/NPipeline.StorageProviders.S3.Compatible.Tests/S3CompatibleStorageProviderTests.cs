using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.StorageProviders.Abstractions;
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

    // ── Name / Schemes / Capabilities ─────────────────────────────────────

    [Fact]
    public void Name_IsS3Compatible()
    {
        _provider.Name.Should().Be("S3-Compatible");
    }

    [Fact]
    public void Schemes_DefaultToS3()
    {
        _provider.Schemes.Should().ContainSingle().Which.Should().Be(StorageScheme.S3);
    }

    [Fact]
    public void Schemes_FollowOptionsSchemes()
    {
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "key",
            SecretKey = "secret",
            Schemes = ["minio", "r2"],
        };

        var provider = new S3CompatibleStorageProvider(_fakeClientFactory, options);

        provider.Schemes.Select(s => s.Value).Should().Equal("minio", "r2");
    }

    [Fact]
    public void Capabilities_DeclaresObjectStoreSet()
    {
        _provider.Capabilities.Should().Be(
            StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move);
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

        await foreach (var item in _provider.ListAsync(uri))
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
