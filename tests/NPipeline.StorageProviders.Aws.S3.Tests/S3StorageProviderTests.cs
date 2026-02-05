using System.Net;
using Amazon.S3;
using Amazon.S3.Model;
using FakeItEasy;
using FluentAssertions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Aws.S3.Tests;

public class S3StorageProviderTests
{
    private readonly S3ClientFactory _fakeClientFactory;
    private readonly IAmazonS3 _fakeS3Client;
    private readonly S3StorageProviderOptions _options;
    private readonly S3StorageProvider _provider;

    public S3StorageProviderTests()
    {
        _fakeClientFactory = A.Fake<S3ClientFactory>(c => c
            .WithArgumentsForConstructor(new object[] { new S3StorageProviderOptions() })
            .CallsBaseMethods());

        _fakeS3Client = A.Fake<IAmazonS3>();
        _options = new S3StorageProviderOptions();
        _provider = new S3StorageProvider(_fakeClientFactory, _options);
    }

    [Fact]
    public void Constructor_WithNullClientFactory_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new S3StorageProvider(null!, _options));
        exception.ParamName.Should().Be("clientFactory");
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new S3StorageProvider(_fakeClientFactory, null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesProvider()
    {
        // Act & Assert
        _provider.Should().NotBeNull();
    }

    [Fact]
    public void Scheme_ReturnsS3()
    {
        // Act & Assert
        _provider.Scheme.Should().Be(StorageScheme.S3);
    }

    [Fact]
    public void CanHandle_WithS3Scheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://bucket/key");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void CanHandle_WithFileScheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("file:///path/to/file");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => _provider.CanHandle(null!));
    }

    [Fact]
    public void CanHandle_WithAzureScheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://container/blob");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void CanHandle_WithValidS3Uri_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://bucket/key");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task OpenReadAsync_WithValidUri_ReturnsStream()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(call => Task.FromResult(_fakeS3Client));

        var responseStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ReturnsLazily(call => Task.FromResult(new GetObjectResponse { ResponseStream = responseStream }));

        // Act
        var stream = await _provider.OpenReadAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeAssignableTo<Stream>();
    }

    [Fact]
    public async Task OpenReadAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenReadAsync(null!));
    }

    [Fact]
    public async Task OpenReadAsync_WithNotFound_ThrowsFileNotFoundException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Not found")
        {
            ErrorCode = "NoSuchBucket",
        };

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithAccessDenied_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Access denied")
        {
            ErrorCode = "AccessDenied",
        };

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithInvalidAccessKeyId_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Invalid access key")
        {
            ErrorCode = "InvalidAccessKeyId",
        };

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithSignatureDoesNotMatch_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Signature does not match")
        {
            ErrorCode = "SignatureDoesNotMatch",
        };

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithInvalidBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Invalid bucket name")
        {
            ErrorCode = "InvalidBucketName",
        };

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithInvalidKey_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Invalid key")
        {
            ErrorCode = "InvalidKey",
        };

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithGenericError_ThrowsIOException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Generic error")
        {
            ErrorCode = "InternalError",
        };

        A.CallTo(() => _fakeS3Client.GetObjectAsync(A<GetObjectRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenWriteAsync_WithValidUri_ReturnsS3WriteStream()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        // Act
        var stream = await _provider.OpenWriteAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeOfType<S3WriteStream>();
    }

    [Fact]
    public async Task OpenWriteAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenWriteAsync(null!));
    }

    [Fact]
    public async Task OpenWriteAsync_WithContentTypeInUri_PassesContentTypeToStream()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key?contentType=application/json");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        // Act
        var stream = await _provider.OpenWriteAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeOfType<S3WriteStream>();
    }

    [Fact]
    public async Task ExistsAsync_WithExistingObject_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(new GetObjectMetadataResponse()));

        // Act
        var result = await _provider.ExistsAsync(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_WithNonExistingObject_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Not found")
        {
            StatusCode = HttpStatusCode.NotFound,
        };

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act
        var result = await _provider.ExistsAsync(uri);

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.ExistsAsync(null!));
    }

    [Fact]
    public async Task ExistsAsync_WithAccessDenied_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Access denied")
        {
            ErrorCode = "AccessDenied",
        };

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.ExistsAsync(uri));
    }

    [Fact]
    public async Task ExistsAsync_WithInvalidBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Invalid bucket name")
        {
            ErrorCode = "InvalidBucketName",
        };

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.ExistsAsync(uri));
    }

    [Fact]
    public async Task DeleteAsync_AlwaysThrowsNotSupportedException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act & Assert
        await Assert.ThrowsAsync<NotSupportedException>(() => _provider.DeleteAsync(uri));
    }

    [Fact]
    public async Task ListAsync_WithRecursiveTrue_ReturnsAllObjects()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(new ListObjectsV2Response
            {
                S3Objects = new List<S3Object>
                {
                    new() { Key = "prefix/file1.txt", Size = 100, LastModified = DateTime.UtcNow },
                    new() { Key = "prefix/subdir/file2.txt", Size = 200, LastModified = DateTime.UtcNow },
                },
                CommonPrefixes = new List<string>(),
            }));

        // Act
        var items = await _provider.ListAsync(uri, true).ToListAsync();

        // Assert
        items.Should().HaveCount(2);
        items.All(i => !i.IsDirectory).Should().BeTrue();
    }

    [Fact]
    public async Task ListAsync_WithRecursiveFalse_ReturnsDirectChildrenOnly()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(new ListObjectsV2Response
            {
                S3Objects = new List<S3Object>
                {
                    new() { Key = "prefix/file1.txt", Size = 100, LastModified = DateTime.UtcNow },
                },
                CommonPrefixes = new List<string> { "prefix/subdir/" },
            }));

        // Act
        var items = await _provider.ListAsync(uri).ToListAsync();

        // Assert
        items.Should().HaveCount(2);
        items.Count(i => i.IsDirectory).Should().Be(1);
        items.Count(i => !i.IsDirectory).Should().Be(1);
    }

    [Fact]
    public async Task ListAsync_WithNullPrefix_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await _provider.ListAsync(null!).ToListAsync());
    }

    [Fact]
    public async Task ListAsync_WithPagination_HandlesContinuationToken()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var callCount = 0;

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;

                if (callCount == 1)
                {
                    return Task.FromResult(new ListObjectsV2Response
                    {
                        S3Objects = new List<S3Object>
                        {
                            new() { Key = "prefix/file1.txt", Size = 100, LastModified = DateTime.UtcNow },
                        },
                        NextContinuationToken = "token",
                        CommonPrefixes = new List<string>(),
                    });
                }

                return Task.FromResult(new ListObjectsV2Response
                {
                    S3Objects = new List<S3Object>
                    {
                        new() { Key = "prefix/file2.txt", Size = 200, LastModified = DateTime.UtcNow },
                    },
                    CommonPrefixes = new List<string>(),
                });
            });

        // Act
        var items = await _provider.ListAsync(uri, true).ToListAsync();

        // Assert
        items.Should().HaveCount(2);

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .MustHaveHappened(2, Times.Exactly);
    }

    [Fact]
    public async Task ListAsync_WithNonExistentBucket_ReturnsEmpty()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Bucket not found")
        {
            StatusCode = HttpStatusCode.NotFound,
        };

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act
        var items = await _provider.ListAsync(uri, true).ToListAsync();

        // Assert
        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_WithAccessDenied_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Access denied")
        {
            ErrorCode = "AccessDenied",
        };

        A.CallTo(() => _fakeS3Client.ListObjectsV2Async(A<ListObjectsV2Request>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await _provider.ListAsync(uri).ToListAsync());
    }

    [Fact]
    public async Task GetMetadataAsync_WithExistingObject_ReturnsMetadata()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var response = new GetObjectMetadataResponse
        {
            ContentLength = 1024,
            LastModified = DateTime.UtcNow,
            ETag = "\"abc123\"",
        };

        response.Headers.ContentType = "application/json";
        response.Metadata["custom-key"] = "custom-value";

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(response));

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(1024);
        metadata.ContentType.Should().Be("application/json");
        metadata.ETag.Should().Be("\"abc123\"");
        metadata.IsDirectory.Should().BeFalse();
        metadata.CustomMetadata.Should().ContainKey("x-amz-meta-custom-key").WhoseValue.Should().Be("custom-value");
    }

    [Fact]
    public async Task GetMetadataAsync_WithNonExistingObject_ReturnsNull()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Not found")
        {
            StatusCode = HttpStatusCode.NotFound,
        };

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().BeNull();
    }

    [Fact]
    public async Task GetMetadataAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.GetMetadataAsync(null!));
    }

    [Fact]
    public async Task GetMetadataAsync_WithAccessDenied_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var s3Exception = new AmazonS3Exception("Access denied")
        {
            ErrorCode = "AccessDenied",
        };

        A.CallTo(() => _fakeS3Client.GetObjectMetadataAsync(A<GetObjectMetadataRequest>._, A<CancellationToken>._))
            .ThrowsAsync(s3Exception);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.GetMetadataAsync(uri));
    }

    [Fact]
    public void GetMetadata_ReturnsCorrectProviderMetadata()
    {
        // Act
        var metadata = _provider.GetMetadata();

        // Assert
        metadata.Should().NotBeNull();
        metadata.Name.Should().Be("AWS S3");
        metadata.SupportedSchemes.Should().Contain("s3");
        metadata.SupportsRead.Should().BeTrue();
        metadata.SupportsWrite.Should().BeTrue();
        metadata.SupportsDelete.Should().BeFalse();
        metadata.SupportsListing.Should().BeTrue();
        metadata.SupportsMetadata.Should().BeTrue();
        metadata.SupportsHierarchy.Should().BeFalse();
        metadata.Capabilities["multipartUploadThresholdBytes"].Should().Be(64 * 1024 * 1024);
        metadata.Capabilities["supportsPathStyle"].Should().Be(true);
        metadata.Capabilities["supportsServiceUrl"].Should().Be(true);
    }

    [Fact]
    public void GetMetadata_WithCustomOptions_ReturnsCorrectCapabilities()
    {
        // Arrange
        var customOptions = new S3StorageProviderOptions
        {
            MultipartUploadThresholdBytes = 128 * 1024 * 1024,
        };

        var customProvider = new S3StorageProvider(_fakeClientFactory, customOptions);

        // Act
        var metadata = customProvider.GetMetadata();

        // Assert
        metadata.Capabilities["multipartUploadThresholdBytes"].Should().Be(128 * 1024 * 1024);
    }

    [Fact]
    public async Task OpenReadAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3:///test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenWriteAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3:///test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenWriteAsync(uri));
    }

    [Fact]
    public async Task ExistsAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3:///test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.ExistsAsync(uri));
    }

    [Fact]
    public async Task GetMetadataAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3:///test-key");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.GetMetadataAsync(uri));
    }

    [Fact]
    public async Task ListAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("s3:///prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () => await _provider.ListAsync(uri).ToListAsync());
    }

    [Theory]
    [InlineData("s3://bucket/key", "bucket", "key")]
    [InlineData("s3://bucket/path/to/key", "bucket", "path/to/key")]
    [InlineData("s3://bucket/", "bucket", "")]
    [InlineData("s3://bucket", "bucket", "")]
    public async Task OpenReadAsync_ParsesBucketAndKeyCorrectly(string uriString, string expectedBucket, string expectedKey)
    {
        // Arrange
        var uri = StorageUri.Parse(uriString);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeS3Client));

        var responseStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });

        A.CallTo(() => _fakeS3Client.GetObjectAsync(
                A<GetObjectRequest>.That.Matches(r => r.BucketName == expectedBucket && r.Key == expectedKey),
                A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(new GetObjectResponse { ResponseStream = responseStream }));

        // Act
        var stream = await _provider.OpenReadAsync(uri);

        // Assert
        stream.Should().NotBeNull();
    }
}
