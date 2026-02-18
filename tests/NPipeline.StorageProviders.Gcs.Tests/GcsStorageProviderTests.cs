using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using Google;
using Google.Apis.Storage.v1.Data;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Gcs.Tests;

public class GcsStorageProviderTests
{
    private readonly GcsClientFactory _fakeClientFactory;
    private readonly StorageClient _fakeStorageClient;
    private readonly GcsStorageProviderOptions _options;
    private readonly GcsStorageProvider _provider;

    public GcsStorageProviderTests()
    {
        _fakeClientFactory = A.Fake<GcsClientFactory>(c => c
            .WithArgumentsForConstructor(new object[] { new GcsStorageProviderOptions() })
            .CallsBaseMethods());

        _fakeStorageClient = A.Fake<StorageClient>();
        _options = new GcsStorageProviderOptions();
        _provider = new GcsStorageProvider(_fakeClientFactory, _options);
    }

    [Fact]
    public void Constructor_WithNullClientFactory_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new GcsStorageProvider(null!, _options));
        exception.ParamName.Should().Be("clientFactory");
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new GcsStorageProvider(_fakeClientFactory, null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesProvider()
    {
        // Act & Assert
        _provider.Should().NotBeNull();
    }

    [Fact]
    public void Scheme_ReturnsGcs()
    {
        // Act & Assert
        _provider.Scheme.Should().Be(StorageScheme.Gcs);
    }

    [Fact]
    public void CanHandle_WithGcsScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://bucket/object");

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
    public void CanHandle_WithS3Scheme_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("s3://bucket/key");

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
    public void CanHandle_WithUpperCaseGcsScheme_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("GS://bucket/object");

        // Act
        var result = _provider.CanHandle(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task OpenReadAsync_WithValidUri_ReturnsStream()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(call => Task.FromResult(_fakeStorageClient));

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .Invokes(call =>
            {
                var stream = call.GetArgument<Stream>(2)!;
                var data = new byte[] { 1, 2, 3, 4, 5 };
                stream.Write(data, 0, data.Length);
            })
            .ReturnsLazily(call => Task.FromResult(new Google.Apis.Storage.v1.Data.Object()));

        // Act
        var stream = await _provider.OpenReadAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeAssignableTo<Stream>();
        stream.CanRead.Should().BeTrue();
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
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Not found")
        {
            HttpStatusCode = HttpStatusCode.NotFound,
        };

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithUnauthorized_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Unauthorized")
        {
            HttpStatusCode = HttpStatusCode.Unauthorized,
        };

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithForbidden_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Forbidden")
        {
            HttpStatusCode = HttpStatusCode.Forbidden,
        };

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithBadRequest_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Bad request")
        {
            HttpStatusCode = HttpStatusCode.BadRequest,
        };

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithConflict_ThrowsIOException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Conflict")
        {
            HttpStatusCode = HttpStatusCode.Conflict,
        };

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        // Act & Assert
        await Assert.ThrowsAsync<IOException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenReadAsync_WithGenericError_ThrowsGcsStorageException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Internal error")
        {
            HttpStatusCode = HttpStatusCode.InternalServerError,
        };

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        // Act & Assert
        await Assert.ThrowsAsync<GcsStorageException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenWriteAsync_WithValidUri_ReturnsGcsWriteStream()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        // Act
        var stream = await _provider.OpenWriteAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeOfType<GcsWriteStream>();
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
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt?contentType=application/json");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        // Act
        var stream = await _provider.OpenWriteAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        stream.Should().BeOfType<GcsWriteStream>();
    }

    [Fact]
    public async Task OpenReadAsync_WithRetryableServerError_RetriesAndSucceeds()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            RetrySettings = new GcsRetrySettings
            {
                InitialDelay = TimeSpan.Zero,
                MaxDelay = TimeSpan.Zero,
                DelayMultiplier = 1.0,
                MaxAttempts = 2,
                RetryOnServerErrors = true,
            },
        };

        var clientFactory = A.Fake<GcsClientFactory>(c => c
            .WithArgumentsForConstructor(new object[] { options })
            .CallsBaseMethods());

        var storageClient = A.Fake<StorageClient>();
        var provider = new GcsStorageProvider(clientFactory, options);
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => clientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(storageClient));

        var attempts = 0;
        A.CallTo(() => storageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .ReturnsLazily(call =>
            {
                attempts++;
                if (attempts == 1)
                {
                    throw new GoogleApiException("storage", "Internal error")
                    {
                        HttpStatusCode = HttpStatusCode.InternalServerError,
                    };
                }

                var stream = call.GetArgument<Stream>(2)!;
                var data = new byte[] { 1, 2, 3 };
                stream.Write(data, 0, data.Length);
                return Task.FromResult(new Google.Apis.Storage.v1.Data.Object());
            });

        // Act
        await using var stream = await provider.OpenReadAsync(uri);

        // Assert
        stream.Should().NotBeNull();
        attempts.Should().Be(2);
    }

    [Fact]
    public async Task OpenWriteAsync_WithRetryableServerError_RetriesUploadAndSucceeds()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            RetrySettings = new GcsRetrySettings
            {
                InitialDelay = TimeSpan.Zero,
                MaxDelay = TimeSpan.Zero,
                DelayMultiplier = 1.0,
                MaxAttempts = 2,
                RetryOnServerErrors = true,
            },
        };

        var clientFactory = A.Fake<GcsClientFactory>(c => c
            .WithArgumentsForConstructor(new object[] { options })
            .CallsBaseMethods());

        var storageClient = A.Fake<StorageClient>();
        var provider = new GcsStorageProvider(clientFactory, options);
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => clientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(storageClient));

        var attempts = 0;
        A.CallTo(() => storageClient.UploadObjectAsync(
            A<Google.Apis.Storage.v1.Data.Object>._,
            A<Stream>._,
            A<UploadObjectOptions>._,
            A<CancellationToken>._))
            .ReturnsLazily(_ =>
            {
                attempts++;
                if (attempts == 1)
                {
                    throw new GoogleApiException("storage", "Service unavailable")
                    {
                        HttpStatusCode = HttpStatusCode.ServiceUnavailable,
                    };
                }

                return Task.FromResult(new Google.Apis.Storage.v1.Data.Object());
            });

        var stream = await provider.OpenWriteAsync(uri);
        await stream.WriteAsync(new byte[] { 1, 2, 3 }, 0, 3);

        // Act
        await stream.DisposeAsync();

        // Assert
        attempts.Should().Be(2);
    }

    [Fact]
    public async Task ExistsAsync_WithExistingObject_ReturnsTrue()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        A.CallTo(() => _fakeStorageClient.GetObjectAsync(
            A<string>._,
            A<string>._,
            A<GetObjectOptions>._,
            A<CancellationToken>._))
            .ReturnsLazily(call => Task.FromResult(new Google.Apis.Storage.v1.Data.Object()));

        // Act
        var result = await _provider.ExistsAsync(uri);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_WithNonExistingObject_ReturnsFalse()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Not found")
        {
            HttpStatusCode = HttpStatusCode.NotFound,
        };

        A.CallTo(() => _fakeStorageClient.GetObjectAsync(
            A<string>._,
            A<string>._,
            A<GetObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

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
    public async Task ExistsAsync_WithUnauthorized_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Unauthorized")
        {
            HttpStatusCode = HttpStatusCode.Unauthorized,
        };

        A.CallTo(() => _fakeStorageClient.GetObjectAsync(
            A<string>._,
            A<string>._,
            A<GetObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

        // Act & Assert
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => _provider.ExistsAsync(uri));
    }

    [Fact]
    public async Task ExistsAsync_WithRetryableRateLimitError_RetriesAndSucceeds()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            RetrySettings = new GcsRetrySettings
            {
                InitialDelay = TimeSpan.Zero,
                MaxDelay = TimeSpan.Zero,
                DelayMultiplier = 1.0,
                MaxAttempts = 2,
                RetryOnRateLimit = true,
                RetryOnServerErrors = false,
            },
        };

        var clientFactory = A.Fake<GcsClientFactory>(c => c
            .WithArgumentsForConstructor(new object[] { options })
            .CallsBaseMethods());

        var storageClient = A.Fake<StorageClient>();
        var provider = new GcsStorageProvider(clientFactory, options);
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => clientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .Returns(Task.FromResult(storageClient));

        var attempts = 0;
        A.CallTo(() => storageClient.GetObjectAsync(
            A<string>._,
            A<string>._,
            A<GetObjectOptions>._,
            A<CancellationToken>._))
            .ReturnsLazily(_ =>
            {
                attempts++;
                if (attempts == 1)
                {
                    throw new GoogleApiException("storage", "Rate limited")
                    {
                        HttpStatusCode = HttpStatusCode.TooManyRequests,
                    };
                }

                return Task.FromResult(new Google.Apis.Storage.v1.Data.Object());
            });

        // Act
        var exists = await provider.ExistsAsync(uri);

        // Assert
        exists.Should().BeTrue();
        attempts.Should().Be(2);
    }

    [Fact]
    public async Task DeleteAsync_AlwaysThrowsNotSupportedException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        // Act & Assert
        await Assert.ThrowsAsync<NotSupportedException>(() => _provider.DeleteAsync(uri));
    }

    [Fact]
    public async Task ListAsync_WithNullPrefix_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await _provider.ListAsync(null!).ToListAsync());
    }

    // Note: ListAsync tests that require mocking StorageClient.ListObjectsAsync are not included
    // because PagedAsyncEnumerable<Objects, Object> is a sealed class that cannot be mocked with FakeItEasy.
    // Integration tests with a real or emulator-backed GCS client are needed for full ListAsync coverage.

    [Fact]
    public async Task GetMetadataAsync_WithExistingObject_ReturnsMetadata()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsObject = new Google.Apis.Storage.v1.Data.Object
        {
            Size = 1024,
            UpdatedDateTimeOffset = DateTimeOffset.UtcNow,
            ContentType = "text/plain",
            ETag = "\"abc123\"",
            Metadata = new Dictionary<string, string> { ["custom-key"] = "custom-value" },
        };

        A.CallTo(() => _fakeStorageClient.GetObjectAsync(
            A<string>._,
            A<string>._,
            A<GetObjectOptions>._,
            A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(gcsObject));

        // Act
        var metadata = await _provider.GetMetadataAsync(uri);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(1024);
        metadata.ContentType.Should().Be("text/plain");
        metadata.ETag.Should().Be("\"abc123\"");
        metadata.IsDirectory.Should().BeFalse();
        metadata.CustomMetadata.Should().ContainKey("custom-key").WhoseValue.Should().Be("custom-value");
    }

    [Fact]
    public async Task GetMetadataAsync_WithNonExistingObject_ReturnsNull()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Not found")
        {
            HttpStatusCode = HttpStatusCode.NotFound,
        };

        A.CallTo(() => _fakeStorageClient.GetObjectAsync(
            A<string>._,
            A<string>._,
            A<GetObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

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
    public async Task GetMetadataAsync_WithUnauthorized_ThrowsUnauthorizedAccessException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        var gcsException = new GoogleApiException("storage", "Unauthorized")
        {
            HttpStatusCode = HttpStatusCode.Unauthorized,
        };

        A.CallTo(() => _fakeStorageClient.GetObjectAsync(
            A<string>._,
            A<string>._,
            A<GetObjectOptions>._,
            A<CancellationToken>._))
            .ThrowsAsync(gcsException);

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
        metadata.Name.Should().Be("Google Cloud Storage");
        metadata.SupportedSchemes.Should().Contain("gs");
        metadata.SupportsRead.Should().BeTrue();
        metadata.SupportsWrite.Should().BeTrue();
        metadata.SupportsDelete.Should().BeFalse();
        metadata.SupportsListing.Should().BeTrue();
        metadata.SupportsMetadata.Should().BeTrue();
        metadata.SupportsHierarchy.Should().BeFalse();
        metadata.Capabilities["uploadChunkSizeBytes"].Should().Be(16 * 1024 * 1024);
        metadata.Capabilities["uploadBufferThresholdBytes"].Should().Be(64 * 1024 * 1024);
        metadata.Capabilities["supportsServiceUrl"].Should().Be(true);
        metadata.Capabilities["supportsAccessToken"].Should().Be(true);
        metadata.Capabilities["supportsCredentialsPath"].Should().Be(true);
    }

    [Fact]
    public void GetMetadata_WithCustomOptions_ReturnsCorrectCapabilities()
    {
        // Arrange
        var customOptions = new GcsStorageProviderOptions
        {
            UploadChunkSizeBytes = 32 * 1024 * 1024,
            UploadBufferThresholdBytes = 128 * 1024 * 1024,
        };

        var customProvider = new GcsStorageProvider(_fakeClientFactory, customOptions);

        // Act
        var metadata = customProvider.GetMetadata();

        // Assert
        metadata.Capabilities["uploadChunkSizeBytes"].Should().Be(32 * 1024 * 1024);
        metadata.Capabilities["uploadBufferThresholdBytes"].Should().Be(128 * 1024 * 1024);
    }

    [Fact]
    public async Task OpenReadAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs:///test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenReadAsync(uri));
    }

    [Fact]
    public async Task OpenWriteAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs:///test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.OpenWriteAsync(uri));
    }

    [Fact]
    public async Task ExistsAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs:///test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.ExistsAsync(uri));
    }

    [Fact]
    public async Task GetMetadataAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs:///test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => _provider.GetMetadataAsync(uri));
    }

    [Fact]
    public async Task ListAsync_WithEmptyBucketName_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("gs:///prefix/");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(async () => await _provider.ListAsync(uri).ToListAsync());
    }

    [Theory]
    [InlineData("gs://bucket/object", "bucket", "object")]
    [InlineData("gs://bucket/path/to/object", "bucket", "path/to/object")]
    public async Task OpenReadAsync_ParsesBucketAndObjectNameCorrectly(string uriString, string expectedBucket, string expectedObjectName)
    {
        // Arrange
        var uri = StorageUri.Parse(uriString);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .Invokes(call =>
            {
                var stream = call.GetArgument<Stream>(2)!;
                var data = new byte[] { 1, 2, 3, 4, 5 };
                stream.Write(data, 0, data.Length);
            })
            .ReturnsLazily(call => Task.FromResult(new Google.Apis.Storage.v1.Data.Object()));

        // Act
        var stream = await _provider.OpenReadAsync(uri);

        // Assert
        stream.Should().NotBeNull();

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            expectedBucket,
            expectedObjectName,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task OpenReadAsync_DisposeCalledMultipleTimes_DoesNotThrow()
    {
        // Regression test: GcsReadStream.DisposeAsync used to double-dispose the inner temp-file stream
        // because base.DisposeAsync() called Dispose(true) which called _inner.Dispose() a second time.

        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .Invokes(call =>
            {
                var stream = call.GetArgument<Stream>(2)!;
                var data = new byte[] { 1, 2, 3, 4, 5 };
                stream.Write(data, 0, data.Length);
            })
            .ReturnsLazily(_ => Task.FromResult(new Google.Apis.Storage.v1.Data.Object()));

        var readStream = await _provider.OpenReadAsync(uri);

        // Act - dispose multiple times; should not throw ObjectDisposedException on second call
        var act = async () =>
        {
            await readStream.DisposeAsync();
            await readStream.DisposeAsync(); // second call should be a no-op
        };

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task OpenReadAsync_DisposeAndDisposeAsync_AreIdempotent()
    {
        // Regression test: mixing Dispose() and DisposeAsync() should not double-dispose.
        var uri = StorageUri.Parse("gs://test-bucket/test-object.txt");

        A.CallTo(() => _fakeClientFactory.GetClientAsync(uri, A<CancellationToken>._))
            .ReturnsLazily(() => Task.FromResult(_fakeStorageClient));

        A.CallTo(() => _fakeStorageClient.DownloadObjectAsync(
            A<string>._,
            A<string>._,
            A<Stream>._,
            A<DownloadObjectOptions>._,
            A<CancellationToken>._))
            .Invokes(call =>
            {
                var stream = call.GetArgument<Stream>(2)!;
                stream.Write(new byte[] { 42 }, 0, 1);
            })
            .ReturnsLazily(_ => Task.FromResult(new Google.Apis.Storage.v1.Data.Object()));

        var readStream = await _provider.OpenReadAsync(uri);

        // Act
        var act = async () =>
        {
            readStream.Dispose();
            await readStream.DisposeAsync();
        };

        await act.Should().NotThrowAsync();
    }
}
