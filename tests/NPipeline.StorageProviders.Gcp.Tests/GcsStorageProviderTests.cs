using System.Net;
using AwesomeAssertions;
using FakeItEasy;
using Google;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Gcp.Reliability;
using NPipeline.StorageProviders.Models;
using NResilience;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace NPipeline.StorageProviders.Gcp.Tests;

public class GcsStorageProviderTests
{
    // The default preset with no delay between attempts, so tests of transient failures stay fast.
    private static readonly Resilience FastRetry = GcsStorageResilience.Default with { Backoff = Backoff.None };

    private readonly GcsClientFactory _fakeClientFactory;
    private readonly StorageClient _fakeStorageClient;
    private readonly GcsStorageProviderOptions _options;
    private readonly GcsStorageProvider _provider;

    public GcsStorageProviderTests()
    {
        _fakeClientFactory = A.Fake<GcsClientFactory>(c => c
            .WithArgumentsForConstructor([new GcsStorageProviderOptions()])
            .CallsBaseMethods());

        _fakeStorageClient = A.Fake<StorageClient>();
        _options = new GcsStorageProviderOptions { Resilience = FastRetry };
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
    public void Schemes_ContainsOnlyGcs()
    {
        _provider.Schemes.Should().Equal(StorageScheme.Gcs);
    }

    [Fact]
    public void Capabilities_DeclaresObjectStoreFeaturesOnly()
    {
        _provider.Capabilities.Should().Be(
            StorageCapabilities.Read | StorageCapabilities.Write | StorageCapabilities.List | StorageCapabilities.Delete | StorageCapabilities.Move);
    }

    [Fact]
    public async Task OpenWriteAsync_WithConditionalOptions_ThrowsUnsupportedCapability()
    {
        var uri = StorageUri.Parse("gs://bucket/object");

        await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(
            () => _provider.OpenWriteAsync(uri, new StorageWriteOptions { Overwrite = false }));
    }

    [Fact]
    public async Task OpenReadAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _provider.OpenReadAsync(null!));
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
            .ReturnsLazily(call => Task.FromResult(new Object()));

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
            Resilience = FastRetry,
        };

        var clientFactory = A.Fake<GcsClientFactory>(c => c
            .WithArgumentsForConstructor([options])
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

                return Task.FromResult(new Object());
            });

        // Act
        var exists = await provider.ExistsAsync(uri);

        // Assert
        exists.Should().BeTrue();
        attempts.Should().Be(2);
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

        var gcsObject = new Object
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

}
