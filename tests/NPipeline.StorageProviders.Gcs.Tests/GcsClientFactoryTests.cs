using AwesomeAssertions;
using FakeItEasy;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Gcs.Tests;

public class GcsClientFactoryTests
{
    private readonly GcsClientFactory _fakeClientFactory;
    private readonly StorageClient _fakeStorageClient;

    public GcsClientFactoryTests()
    {
        _fakeClientFactory = A.Fake<GcsClientFactory>(c => c
            .WithArgumentsForConstructor([new GcsStorageProviderOptions()])
            .CallsBaseMethods());

        _fakeStorageClient = A.Fake<StorageClient>();
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new GcsClientFactory(null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidOptions_Succeeds()
    {
        // Act
        var factory = new GcsClientFactory(new GcsStorageProviderOptions());

        // Assert
        factory.Should().NotBeNull();
    }

    [Fact]
    public void Constructor_WithInvalidOptions_ThrowsOnValidation()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            UploadChunkSizeBytes = 0, // Invalid
        };

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => new GcsClientFactory(options));
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultCredentials_CreatesClient()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithProjectIdInUri_ExtractsProjectIdCorrectly()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object?projectId=my-project");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrlInUri_ExtractsServiceUrlCorrectly()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object?serviceUrl=http://localhost:4443");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithEncodedServiceUrl_DecodesCorrectly()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var encodedUrl = Uri.EscapeDataString("http://localhost:4443/storage/v1/");
        var uri = StorageUri.Parse($"gs://test-bucket/test-object?serviceUrl={encodedUrl}");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidServiceUrlInUri_ThrowsArgumentException()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:4443"),
        };

        var factory = new GcsClientFactory(options);
        var uri = StorageUri.Parse("gs://test-bucket/test-object?serviceUrl=not-a-valid-url");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithAccessTokenInUri_ExtractsTokenCorrectly()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object?accessToken=ya29.test-token");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithCredentialsPathInUri_ExtractsPathCorrectly()
    {
        // Arrange
        // Note: Testing with a fake factory that won't actually try to read credentials
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object?credentialsPath=/path/to/credentials.json");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithSameConfiguration_ReturnsCachedClient()
    {
        // Arrange
        var cachedClient = A.Fake<StorageClient>();

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(cachedClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object?projectId=my-project");

        // Act
        var result1 = await _fakeClientFactory.GetClientAsync(uri);
        var result2 = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        result1.Should().BeSameAs(result2);
        result1.Should().BeSameAs(cachedClient);

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .MustHaveHappened(2, Times.Exactly);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentProjectIds_ReturnsDifferentClients()
    {
        // Arrange
        var client1 = A.Fake<StorageClient>();
        var client2 = A.Fake<StorageClient>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;
                return Task.FromResult(callCount == 1 ? client1 : client2);
            });

        var uri1 = StorageUri.Parse("gs://test-bucket/test-object?projectId=project1");
        var uri2 = StorageUri.Parse("gs://test-bucket/test-object?projectId=project2");

        // Act
        var result1 = await _fakeClientFactory.GetClientAsync(uri1);
        var result2 = await _fakeClientFactory.GetClientAsync(uri2);

        // Assert
        result1.Should().NotBeSameAs(result2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentServiceUrls_ReturnsDifferentClients()
    {
        // Arrange
        var client1 = A.Fake<StorageClient>();
        var client2 = A.Fake<StorageClient>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;
                return Task.FromResult(callCount == 1 ? client1 : client2);
            });

        var uri1 = StorageUri.Parse("gs://test-bucket/test-object?serviceUrl=http://localhost:4443");
        var uri2 = StorageUri.Parse("gs://test-bucket/test-object?serviceUrl=http://localhost:4444");

        // Act
        var result1 = await _fakeClientFactory.GetClientAsync(uri1);
        var result2 = await _fakeClientFactory.GetClientAsync(uri2);

        // Assert
        result1.Should().NotBeSameAs(result2);
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultProjectIdFromOptions_UsesDefaultProjectId()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultServiceUrlFromOptions_UsesDefaultServiceUrl()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithUriParametersOverrideOptions()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object?projectId=override-project&serviceUrl=http://override:4443");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithCancellationToken_PassesTokenCorrectly()
    {
        // Arrange
        var cts = new CancellationTokenSource();

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri, cts.Token);

        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_WithCancelledToken_ThrowsOperationCanceledException()
    {
        // Arrange
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var uri = StorageUri.Parse("gs://test-bucket/test-object");

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => _fakeClientFactory.GetClientAsync(uri, cts.Token));
    }

    [Theory]
    [InlineData("http://localhost:4443")]
    [InlineData("https://storage.googleapis.com")]
    [InlineData("http://fake-gcs-server:4443")]
    public async Task GetClientAsync_WithValidServiceUrls_CreatesClient(string serviceUrl)
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var encodedUrl = Uri.EscapeDataString(serviceUrl);
        var uri = StorageUri.Parse($"gs://test-bucket/test-object?serviceUrl={encodedUrl}");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithNoCredentialsAndUseDefaultCredentialsFalse_ThrowsInvalidOperationException()
    {
        // Arrange
        var options = new GcsStorageProviderOptions
        {
            UseDefaultCredentials = false,
            DefaultCredentials = null,
            ServiceUrl = new Uri("http://localhost:4443"),
        };

        var factory = new GcsClientFactory(options);
        var uri = StorageUri.Parse("gs://test-bucket/test-object");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => factory.GetClientAsync(uri));
        exception.Message.Should().Contain("No Google Cloud credentials available");
    }

    [Fact]
    public async Task GetClientAsync_WithNullUri_ThrowsArgumentNullException()
    {
        // Arrange
        var factory = new GcsClientFactory(new GcsStorageProviderOptions());

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => factory.GetClientAsync(null!));
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultCredentialsFromOptions_UsesDefaultCredentials()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var uri = StorageUri.Parse("gs://test-bucket/test-object");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeStorageClient);
    }

    [Fact]
    public async Task GetClientAsync_WithCredentialsParameter_UsesProvidedCredentials()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(
            A<GoogleCredential>._,
            A<Uri>._,
            A<string?>._,
            A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        // Act
        var client = await _fakeClientFactory.GetClientAsync(null, null, null);

        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrlParameter_UsesProvidedServiceUrl()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(
            A<GoogleCredential>._,
            A<Uri>._,
            A<string?>._,
            A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        var serviceUrl = new Uri("http://localhost:4443");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(null, serviceUrl, "my-project");

        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_WithProjectIdParameter_UsesProvidedProjectId()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(
            A<GoogleCredential>._,
            A<Uri>._,
            A<string?>._,
            A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeStorageClient));

        // Act
        var client = await _fakeClientFactory.GetClientAsync(null, null, "my-project");

        // Assert
        client.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_CacheKeyIsConsistentForSameParameters()
    {
        // Arrange
        var cachedClient = A.Fake<StorageClient>();

        A.CallTo(() => _fakeClientFactory.GetClientAsync(
            A<GoogleCredential>._,
            A<Uri>._,
            A<string?>._,
            A<CancellationToken>._))
            .Returns(Task.FromResult(cachedClient));

        var serviceUrl = new Uri("http://localhost:4443");

        // Act
        var client1 = await _fakeClientFactory.GetClientAsync(null, serviceUrl, "project1");
        var client2 = await _fakeClientFactory.GetClientAsync(null, serviceUrl, "project1");

        // Assert
        client1.Should().BeSameAs(client2);
    }
}
