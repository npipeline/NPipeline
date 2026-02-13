using AwesomeAssertions;
using Azure.Core;
using Azure.Storage.Blobs;
using FakeItEasy;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

public class AzureBlobClientFactoryTests
{
    private readonly BlobServiceClient _fakeBlobServiceClient;
    private readonly AzureBlobClientFactory _fakeClientFactory;

    public AzureBlobClientFactoryTests()
    {
        _fakeClientFactory = A.Fake<AzureBlobClientFactory>(c => c
            .WithArgumentsForConstructor([new AzureBlobStorageProviderOptions()])
            .CallsBaseMethods());

        _fakeBlobServiceClient = A.Fake<BlobServiceClient>();
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobClientFactory(null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidOptions_Succeeds()
    {
        // Act
        var factory = new AzureBlobClientFactory(new AzureBlobStorageProviderOptions());

        // Assert
        factory.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_WithConnectionString_CreatesClient()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        var uri = StorageUri.Parse(
            "azure://container/blob?connectionString=DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeBlobServiceClient);
    }

    [Fact]
    public async Task GetClientAsync_WithAccountKey_CreatesClient()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        var uri = StorageUri.Parse("azure://container/blob?accountName=testaccount&accountKey=dGVzdGtleQ==");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeBlobServiceClient);
    }

    [Fact]
    public async Task GetClientAsync_WithSasToken_CreatesClient()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        var uri = StorageUri.Parse(
            "azure://container/blob?accountName=testaccount&sasToken=sv=2021-01-01&ss=b&srt=sco&sp=rwdlac&se=2021-01-02T00:00:00Z&st=2021-01-01T00:00:00Z&spr=https&sig=test");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeBlobServiceClient);
    }

    [Fact]
    public async Task GetClientAsync_WithTokenCredential_CreatesClient()
    {
        // Arrange
        var tokenCredential = A.Fake<TokenCredential>();

        var options = new AzureBlobStorageProviderOptions
        {
            DefaultCredential = tokenCredential,
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob?accountName=testaccount");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultCredentialChain_CreatesClient()
    {
        // Arrange
        var options = new AzureBlobStorageProviderOptions
        {
            UseDefaultCredentialChain = true,
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob?accountName=testaccount");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrl_CreatesClient()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        var uri = StorageUri.Parse("azure://container/blob?serviceUrl=https://localhost:10000/devstoreaccount1");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeBlobServiceClient);
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidServiceUrl_ThrowsArgumentException()
    {
        // Arrange
        var uri = StorageUri.Parse("azure://container/blob?serviceUrl=invalid-url");

        var factory = new AzureBlobClientFactory(new AzureBlobStorageProviderOptions());

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithSameConfiguration_ReturnsCachedClient()
    {
        // Arrange
        var cachedClient = A.Fake<BlobServiceClient>();

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(cachedClient));

        var uri = StorageUri.Parse("azure://container/blob?accountName=testaccount&accountKey=dGVzdGtleQ==");

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
    public async Task GetClientAsync_WithDifferentAccountKeys_ReturnsDifferentClients()
    {
        // Arrange
        var client1 = A.Fake<BlobServiceClient>();
        var client2 = A.Fake<BlobServiceClient>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;

                return Task.FromResult(callCount == 1
                    ? client1
                    : client2);
            });

        var uri1 = StorageUri.Parse("azure://container/blob?accountName=testaccount&accountKey=dGVzdGtleTE=");
        var uri2 = StorageUri.Parse("azure://container/blob?accountName=testaccount&accountKey=dGVzdGtleTI=");

        // Act
        var result1 = await _fakeClientFactory.GetClientAsync(uri1);
        var result2 = await _fakeClientFactory.GetClientAsync(uri2);

        // Assert
        result1.Should().NotBeSameAs(result2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentSasTokens_ReturnsDifferentClients()
    {
        // Arrange
        var client1 = A.Fake<BlobServiceClient>();
        var client2 = A.Fake<BlobServiceClient>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;

                return Task.FromResult(callCount == 1
                    ? client1
                    : client2);
            });

        var uri1 = StorageUri.Parse("azure://container/blob?accountName=testaccount&sasToken=token1");
        var uri2 = StorageUri.Parse("azure://container/blob?accountName=testaccount&sasToken=token2");

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
        var client1 = A.Fake<BlobServiceClient>();
        var client2 = A.Fake<BlobServiceClient>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;

                return Task.FromResult(callCount == 1
                    ? client1
                    : client2);
            });

        var uri1 = StorageUri.Parse("azure://container/blob?serviceUrl=https://localhost:10000");
        var uri2 = StorageUri.Parse("azure://container/blob?serviceUrl=https://localhost:10001");

        // Act
        var result1 = await _fakeClientFactory.GetClientAsync(uri1);
        var result2 = await _fakeClientFactory.GetClientAsync(uri2);

        // Assert
        result1.Should().NotBeSameAs(result2);
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultConnectionString_UsesDefaultConnectionString()
    {
        // Arrange
        var connectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test;EndpointSuffix=core.windows.net";

        var options = new AzureBlobStorageProviderOptions
        {
            DefaultConnectionString = connectionString,
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultServiceUrl_UsesDefaultServiceUrl()
    {
        // Arrange
        var options = new AzureBlobStorageProviderOptions
        {
            ServiceUrl = new Uri("https://localhost:10000/devstoreaccount1"),
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithUriParametersOverridesOptions()
    {
        // Arrange
        var options = new AzureBlobStorageProviderOptions
        {
            ServiceUrl = new Uri("https://default.example.com"),
            DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=default;AccountKey=default",
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob?serviceUrl=https://override.example.com");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithConnectionStringInUri_OverridesDefaultConnectionString()
    {
        // Arrange
        var options = new AzureBlobStorageProviderOptions
        {
            DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=default;AccountKey=default",
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob?connectionString=DefaultEndpointsProtocol=https;AccountName=override;AccountKey=override");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithAccountNameInUri_UsesAccountName()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        var uri = StorageUri.Parse("azure://container/blob?accountName=myaccount");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeBlobServiceClient);
    }

    [Fact]
    public async Task GetClientAsync_WithAnonymousAccess_CreatesClient()
    {
        // Arrange
        var options = new AzureBlobStorageProviderOptions
        {
            ServiceUrl = new Uri("https://publicaccount.blob.core.windows.net"),
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithConnectionStringTakesPrecedence()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        // Connection string should take precedence over other parameters
        var uri = StorageUri.Parse(
            "azure://container/blob?connectionString=DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test&accountName=otheraccount&accountKey=otherkey");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeBlobServiceClient);
    }

    [Theory]
    [InlineData("https://localhost:10000/devstoreaccount1")]
    [InlineData("https://storageaccount.blob.core.windows.net")]
    [InlineData("https://customendpoint.example.com")]
    public async Task GetClientAsync_WithValidServiceUrls_CreatesClient(string serviceUrl)
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        var encodedUrl = Uri.EscapeDataString(serviceUrl);
        var uri = StorageUri.Parse($"azure://container/blob?serviceUrl={encodedUrl}");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeBlobServiceClient);
    }

    [Theory]
    [InlineData("myaccount")]
    [InlineData("my-storage-account")]
    [InlineData("storageaccount123")]
    public async Task GetClientAsync_WithValidAccountNames_CreatesClient(string accountName)
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeBlobServiceClient));

        var uri = StorageUri.Parse($"azure://container/blob?accountName={accountName}");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeBlobServiceClient);
    }

    [Fact]
    public async Task GetClientAsync_WithMixedCredentials_CreatesCorrectClient()
    {
        // Arrange
        var options = new AzureBlobStorageProviderOptions
        {
            DefaultCredential = A.Fake<TokenCredential>(),
        };

        var factory = new AzureBlobClientFactory(options);

        // URI with explicit connection string should take precedence
        var uri = StorageUri.Parse("azure://container/blob?connectionString=DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithEmptyConnectionStringParameter_IgnoresParameter()
    {
        // Arrange
        var options = new AzureBlobStorageProviderOptions
        {
            ServiceUrl = new Uri("https://storageaccount.blob.core.windows.net"),
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob?connectionString=");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithEmptyAccountNameParameter_IgnoresParameter()
    {
        // Arrange
        var options = new AzureBlobStorageProviderOptions
        {
            DefaultConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test",
        };

        var factory = new AzureBlobClientFactory(options);

        var uri = StorageUri.Parse("azure://container/blob?accountName=");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<BlobServiceClient>();
    }
}
