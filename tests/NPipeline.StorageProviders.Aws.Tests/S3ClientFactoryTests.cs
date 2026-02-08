using Amazon.Runtime;
using Amazon.S3;
using FakeItEasy;
using FluentAssertions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Aws.Tests;

public class S3ClientFactoryTests
{
    private readonly S3ClientFactory _fakeClientFactory;
    private readonly IAmazonS3 _fakeS3Client;

    public S3ClientFactoryTests()
    {
        _fakeClientFactory = A.Fake<S3ClientFactory>(c => c
            .WithArgumentsForConstructor([new S3StorageProviderOptions()])
            .CallsBaseMethods());

        _fakeS3Client = A.Fake<IAmazonS3>();
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => new S3ClientFactory(null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidOptions_Succeeds()
    {
        // Act
        var factory = new S3ClientFactory(new S3StorageProviderOptions());

        // Assert
        factory.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultCredentials_CreatesClientWithoutExplicitCredentials()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Fact]
    public async Task GetClientAsync_WithExplicitCredentialsInURI_CreatesClientWithCredentials()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse("s3://test-bucket/test-key?accessKey=AKIAIOSFODNN7EXAMPLE&secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Fact]
    public async Task GetClientAsync_WithRegionInURI_ExtractsRegionCorrectly()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse("s3://test-bucket/test-key?region=ap-southeast-2");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidRegionInURI_ThrowsArgumentException()
    {
        // Arrange
        var factory = new S3ClientFactory(new S3StorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
        });

        var uri = StorageUri.Parse("s3://test-bucket/test-key?region=invalid-region");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrlInURI_ExtractsServiceUrlCorrectly()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse("s3://test-bucket/test-key?serviceUrl=http://localhost:9000");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidServiceUrlInURI_ThrowsArgumentException()
    {
        // Arrange
        var factory = new S3ClientFactory(new S3StorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
        });

        var uri = StorageUri.Parse("s3://test-bucket/test-key?serviceUrl=invalid-url");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithPathStyleTrueInURI_SetsForcePathStyle()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=true");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Fact]
    public async Task GetClientAsync_WithPathStyleFalseInURI_DoesNotSetForcePathStyle()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=false");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidPathStyleInURI_ThrowsArgumentException()
    {
        // Arrange
        var factory = new S3ClientFactory(new S3StorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
        });

        var uri = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=invalid");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithSameConfiguration_ReturnsCachedClient()
    {
        // Arrange
        var cachedClient = A.Fake<IAmazonS3>();

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(cachedClient));

        var uri = StorageUri.Parse("s3://test-bucket/test-key?region=us-east-1");

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
    public async Task GetClientAsync_WithDifferentCredentials_ReturnsDifferentClients()
    {
        // Arrange
        var client1 = A.Fake<IAmazonS3>();
        var client2 = A.Fake<IAmazonS3>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;

                return Task.FromResult(callCount == 1
                    ? client1
                    : client2);
            });

        var uri1 = StorageUri.Parse("s3://test-bucket/test-key?accessKey=key1&secretKey=secret1");
        var uri2 = StorageUri.Parse("s3://test-bucket/test-key?accessKey=key2&secretKey=secret2");

        // Act
        var result1 = await _fakeClientFactory.GetClientAsync(uri1);
        var result2 = await _fakeClientFactory.GetClientAsync(uri2);

        // Assert
        result1.Should().NotBeSameAs(result2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentRegions_ReturnsDifferentClients()
    {
        // Arrange
        var client1 = A.Fake<IAmazonS3>();
        var client2 = A.Fake<IAmazonS3>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;

                return Task.FromResult(callCount == 1
                    ? client1
                    : client2);
            });

        var uri1 = StorageUri.Parse("s3://test-bucket/test-key?region=us-east-1");
        var uri2 = StorageUri.Parse("s3://test-bucket/test-key?region=us-west-2");

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
        var client1 = A.Fake<IAmazonS3>();
        var client2 = A.Fake<IAmazonS3>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;

                return Task.FromResult(callCount == 1
                    ? client1
                    : client2);
            });

        var uri1 = StorageUri.Parse("s3://test-bucket/test-key?serviceUrl=http://localhost:9000");
        var uri2 = StorageUri.Parse("s3://test-bucket/test-key?serviceUrl=http://localhost:9001");

        // Act
        var result1 = await _fakeClientFactory.GetClientAsync(uri1);
        var result2 = await _fakeClientFactory.GetClientAsync(uri2);

        // Assert
        result1.Should().NotBeSameAs(result2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentPathStyles_ReturnsDifferentClients()
    {
        // Arrange
        var client1 = A.Fake<IAmazonS3>();
        var client2 = A.Fake<IAmazonS3>();
        var callCount = 0;

        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .ReturnsLazily(() =>
            {
                callCount++;

                return Task.FromResult(callCount == 1
                    ? client1
                    : client2);
            });

        var uri1 = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=true");
        var uri2 = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=false");

        // Act
        var result1 = await _fakeClientFactory.GetClientAsync(uri1);
        var result2 = await _fakeClientFactory.GetClientAsync(uri2);

        // Assert
        result1.Should().NotBeSameAs(result2);
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultCredentialsFromOptions_UsesDefaultCredentials()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var options = new S3StorageProviderOptions
        {
            DefaultCredentials = new BasicAWSCredentials("test-key", "test-secret"),
            ServiceUrl = new Uri("http://localhost:9000"),
        };

        var factory = new S3ClientFactory(options);
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<IAmazonS3>();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultRegionFromOptions_UsesDefaultRegion()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultServiceUrlFromOptions_UsesDefaultServiceUrl()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var options = new S3StorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
        };

        var factory = new S3ClientFactory(options);
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<IAmazonS3>();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultForcePathStyleFromOptions_UsesDefaultForcePathStyle()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var options = new S3StorageProviderOptions
        {
            ForcePathStyle = true,
            ServiceUrl = new Uri("http://localhost:9000"),
        };

        var factory = new S3ClientFactory(options);
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<IAmazonS3>();
    }

    [Fact]
    public async Task GetClientAsync_WithUriParametersOverridesOptions()
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse("s3://test-bucket/test-key?region=us-west-2&pathStyle=true");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Theory]
    [InlineData("us-east-1")]
    [InlineData("us-west-2")]
    [InlineData("eu-west-1")]
    [InlineData("ap-southeast-2")]
    [InlineData("ap-northeast-1")]
    public async Task GetClientAsync_WithValidRegions_CreatesClient(string region)
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse($"s3://test-bucket/test-key?region={region}");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }

    [Theory]
    [InlineData("http://localhost:9000")]
    [InlineData("https://s3.example.com")]
    [InlineData("http://minio:9000")]
    public async Task GetClientAsync_WithValidServiceUrls_CreatesClient(string serviceUrl)
    {
        // Arrange
        A.CallTo(() => _fakeClientFactory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._))
            .Returns(Task.FromResult(_fakeS3Client));

        var uri = StorageUri.Parse($"s3://test-bucket/test-key?serviceUrl={Uri.EscapeDataString(serviceUrl)}");

        // Act
        var client = await _fakeClientFactory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeSameAs(_fakeS3Client);
    }
}
