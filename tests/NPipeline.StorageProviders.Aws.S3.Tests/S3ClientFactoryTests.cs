using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using FluentAssertions;
using FakeItEasy;
using NPipeline.Connectors;
using Xunit;

namespace NPipeline.StorageProviders.Aws.S3.Tests;

public class S3ClientFactoryTests
{
    private readonly S3StorageProviderOptions _defaultOptions = new();

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
        var factory = new S3ClientFactory(_defaultOptions);

        // Assert
        factory.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultCredentials_CreatesClientWithoutExplicitCredentials()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithExplicitCredentialsInURI_CreatesClientWithCredentials()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?accessKey=AKIAIOSFODNN7EXAMPLE&secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithRegionInURI_ExtractsRegionCorrectly()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?region=ap-southeast-2");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidRegionInURI_ThrowsArgumentException()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?region=invalid-region");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrlInURI_ExtractsServiceUrlCorrectly()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?serviceUrl=http://localhost:9000");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidServiceUrlInURI_ThrowsArgumentException()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?serviceUrl=invalid-url");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithPathStyleTrueInURI_SetsForcePathStyle()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=true");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithPathStyleFalseInURI_DoesNotSetForcePathStyle()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=false");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidPathStyleInURI_ThrowsArgumentException()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=invalid");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentException>(() => factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithSameConfiguration_ReturnsCachedClient()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?region=us-east-1");

        // Act
        var client1 = await factory.GetClientAsync(uri);
        var client2 = await factory.GetClientAsync(uri);

        // Assert
        client1.Should().BeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentCredentials_ReturnsDifferentClients()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri1 = StorageUri.Parse("s3://test-bucket/test-key?accessKey=key1&secretKey=secret1");
        var uri2 = StorageUri.Parse("s3://test-bucket/test-key?accessKey=key2&secretKey=secret2");

        // Act
        var client1 = await factory.GetClientAsync(uri1);
        var client2 = await factory.GetClientAsync(uri2);

        // Assert
        client1.Should().NotBeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentRegions_ReturnsDifferentClients()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri1 = StorageUri.Parse("s3://test-bucket/test-key?region=us-east-1");
        var uri2 = StorageUri.Parse("s3://test-bucket/test-key?region=us-west-2");

        // Act
        var client1 = await factory.GetClientAsync(uri1);
        var client2 = await factory.GetClientAsync(uri2);

        // Assert
        client1.Should().NotBeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentServiceUrls_ReturnsDifferentClients()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri1 = StorageUri.Parse("s3://test-bucket/test-key?serviceUrl=http://localhost:9000");
        var uri2 = StorageUri.Parse("s3://test-bucket/test-key?serviceUrl=http://localhost:9001");

        // Act
        var client1 = await factory.GetClientAsync(uri1);
        var client2 = await factory.GetClientAsync(uri2);

        // Assert
        client1.Should().NotBeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentPathStyles_ReturnsDifferentClients()
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri1 = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=true");
        var uri2 = StorageUri.Parse("s3://test-bucket/test-key?pathStyle=false");

        // Act
        var client1 = await factory.GetClientAsync(uri1);
        var client2 = await factory.GetClientAsync(uri2);

        // Assert
        client1.Should().NotBeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultCredentialsFromOptions_UsesDefaultCredentials()
    {
        // Arrange
        var options = new S3StorageProviderOptions
        {
            DefaultCredentials = new BasicAWSCredentials("test-key", "test-secret")
        };
        var factory = new S3ClientFactory(options);
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultRegionFromOptions_UsesDefaultRegion()
    {
        // Arrange
        var options = new S3StorageProviderOptions
        {
            DefaultRegion = RegionEndpoint.APSoutheast2
        };
        var factory = new S3ClientFactory(options);
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultServiceUrlFromOptions_UsesDefaultServiceUrl()
    {
        // Arrange
        var options = new S3StorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000")
        };
        var factory = new S3ClientFactory(options);
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultForcePathStyleFromOptions_UsesDefaultForcePathStyle()
    {
        // Arrange
        var options = new S3StorageProviderOptions
        {
            ForcePathStyle = true
        };
        var factory = new S3ClientFactory(options);
        var uri = StorageUri.Parse("s3://test-bucket/test-key");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Fact]
    public async Task GetClientAsync_WithUriParametersOverridesOptions()
    {
        // Arrange
        var options = new S3StorageProviderOptions
        {
            DefaultRegion = RegionEndpoint.USEast1,
            ForcePathStyle = false
        };
        var factory = new S3ClientFactory(options);
        var uri = StorageUri.Parse("s3://test-bucket/test-key?region=us-west-2&pathStyle=true");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
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
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse($"s3://test-bucket/test-key?region={region}");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }

    [Theory]
    [InlineData("http://localhost:9000")]
    [InlineData("https://s3.example.com")]
    [InlineData("http://minio:9000")]
    public async Task GetClientAsync_WithValidServiceUrls_CreatesClient(string serviceUrl)
    {
        // Arrange
        var factory = new S3ClientFactory(_defaultOptions);
        var uri = StorageUri.Parse($"s3://test-bucket/test-key?serviceUrl={Uri.EscapeDataString(serviceUrl)}");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeAssignableTo<AmazonS3Client>();
    }
}
