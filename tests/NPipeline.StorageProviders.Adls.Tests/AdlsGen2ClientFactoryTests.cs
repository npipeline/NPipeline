using Azure.Storage.Files.DataLake;
using FluentAssertions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

public class AdlsGen2ClientFactoryTests
{
    private readonly AdlsGen2ClientFactory _factory;
    private readonly AdlsGen2StorageProviderOptions _options;

    public AdlsGen2ClientFactoryTests()
    {
        _options = new AdlsGen2StorageProviderOptions();
        _factory = new AdlsGen2ClientFactory(_options);
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new AdlsGen2ClientFactory(null!));
    }

    [Fact]
    public async Task GetClientAsync_WithConnectionString_ReturnsClient()
    {
        // Arrange
        var connectionString = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";
        var uri = StorageUri.Parse($"adls://filesystem/path/file.txt?connectionString={Uri.EscapeDataString(connectionString)}");

        // Act
        var client = await _factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<DataLakeServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithAccountKey_ReturnsClient()
    {
        // Arrange
        var accountName = "testaccount";
        var accountKey = "dGVzdA==";
        var uri = StorageUri.Parse($"adls://filesystem/path/file.txt?accountName={accountName}&accountKey={Uri.EscapeDataString(accountKey)}");

        // Act
        var client = await _factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<DataLakeServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithSasToken_ReturnsClient()
    {
        // Arrange
        var accountName = "testaccount";
        var sasToken = "sv=2024-01-01&sig=test";
        var uri = StorageUri.Parse($"adls://filesystem/path/file.txt?accountName={accountName}&sasToken={Uri.EscapeDataString(sasToken)}");

        // Act
        var client = await _factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<DataLakeServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrl_ReturnsClient()
    {
        // Arrange
        var serviceUrl = "https://testaccount.dfs.core.windows.net";
        var uri = StorageUri.Parse($"adls://filesystem/path/file.txt?serviceUrl={Uri.EscapeDataString(serviceUrl)}&accountName=testaccount");

        // Act
        var client = await _factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<DataLakeServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_CachesClient()
    {
        // Arrange
        var connectionString = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";
        var uri = StorageUri.Parse($"adls://filesystem/path/file.txt?connectionString={Uri.EscapeDataString(connectionString)}");

        // Act
        var client1 = await _factory.GetClientAsync(uri);
        var client2 = await _factory.GetClientAsync(uri);

        // Assert
        client1.Should().BeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentConnectionStrings_ReturnsDifferentClients()
    {
        // Arrange
        var connectionString1 = "DefaultEndpointsProtocol=https;AccountName=testaccount1;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";
        var connectionString2 = "DefaultEndpointsProtocol=https;AccountName=testaccount2;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";
        var uri1 = StorageUri.Parse($"adls://filesystem/path/file.txt?connectionString={Uri.EscapeDataString(connectionString1)}");
        var uri2 = StorageUri.Parse($"adls://filesystem/path/file.txt?connectionString={Uri.EscapeDataString(connectionString2)}");

        // Act
        var client1 = await _factory.GetClientAsync(uri1);
        var client2 = await _factory.GetClientAsync(uri2);

        // Assert
        client1.Should().NotBeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultConnectionString_UsesDefault()
    {
        // Arrange
        var connectionString = "DefaultEndpointsProtocol=https;AccountName=defaultaccount;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";
        _options.DefaultConnectionString = connectionString;
        var factory = new AdlsGen2ClientFactory(_options);
        var uri = StorageUri.Parse("adls://filesystem/path/file.txt");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<DataLakeServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultServiceUrl_UsesDefault()
    {
        // Arrange
        var serviceUrl = new Uri("https://testaccount.dfs.core.windows.net");
        _options.ServiceUrl = serviceUrl;
        var factory = new AdlsGen2ClientFactory(_options);
        var uri = StorageUri.Parse("adls://filesystem/path/file.txt");

        // Act
        var client = await factory.GetClientAsync(uri);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<DataLakeServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithNoCredentials_ThrowsInvalidOperationException()
    {
        // Arrange - no credentials configured
        var uri = StorageUri.Parse("adls://filesystem/path/file.txt");

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() => _factory.GetClientAsync(uri));
    }

    [Fact]
    public async Task GetClientAsync_WithCancellationToken_PassesToken()
    {
        // Arrange
        var connectionString = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";
        var uri = StorageUri.Parse($"adls://filesystem/path/file.txt?connectionString={Uri.EscapeDataString(connectionString)}");
        using var cts = new CancellationTokenSource();

        // Act
        var client = await _factory.GetClientAsync(uri, cts.Token);

        // Assert
        client.Should().NotBeNull();
    }
}
