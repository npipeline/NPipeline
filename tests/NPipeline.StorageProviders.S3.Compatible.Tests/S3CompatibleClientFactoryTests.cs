using Amazon.S3;
using AwesomeAssertions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.S3.Compatible.Tests;

public class S3CompatibleClientFactoryTests
{
    private static S3CompatibleStorageProviderOptions CreateOptions(
        string url = "http://localhost:9000",
        string accessKey = "test-access",
        string secretKey = "test-secret",
        string signingRegion = "us-east-1",
        bool forcePathStyle = true)
    {
        return new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri(url),
            AccessKey = accessKey,
            SecretKey = secretKey,
            SigningRegion = signingRegion,
            ForcePathStyle = forcePathStyle,
        };
    }

    // ── Constructor ───────────────────────────────────────────────────────

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => new S3CompatibleClientFactory(null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidOptions_Succeeds()
    {
        var factory = new S3CompatibleClientFactory(CreateOptions());
        factory.Should().NotBeNull();
    }

    // ── GetClientAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task GetClientAsync_WithValidUri_ReturnsNonNullClient()
    {
        var factory = new S3CompatibleClientFactory(CreateOptions());
        var uri = StorageUri.Parse("s3://my-bucket/some-key");

        var client = await factory.GetClientAsync(uri);

        client.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_CalledTwice_ReturnsSameInstance()
    {
        // The base class caches clients by key — same options → same instance.
        var factory = new S3CompatibleClientFactory(CreateOptions());
        var uri = StorageUri.Parse("s3://my-bucket/some-key");

        var client1 = await factory.GetClientAsync(uri);
        var client2 = await factory.GetClientAsync(uri);

        client1.Should().BeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithDifferentBuckets_ReturnsSameInstance()
    {
        // Unlike AwsS3ClientFactory, the compatible factory uses a fixed endpoint.
        // Different bucket URIs should reuse the same cached client.
        var factory = new S3CompatibleClientFactory(CreateOptions());
        var uri1 = StorageUri.Parse("s3://bucket-a/key");
        var uri2 = StorageUri.Parse("s3://bucket-b/key");

        var client1 = await factory.GetClientAsync(uri1);
        var client2 = await factory.GetClientAsync(uri2);

        client1.Should().BeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithCancelledToken_ThrowsOperationCancelled()
    {
        var factory = new S3CompatibleClientFactory(CreateOptions());
        var uri = StorageUri.Parse("s3://my-bucket/some-key");
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => factory.GetClientAsync(uri, cts.Token));
    }

    // ── ClearCache ────────────────────────────────────────────────────────

    [Fact]
    public async Task ClearCache_AfterGet_AllowsNewClientCreation()
    {
        var factory = new S3CompatibleClientFactory(CreateOptions());
        var uri = StorageUri.Parse("s3://my-bucket/some-key");

        var client1 = await factory.GetClientAsync(uri);
        factory.ClearCache();
        var client2 = await factory.GetClientAsync(uri);

        // After clearing the cache a new client instance is returned.
        client2.Should().NotBeNull();
        client1.Should().NotBeSameAs(client2);
    }

    // ── Options influence ─────────────────────────────────────────────────

    [Fact]
    public async Task GetClientAsync_WithDifferentOptionInstances_ReturnsDifferentClients()
    {
        // Two separate factory instances (different options) should produce different clients.
        var factory1 = new S3CompatibleClientFactory(CreateOptions());
        var factory2 = new S3CompatibleClientFactory(CreateOptions("http://localhost:9001"));

        var uri = StorageUri.Parse("s3://my-bucket/key");

        var client1 = await factory1.GetClientAsync(uri);
        var client2 = await factory2.GetClientAsync(uri);

        client1.Should().NotBeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_WithLocalStackOptions_ReturnsClient()
    {
        var factory = new S3CompatibleClientFactory(new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:4566"),
            AccessKey = "test",
            SecretKey = "test",
        });

        var client = await factory.GetClientAsync(StorageUri.Parse("s3://test-bucket/key"));

        client.Should().BeAssignableTo<IAmazonS3>();
    }
}
