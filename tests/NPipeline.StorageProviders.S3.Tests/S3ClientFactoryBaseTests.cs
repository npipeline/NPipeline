using Amazon.S3;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.S3.Tests;

public class S3ClientFactoryBaseTests
{
    private static StorageUri Uri(string host = "my-bucket")
    {
        return StorageUri.Parse($"s3://{host}/some-key");
    }

    // ── GetClientAsync ────────────────────────────────────────────────────

    [Fact]
    public async Task GetClientAsync_ReturnsNonNullClient()
    {
        var factory = new TestClientFactory();

        var client = await factory.GetClientAsync(Uri());

        client.Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_CalledTwiceWithSameUri_ReturnsSameInstance()
    {
        var factory = new TestClientFactory();

        var client1 = await factory.GetClientAsync(Uri());
        var client2 = await factory.GetClientAsync(Uri());

        client1.Should().BeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_CalledWithDifferentHosts_ReturnsDifferentInstances()
    {
        var factory = new TestClientFactory();

        var client1 = await factory.GetClientAsync(Uri("bucket-a"));
        var client2 = await factory.GetClientAsync(Uri("bucket-b"));

        client1.Should().NotBeSameAs(client2);
    }

    [Fact]
    public async Task GetClientAsync_CreateClientCalledOnce_ForSameUri()
    {
        var callCount = 0;

        var factory = new TestClientFactory(_ =>
        {
            callCount++;
            return A.Fake<IAmazonS3>();
        });

        await factory.GetClientAsync(Uri());
        await factory.GetClientAsync(Uri());

        callCount.Should().Be(1);
    }

    [Fact]
    public async Task GetClientAsync_WithCancelledToken_ThrowsOperationCancelledException()
    {
        var factory = new TestClientFactory();
        var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => factory.GetClientAsync(Uri(), cts.Token));
    }

    // ── ClearCache ────────────────────────────────────────────────────────

    [Fact]
    public async Task ClearCache_AfterGet_ForcesNewClientCreation()
    {
        var factory = new TestClientFactory();

        var client1 = await factory.GetClientAsync(Uri());
        factory.ClearCache();
        var client2 = await factory.GetClientAsync(Uri());

        client2.Should().NotBeNull();
        client1.Should().NotBeSameAs(client2);
    }

    [Fact]
    public async Task ClearCache_CalledMultipleTimes_DoesNotThrow()
    {
        var factory = new TestClientFactory();
        await factory.GetClientAsync(Uri());

        factory.Invoking(f =>
        {
            f.ClearCache();
            f.ClearCache();
        }).Should().NotThrow();
    }

    [Fact]
    public void ClearCache_OnEmptyCache_DoesNotThrow()
    {
        var factory = new TestClientFactory();

        factory.Invoking(f => f.ClearCache()).Should().NotThrow();
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    /// <summary>
    ///     Minimal concrete implementation used to exercise the abstract base class.
    /// </summary>
    private sealed class TestClientFactory : S3ClientFactoryBase
    {
        private readonly Func<StorageUri, IAmazonS3> _clientFactory;

        public TestClientFactory(Func<StorageUri, IAmazonS3>? clientFactory = null)
        {
            _clientFactory = clientFactory ?? (_ => A.Fake<IAmazonS3>());
        }

        protected override IAmazonS3 CreateClient(StorageUri uri)
        {
            return _clientFactory(uri);
        }
    }
}
