using Amazon.S3;
using AwesomeAssertions;
using FakeItEasy;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.S3.Tests;

public class S3ClientFactoryBaseTests
{
    private static StorageUri Uri(string host = "my-bucket") => StorageUri.Parse($"s3://{host}/some-key");

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
    public async Task GetClientAsync_CalledWithDifferentEndpoints_ReturnsDifferentInstances()
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
    public async Task GetClientAsync_BeyondTheCacheLimit_StillReturnsWorkingClients()
    {
        var factory = new TestClientFactory(limit: 2);

        var first = await factory.GetClientAsync(Uri("bucket-a"));
        await factory.GetClientAsync(Uri("bucket-b"));
        await factory.GetClientAsync(Uri("bucket-c"));

        // The oldest endpoint was evicted, so it is created again; the evicted client is not disposed under its users.
        var again = await factory.GetClientAsync(Uri("bucket-a"));

        again.Should().NotBeSameAs(first);
        A.CallTo(() => first.Dispose()).MustNotHaveHappened();
    }

    [Fact]
    public async Task Dispose_DisposesCachedClients()
    {
        var factory = new TestClientFactory();
        var client = await factory.GetClientAsync(Uri());

        factory.Dispose();

        A.CallTo(() => client.Dispose()).MustHaveHappenedOnceExactly();
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
        private readonly Func<S3EndpointKey, IAmazonS3> _clientFactory;

        public TestClientFactory(Func<S3EndpointKey, IAmazonS3>? clientFactory = null, int limit = 100)
            : base(limit)
        {
            _clientFactory = clientFactory ?? (_ => A.Fake<IAmazonS3>());
        }

        // The host stands in for the endpoint, so tests can tell endpoints apart.
        protected override S3EndpointKey GetEndpoint(StorageUri uri) => new(uri.Host, null, false);

        protected override IAmazonS3 CreateClient(S3EndpointKey endpoint) => _clientFactory(endpoint);
    }
}
