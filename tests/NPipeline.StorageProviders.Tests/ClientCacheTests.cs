using AwesomeAssertions;
using NPipeline.StorageProviders.Utilities;
using Xunit;

namespace NPipeline.StorageProviders.Tests;

public sealed class ClientCacheTests
{
    private readonly record struct Endpoint(string Host, int Port);

    [Fact]
    public void GetOrCreate_SameKey_CreatesOnceAndReturnsTheSameClient()
    {
        using var cache = new ClientCache<Endpoint, object>(10);
        var created = 0;

        var first = cache.GetOrCreate(new Endpoint("a", 1), _ => { created++; return new object(); });
        var second = cache.GetOrCreate(new Endpoint("a", 1), _ => { created++; return new object(); });

        second.Should().BeSameAs(first);
        created.Should().Be(1);
    }

    [Fact]
    public void GetOrCreate_DifferentKeys_CreateDifferentClients()
    {
        using var cache = new ClientCache<Endpoint, object>(10);

        var a = cache.GetOrCreate(new Endpoint("a", 1), _ => new object());
        var b = cache.GetOrCreate(new Endpoint("a", 2), _ => new object());

        b.Should().NotBeSameAs(a);
    }

    [Fact]
    public void GetOrCreate_BeyondTheLimit_EvictsTheLeastRecentlyUsed()
    {
        using var cache = new ClientCache<Endpoint, object>(2);
        var a = cache.GetOrCreate(new Endpoint("a", 1), _ => new object());
        var b = cache.GetOrCreate(new Endpoint("b", 1), _ => new object());

        // Touch "a" so "b" is the least recently used.
        cache.GetOrCreate(new Endpoint("a", 1), _ => new object()).Should().BeSameAs(a);
        _ = cache.GetOrCreate(new Endpoint("c", 1), _ => new object());

        cache.Count.Should().Be(2);
        cache.GetOrCreate(new Endpoint("a", 1), _ => new object()).Should().BeSameAs(a);
        cache.GetOrCreate(new Endpoint("b", 1), _ => new object()).Should().NotBeSameAs(b);
    }

    [Fact]
    public void GetOrCreate_Eviction_DoesNotDisposeTheEvictedClient()
    {
        using var cache = new ClientCache<Endpoint, Disposable>(1);
        var evicted = cache.GetOrCreate(new Endpoint("a", 1), _ => new Disposable());

        _ = cache.GetOrCreate(new Endpoint("b", 1), _ => new Disposable());

        evicted.Disposed.Should().BeFalse();
    }

    [Fact]
    public void GetOrCreate_WhenCreateThrows_CachesNothing()
    {
        using var cache = new ClientCache<Endpoint, object>(10);

        var act = () => cache.GetOrCreate(new Endpoint("a", 1), _ => throw new InvalidOperationException("no credentials"));

        act.Should().Throw<InvalidOperationException>();
        cache.Count.Should().Be(0);
        cache.GetOrCreate(new Endpoint("a", 1), _ => new object()).Should().NotBeNull();
    }

    [Fact]
    public void Dispose_DisposesCachedClientsOnce()
    {
        var cache = new ClientCache<Endpoint, Disposable>(10);
        var client = cache.GetOrCreate(new Endpoint("a", 1), _ => new Disposable());

        cache.Dispose();
        cache.Dispose();

        client.DisposeCount.Should().Be(1);
        cache.Invoking(c => c.GetOrCreate(new Endpoint("a", 1), _ => new Disposable())).Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Clear_EmptiesTheCacheWithoutDisposing()
    {
        using var cache = new ClientCache<Endpoint, Disposable>(10);
        var client = cache.GetOrCreate(new Endpoint("a", 1), _ => new Disposable());

        cache.Clear();

        cache.Count.Should().Be(0);
        client.Disposed.Should().BeFalse();
    }

    [Fact]
    public async Task GetOrCreate_ConcurrentCallsForOneKey_CreateOnce()
    {
        using var cache = new ClientCache<Endpoint, object>(10);
        var created = 0;

        var clients = await Task.WhenAll(Enumerable.Range(0, 32).Select(_ => Task.Run(() =>
            cache.GetOrCreate(new Endpoint("a", 1), _ =>
            {
                Interlocked.Increment(ref created);
                Thread.Sleep(5);

                return new object();
            }))));

        created.Should().Be(1);
        clients.Distinct().Should().ContainSingle();
    }

    private sealed class Disposable : IDisposable
    {
        public int DisposeCount { get; private set; }
        public bool Disposed => DisposeCount > 0;

        public void Dispose() => DisposeCount++;
    }
}
